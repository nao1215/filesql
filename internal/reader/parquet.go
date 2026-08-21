package reader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	pqfile "github.com/apache/arrow/go/v18/parquet/file"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	"github.com/nao1215/filesql/internal/infer"
)

// parquetTable reads the whole of a Parquet file into an Arrow table, with the
// panic a damaged file can raise turned into an error.
//
// A Parquet file this package did not write is untrusted input, and the Arrow
// library panics on some of it rather than reporting -- while parsing the footer
// as well as while reading a page. One changed byte in the footer of a file
// whose magic bytes, length and offsets are all still right reached a nil
// dereference inside NewParquetReader, before the guard that used to sit around
// the table read alone. A caller loading a file chosen by someone else cannot
// defend against a panic, and every other malformed input here is an error.
//
// The guard covers the library and nothing else: the rows go to the caller's
// emit after this returns, so a failure of theirs is not reported as damaged
// data. It can go when the library stops panicking on its own error paths.
func parquetTable(ctx context.Context, data []byte) (tbl arrow.Table, err error) {
	defer func() {
		if r := recover(); r != nil {
			tbl = nil
			err = parseError(nil, "parquet data is damaged: %v", r)
		}
	}()

	pqReader, err := pqfile.NewParquetReader(&bytesReaderAt{data: data})
	if err != nil {
		return nil, parseError(err, "failed to create parquet reader")
	}
	defer pqReader.Close()

	if err := parquetChunksLieInTheFile(pqReader, int64(len(data))); err != nil {
		return nil, err
	}

	arrowReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{}, nil)
	if err != nil {
		return nil, parseError(err, "failed to create arrow reader")
	}

	table, err := readArrowTable(ctx, arrowReader)
	if err != nil {
		return nil, parseError(err, "failed to read table")
	}
	return table, nil
}

// parquetChunksLieInTheFile refuses a file whose metadata places a column chunk
// outside it.
//
// A file's footer says where each column chunk begins and how many bytes it
// occupies, and a chunk that runs past the end of the file it is in cannot be
// read. The decoder does not check: handed a 433-byte file declaring chunks that
// begin at offsets 3098 and 3116, it allocated some 350MiB a second for as long
// as it was left to run. It cannot be stopped from outside either, because the
// page decoding does not check the context it is given, so a deadline passed
// down from here expires without effect and a caller who bounds their own work
// cannot bound this. The only place the file can be refused is before any of it
// is decoded, which is here.
//
// What is asked is only that the bytes a chunk names are in the file: each page
// offset it declares falls inside it, and the chunk's own length does not run
// past the end from wherever it starts. Nothing else is checked -- the footer's
// own bytes are not excluded from the range a chunk may occupy, and a chunk
// whose sizes disagree with each other is left alone -- because the purpose is
// to bound a read rather than to validate a file, so a file that is merely
// unusual passes and only one that could not be read either way is refused. A
// chunk held in another file, which the format allows and this package does not
// follow, is left to the reader to report.
func parquetChunksLieInTheFile(reader *pqfile.Reader, size int64) error {
	meta := reader.MetaData()
	for group := range reader.NumRowGroups() {
		rowGroup := meta.RowGroup(group)
		for column := range rowGroup.NumColumns() {
			chunk, err := rowGroup.ColumnChunk(column)
			if err != nil {
				return parseError(err, "failed to read the metadata of column %d in row group %d", column, group)
			}
			if chunk.FilePath() != "" {
				continue
			}

			// An offset of zero means the page is not there rather than that it
			// sits at the start of the file: a column of a row group with no
			// rows has a dictionary page and no data page, and says so with a
			// zero.
			start := size
			for _, page := range []struct {
				what string
				at   int64
			}{
				{"data page", chunk.DataPageOffset()},
				{"dictionary page", chunk.DictionaryPageOffset()},
			} {
				if page.at == 0 {
					continue
				}
				if page.at < int64(len(parquetMagic)) || page.at >= size {
					return parseError(nil,
						"the %s of column %d in row group %d is declared at offset %d, outside a file of %d bytes",
						page.what, column, group, page.at, size)
				}
				start = min(start, page.at)
			}
			if start == size {
				// The chunk names no page at all, so there is nothing to bound.
				continue
			}
			if length := chunk.TotalCompressedSize(); start+length > size {
				return parseError(nil,
					"column %d of row group %d is declared as %d bytes from offset %d, which runs past the end of a file of %d bytes",
					column, group, length, start, size)
			}
		}
	}
	return nil
}

// readArrowTable is pqarrow.FileReader.ReadTable done in the calling goroutine,
// so a panic it raises can be recovered by the read that called it.
//
// A Parquet file this package did not write is untrusted input, and the reader
// panics on some of it rather than reporting: a corrupted page header reaches a
// nil dereference, and a row group that fails to read is cleaned up by releasing
// a column that was never built. ReadTable does the column reads in goroutines
// of its own, where a recover in the calling goroutine cannot reach them and the
// process dies, so the same reads are done here instead -- one column at a time,
// where the boundary can be held.
//
// This loop can go when the library stops panicking on its own error paths;
// ReadTable is the call it replaces.
func readArrowTable(ctx context.Context, arrowReader *pqarrow.FileReader) (tbl arrow.Table, err error) {
	meta := arrowReader.ParquetReader().MetaData()
	columnIndices := make([]int, meta.Schema.NumColumns())
	for i := range columnIndices {
		columnIndices[i] = i
	}
	rowGroups := make([]int, arrowReader.ParquetReader().NumRowGroups())
	for i := range rowGroups {
		rowGroups[i] = i
	}

	// GetFieldReaders, the plural form, fans the per-field reads out into an
	// errgroup, and a panic in one of those goroutines is not this function's to
	// recover either. The singular form does the same work here.
	fieldIndices, err := arrowReader.Manifest.GetFieldIndices(columnIndices)
	if err != nil {
		return nil, err
	}
	includedLeaves := make(map[int]bool, len(columnIndices))
	for _, col := range columnIndices {
		includedLeaves[col] = true
	}
	// The readers are collected as they are built and released together, so a
	// failure part way through this loop does not leave the ones before it
	// unreleased.
	readers := make([]*pqarrow.ColumnReader, 0, len(fieldIndices))
	defer func() {
		for _, reader := range readers {
			reader.Release()
		}
	}()
	fields := make([]arrow.Field, 0, len(fieldIndices))
	for _, fieldIndex := range fieldIndices {
		reader, readerErr := arrowReader.GetFieldReader(ctx, fieldIndex, includedLeaves, rowGroups)
		if readerErr != nil {
			return nil, readerErr
		}
		if reader == nil || reader.Field() == nil {
			return nil, fmt.Errorf("parquet data is damaged: no reader for field %d", fieldIndex)
		}
		readers = append(readers, reader)
		fields = append(fields, *reader.Field())
	}
	schema := arrow.NewSchema(fields, arrowReader.Manifest.SchemaMeta)

	// The columns are appended as they are built, so the cleanup below releases
	// the ones that exist and never a zero value, whose Release dereferences a
	// chunk it does not have.
	columns := make([]arrow.Column, 0, len(readers))
	defer func() {
		// The columns are copied into the table, which owns them from then on;
		// on the way out with an error they are this function's to release.
		if err == nil {
			return
		}
		for i := range columns {
			columns[i].Release()
		}
	}()
	for i, reader := range readers {
		chunked, readErr := arrowReader.ReadColumn(rowGroups, reader)
		if readErr != nil {
			return nil, readErr
		}
		columns = append(columns, *arrow.NewColumn(schema.Field(i), chunked))
		chunked.Release()
	}

	var rows int
	if len(columns) > 0 {
		rows = columns[0].Len()
	}
	return array.NewTable(schema, columns, int64(rows)), nil
}

// parquetMagic is the four bytes a Parquet file begins and ends with. The
// format defines both, and checking the leading one is worth doing here because
// the reader this package uses checks only the trailing one: a file that ends
// "PAR1" and begins with anything at all is taken for a Parquet file and read
// into its metadata, where damaged input has reached a panic and an allocation
// that does not stop. Fuzzing the reader with the check in place ran 1.4 million
// inputs without either; without it, a worker died within thirty seconds.
var parquetMagic = []byte("PAR1") //nolint:gochecknoglobals // constant-like

// errNotParquet reports bytes that do not begin the way the format says.
func errNotParquet(head []byte) error {
	return parseError(nil, "not a parquet file: it begins %q rather than %q", head, parquetMagic)
}

// bytesReaderAt wraps a byte slice to implement io.ReaderAt and io.Seeker
type bytesReaderAt struct {
	data   []byte
	offset int64
}

// ReadAt implements io.ReaderAt
func (b *bytesReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 || off >= int64(len(b.data)) {
		return 0, io.EOF
	}
	n = copy(p, b.data[off:])
	if n < len(p) {
		err = io.EOF
	}
	return n, err
}

// Size returns the size of the data
func (b *bytesReaderAt) Size() int64 {
	return int64(len(b.data))
}

// Read implements io.Reader
func (b *bytesReaderAt) Read(p []byte) (int, error) {
	if b.offset >= int64(len(b.data)) {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.offset:])
	b.offset += int64(n)
	return n, nil
}

// Seek implements io.Seeker
func (b *bytesReaderAt) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = b.offset + offset
	case io.SeekEnd:
		newOffset = int64(len(b.data)) + offset
	default:
		return 0, errors.New("invalid whence")
	}

	if newOffset < 0 {
		return 0, errors.New("negative position")
	}

	b.offset = newOffset
	return newOffset, nil
}

// readParquet reads a Parquet file in chunks. The whole file is buffered first
// because the format is read back to front: its metadata is at the end.
func readParquet(src io.Reader, opts Options, emit Emit) (Result, error) {
	data, err := io.ReadAll(src)
	if err != nil {
		return Result{}, parseError(err, "failed to read parquet data")
	}
	if len(data) == 0 {
		return Result{}, emptyError("empty parquet file")
	}
	if !bytes.HasPrefix(data, parquetMagic) {
		return Result{}, errNotParquet(data[:min(len(data), len(parquetMagic))])
	}

	table, err := parquetTable(context.Background(), data)
	if err != nil {
		return Result{}, err
	}
	defer table.Release()

	schema := table.Schema()
	header := make([]string, schema.NumFields())
	for i, field := range schema.Fields() {
		header[i] = field.Name
	}
	// Parquet declares the type of every column, so the schema is read rather
	// than inferred from the rendered values: inference cannot tell a STRING
	// column of digits from an INT64 one, and would turn a zip code into a
	// number.
	types := arrowColumnTypes(schema, opts.Rendering)
	result := Result{Header: header, Types: types}

	// A file with a schema and no rows still names its columns.
	if table.NumRows() == 0 {
		return result, emit(&Chunk{Header: header, Types: types})
	}

	tableReader := array.NewTableReader(table, int64(chunkSizeOf(opts)))
	defer tableReader.Release()

	for tableReader.Next() {
		batch := tableReader.Record()

		records := make([][]string, 0, batch.NumRows())
		nulls := make([][]bool, 0, batch.NumRows())
		for i := range batch.NumRows() {
			row := make([]string, batch.NumCols())
			nullRow := make([]bool, batch.NumCols())
			for j, col := range batch.Columns() {
				if arrowCellIsNull(col, i, opts.Rendering) {
					nullRow[j] = true
					continue
				}
				row[j] = extractValueFromArrowArray(col, i, opts.Rendering)
			}
			records = append(records, row)
			nulls = append(nulls, nullRow)
		}

		if len(records) == 0 {
			continue
		}
		result.Rows += len(records)
		result.Total += len(records)
		if err := emit(&Chunk{Header: header, Records: records, Types: types, Nulls: nulls}); err != nil {
			return Result{}, err
		}
	}

	if err := tableReader.Err(); err != nil {
		return Result{}, parseError(err, "error reading table records")
	}
	return result, nil
}

// arrowColumnTypes maps a Parquet file's Arrow schema onto column types.
//
// The type chosen for each column has to agree with what
// extractValueFromArrowArray renders under the same rendering, because a value
// is read back by parsing that string -- and, for a load, because SQLite
// applies the column's affinity to it. A mismatch is worse than text: it would
// store a value the column claims not to hold.
func arrowColumnTypes(schema *arrow.Schema, rendering Rendering) []infer.Type {
	fields := schema.Fields()
	types := make([]infer.Type, len(fields))
	for i, field := range fields {
		types[i] = arrowColumnType(field.Type, rendering)
	}
	return types
}

// arrowColumnType is the column type for one Arrow type. The temporal types
// render as the raw count they store (days, milliseconds, or ticks since the
// epoch), which is an integer. Anything not named here stays text, which is the
// safe answer: an unrecognized type is rendered by extractValueFromArrowArray's
// default branch, and its shape is not known.
func arrowColumnType(dt arrow.DataType, rendering Rendering) infer.Type {
	switch dt.ID() {
	case arrow.BOOL:
		// A boolean renders as 1 or 0 for SQLite, which is an integer there, and
		// as "true" or "false" otherwise, which is not.
		if rendering == RenderSQLite {
			return infer.Integer
		}
		return infer.Text
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.DATE32, arrow.DATE64, arrow.TIMESTAMP:
		return infer.Integer
	case arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64:
		return infer.Real
	default:
		return infer.Text
	}
}

// arrowCellIsNull reports whether a cell has no value the destination can
// store: a Parquet null always, and under RenderSQLite a NaN as well, which
// SQLite has no representation for at all -- a computed NaN is NULL there, so
// NULL is what the value already means. Left as text it would sit in a column
// declared REAL as the word "NaN".
func arrowCellIsNull(arr arrow.Array, index int64, rendering Rendering) bool {
	if arr.IsNull(int(index)) {
		return true
	}
	if rendering != RenderSQLite {
		return false
	}
	switch a := arr.(type) {
	case *array.Float16:
		return math.IsNaN(float64(a.Value(int(index)).Float32()))
	case *array.Float32:
		return math.IsNaN(float64(a.Value(int(index))))
	case *array.Float64:
		return math.IsNaN(a.Value(int(index)))
	default:
		return false
	}
}

// SQLiteFloatText renders a float at bitSize so SQLite's REAL affinity converts
// it back to the same number, which "%g" does not for the three values that have
// no decimal spelling. It is what a load binds and what a dump writes, so a
// value that survives one survives the other.
//
// The column is declared REAL from the Parquet schema, and SQLite applies that
// affinity to the text an import binds: "+Inf" is not a number to it, so the
// cell was stored as TEXT inside a REAL column and typeof() answered "text" for
// a value the file held as a double. "9e999" overflows to infinity when SQLite
// parses it, which is the only spelling that survives.
//
// NaN renders as empty, the same as a null, because SQLite has no NaN at all: a
// computed one becomes NULL there, so NULL is what the value already means in
// the destination. Keeping the word would leave the same TEXT-in-a-REAL-column
// mismatch this exists to remove.
func SQLiteFloatText(f float64, bitSize int) string {
	return floatText(f, bitSize, RenderSQLite)
}

// floatText is SQLiteFloatText with the ".0" suffix left off for a caller that
// renders a value rather than storing it in a typed column.
func floatText(f float64, bitSize int, rendering Rendering) string {
	// A literal SQLite overflows to an infinity while parsing it. There is no
	// spelling of the value itself that its REAL affinity accepts.
	const infinityLiteral = "9e999"
	switch {
	case math.IsInf(f, 1):
		return infinityLiteral
	case math.IsInf(f, -1):
		return "-" + infinityLiteral
	case math.IsNaN(f):
		return ""
	}
	text := strconv.FormatFloat(f, 'g', -1, bitSize)
	// A whole number renders with neither a point nor an exponent, and read back
	// that spelling is an integer. The suffix is what keeps a loaded column REAL;
	// a caller that only renders the value has no column to keep.
	if rendering == RenderSQLite && !strings.ContainsAny(text, ".eE") {
		text += ".0"
	}
	return text
}

// extractValueFromArrowArray extracts a value from an Arrow array at the given index.
func extractValueFromArrowArray(arr arrow.Array, index int64, rendering Rendering) string {
	if arr.IsNull(int(index)) {
		return ""
	}

	switch a := arr.(type) {
	case *array.Boolean:
		return boolText(a.Value(int(index)), rendering)

	case *array.Int8:
		return strconv.Itoa(int(a.Value(int(index))))
	case *array.Int16:
		return strconv.Itoa(int(a.Value(int(index))))
	case *array.Int32:
		return strconv.Itoa(int(a.Value(int(index))))
	case *array.Int64:
		return strconv.FormatInt(a.Value(int(index)), 10)

	case *array.Uint8:
		return strconv.FormatUint(uint64(a.Value(int(index))), 10)
	case *array.Uint16:
		return strconv.FormatUint(uint64(a.Value(int(index))), 10)
	case *array.Uint32:
		return strconv.FormatUint(uint64(a.Value(int(index))), 10)
	case *array.Uint64:
		return strconv.FormatUint(a.Value(int(index)), 10)

	// A half float is rendered at 32 bits, the narrowest width Go can format
	// it at. Without a case of its own it reached the default branch, where
	// "%v" spelled a NaN as "NaN" and a whole number without the point that
	// keeps its column REAL -- in a column the schema had already declared
	// REAL, since arrowColumnType reads FLOAT16 as a real number.
	case *array.Float16:
		return floatText(float64(a.Value(int(index)).Float32()), 32, rendering)
	case *array.Float32:
		return floatText(float64(a.Value(int(index))), 32, rendering)
	case *array.Float64:
		return floatText(a.Value(int(index)), 64, rendering)

	case *array.String:
		return a.Value(int(index))
	case *array.Binary:
		return string(a.Value(int(index)))

	// The temporal types keep the raw count the file stores: days for Date32,
	// milliseconds for Date64, and whatever unit the schema names for Timestamp.
	case *array.Date32:
		return strconv.FormatInt(int64(a.Value(int(index))), 10)
	case *array.Date64:
		return strconv.FormatInt(int64(a.Value(int(index))), 10)
	case *array.Timestamp:
		return strconv.FormatInt(int64(a.Value(int(index))), 10)

	default:
		return fmt.Sprintf("%v", arr.GetOneForMarshal(int(index)))
	}
}

// boolText spells a boolean the way its column is declared: 1 and 0 for the
// INTEGER column a load declares, and the words otherwise.
func boolText(v bool, rendering Rendering) string {
	if rendering == RenderSQLite {
		if v {
			return "1"
		}
		return "0"
	}
	if v {
		return "true"
	}
	return "false"
}
