//nolint:goconst // Arrow scalar conversions are easier to read with inline literal outputs.
package parser

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	pqfile "github.com/apache/arrow/go/v18/parquet/file"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
)

// bytesReaderAt wraps a byte slice to implement io.ReaderAt and io.Seeker
type bytesReaderAt struct {
	data   []byte
	offset int64
}

// ReadAt implements io.ReaderAt
func (b *bytesReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= int64(len(b.data)) {
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

// parseParquet parses Parquet data from reader.
func parseParquet(reader io.Reader) (*TableData, error) {
	// Read all data into memory (Parquet requires random access)
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read parquet data: %w", err)
	}

	if len(data) == 0 {
		return nil, errors.New("empty parquet file")
	}

	// Create a bytes reader for the parquet data
	bytesReader := &bytesReaderAt{data: data}

	// Create parquet file reader from bytes
	pqReader, err := pqfile.NewParquetReader(bytesReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader: %w", err)
	}
	defer pqReader.Close()

	// Create arrow file reader
	arrowReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create arrow reader: %w", err)
	}

	// Read all record batches using the table reader approach
	table, err := arrowReader.ReadTable(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to read table: %w", err)
	}
	defer table.Release()

	// Extract headers from schema
	schema := table.Schema()
	headers := make([]string, schema.NumFields())
	for i, field := range schema.Fields() {
		headers[i] = field.Name
	}

	// Parquet declares the type of every column, so the schema is read rather
	// than inferred from the rendered values: inference cannot tell a STRING
	// column of digits from an INT64 one, and would turn a zip code into a
	// number.
	columnTypes := arrowColumnTypes(schema)

	if table.NumRows() == 0 {
		return &TableData{
			Headers:     headers,
			Records:     [][]string{},
			ColumnTypes: columnTypes,
		}, nil
	}

	// Read data by converting table to record batches
	tableReader := array.NewTableReader(table, 0)
	defer tableReader.Release()

	var records [][]string
	for tableReader.Next() {
		batch := tableReader.Record()

		// Convert each row in the batch
		numRows := batch.NumRows()
		for i := range numRows {
			row := make([]string, batch.NumCols())
			for j, col := range batch.Columns() {
				row[j] = extractValueFromArrowArray(col, i)
			}
			records = append(records, row)
		}

		// Release the batch to free memory immediately
		batch.Release()
	}

	if err := tableReader.Err(); err != nil {
		return nil, fmt.Errorf("error reading table records: %w", err)
	}

	return &TableData{
		Headers:     headers,
		Records:     records,
		ColumnTypes: columnTypes,
	}, nil
}

// arrowColumnTypes maps a Parquet file's Arrow schema onto column types. The
// type chosen for each column has to agree with what extractValueFromArrowArray
// renders, because the value a caller reads back is parsed from that string.
// Anything not named here stays text, which is the safe answer for a type whose
// rendered shape is not known.
func arrowColumnTypes(schema *arrow.Schema) []ColumnType {
	fields := schema.Fields()
	types := make([]ColumnType, len(fields))
	for i, field := range fields {
		types[i] = arrowColumnType(field.Type)
	}
	return types
}

// arrowColumnType is the column type for one Arrow type. Booleans render as
// "true"/"false" and stay text; the temporal types render as the raw count they
// store, which is an integer.
func arrowColumnType(dt arrow.DataType) ColumnType {
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.DATE32, arrow.DATE64, arrow.TIMESTAMP:
		return TypeInteger
	case arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64:
		return TypeReal
	default:
		return TypeText
	}
}

// sqliteFloatText renders a float at bitSize so SQLite's REAL affinity converts
// it back to the same number, which "%g" does not for the three values that have
// no decimal spelling.
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
func sqliteFloatText(f float64, bitSize int) string {
	switch {
	case math.IsInf(f, 1):
		return "9e999"
	case math.IsInf(f, -1):
		return "-9e999"
	case math.IsNaN(f):
		return ""
	}
	return strconv.FormatFloat(f, 'g', -1, bitSize)
}

// extractValueFromArrowArray extracts a value from an Arrow array at the given index.
func extractValueFromArrowArray(arr arrow.Array, index int64) string {
	if arr.IsNull(int(index)) {
		return ""
	}

	switch a := arr.(type) {
	case *array.Boolean:
		if a.Value(int(index)) {
			return "true"
		}
		return "false"

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

	case *array.Float32:
		return sqliteFloatText(float64(a.Value(int(index))), 32)
	case *array.Float64:
		return sqliteFloatText(a.Value(int(index)), 64)

	case *array.String:
		return a.Value(int(index))
	case *array.Binary:
		return string(a.Value(int(index)))

	case *array.Date32:
		days := a.Value(int(index))
		return fmt.Sprintf("%d", days)
	case *array.Date64:
		millis := a.Value(int(index))
		return fmt.Sprintf("%d", millis)

	case *array.Timestamp:
		ts := a.Value(int(index))
		return fmt.Sprintf("%d", ts)

	default:
		return fmt.Sprintf("%v", arr.GetOneForMarshal(int(index)))
	}
}
