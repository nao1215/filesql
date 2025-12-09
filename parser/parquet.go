package parser

import (
	"context"
	"errors"
	"fmt"
	"io"
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

	if table.NumRows() == 0 {
		return &TableData{
			Headers:     headers,
			Records:     [][]string{},
			ColumnTypes: make([]ColumnType, len(headers)),
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

	// Infer column types from the string records
	columnTypes := inferColumnTypes(headers, records)

	return &TableData{
		Headers:     headers,
		Records:     records,
		ColumnTypes: columnTypes,
	}, nil
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
		return fmt.Sprintf("%g", a.Value(int(index)))
	case *array.Float64:
		return fmt.Sprintf("%g", a.Value(int(index)))

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
