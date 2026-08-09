package parser

import (
	"bytes"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseParquet(t *testing.T) {
	t.Parallel()

	testdataDir := "testdata"

	t.Run("parses products.parquet from testdata", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(filepath.Join(testdataDir, "products.parquet"))
		if os.IsNotExist(err) {
			t.Skip("testdata/products.parquet not found")
		}
		require.NoError(t, err)
		defer f.Close()

		result, err := parseParquet(f)

		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name", "price"}, result.Headers)
		assert.Equal(t, 3, len(result.Records))
		assert.Equal(t, "1", result.Records[0][0])
		assert.Equal(t, "Laptop", result.Records[0][1])
		assert.Equal(t, "999.99", result.Records[0][2])
		// The types come from the Arrow schema (id INT64, name STRING, price
		// DOUBLE), not from what the rendered values look like.
		assert.Equal(t, []ColumnType{TypeInteger, TypeText, TypeReal}, result.ColumnTypes)
	})

	t.Run("returns error for empty data", func(t *testing.T) {
		t.Parallel()

		reader := bytes.NewReader([]byte{})

		_, err := parseParquet(reader)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "empty parquet file")
	})

	t.Run("returns error for invalid parquet data", func(t *testing.T) {
		t.Parallel()

		reader := bytes.NewReader([]byte("not a parquet file"))

		_, err := parseParquet(reader)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create parquet reader")
	})
}

func TestBytesReaderAt_ReadAt(t *testing.T) {
	t.Parallel()

	t.Run("reads data at specified offset", func(t *testing.T) {
		t.Parallel()

		data := []byte("hello world")
		br := &bytesReaderAt{data: data}
		buf := make([]byte, 5)

		n, err := br.ReadAt(buf, 0)

		assert.NoError(t, err)
		assert.Equal(t, 5, n)
		assert.Equal(t, []byte("hello"), buf)
	})

	t.Run("reads data from middle offset", func(t *testing.T) {
		t.Parallel()

		data := []byte("hello world")
		br := &bytesReaderAt{data: data}
		buf := make([]byte, 5)

		n, err := br.ReadAt(buf, 6)

		assert.NoError(t, err)
		assert.Equal(t, 5, n)
		assert.Equal(t, []byte("world"), buf)
	})

	t.Run("returns EOF when offset beyond data", func(t *testing.T) {
		t.Parallel()

		data := []byte("hello")
		br := &bytesReaderAt{data: data}
		buf := make([]byte, 5)

		n, err := br.ReadAt(buf, 100)

		assert.Equal(t, io.EOF, err)
		assert.Equal(t, 0, n)
	})

	t.Run("returns EOF when buffer larger than remaining data", func(t *testing.T) {
		t.Parallel()

		data := []byte("hi")
		br := &bytesReaderAt{data: data}
		buf := make([]byte, 10)

		n, err := br.ReadAt(buf, 0)

		assert.Equal(t, io.EOF, err)
		assert.Equal(t, 2, n)
		assert.Equal(t, []byte("hi"), buf[:2])
	})
}

func TestBytesReaderAt_Size(t *testing.T) {
	t.Parallel()

	t.Run("returns correct size", func(t *testing.T) {
		t.Parallel()

		data := []byte("hello world")
		br := &bytesReaderAt{data: data}

		assert.Equal(t, int64(11), br.Size())
	})

	t.Run("returns zero for empty data", func(t *testing.T) {
		t.Parallel()

		br := &bytesReaderAt{data: []byte{}}

		assert.Equal(t, int64(0), br.Size())
	})
}

func TestBytesReaderAt_Read(t *testing.T) {
	t.Parallel()

	t.Run("reads data sequentially", func(t *testing.T) {
		t.Parallel()

		data := []byte("hello world")
		br := &bytesReaderAt{data: data}
		buf := make([]byte, 5)

		n, err := br.Read(buf)

		assert.NoError(t, err)
		assert.Equal(t, 5, n)
		assert.Equal(t, []byte("hello"), buf)

		n, err = br.Read(buf)

		assert.NoError(t, err)
		assert.Equal(t, 5, n)
		assert.Equal(t, []byte(" worl"), buf)
	})

	t.Run("returns EOF when offset beyond data", func(t *testing.T) {
		t.Parallel()

		data := []byte("hi")
		br := &bytesReaderAt{data: data, offset: 10}
		buf := make([]byte, 5)

		n, err := br.Read(buf)

		assert.Equal(t, io.EOF, err)
		assert.Equal(t, 0, n)
	})
}

func TestBytesReaderAt_Seek(t *testing.T) {
	t.Parallel()

	t.Run("seeks from start", func(t *testing.T) {
		t.Parallel()

		data := []byte("hello world")
		br := &bytesReaderAt{data: data}

		newOffset, err := br.Seek(5, io.SeekStart)

		assert.NoError(t, err)
		assert.Equal(t, int64(5), newOffset)
		assert.Equal(t, int64(5), br.offset)
	})

	t.Run("seeks from current position", func(t *testing.T) {
		t.Parallel()

		data := []byte("hello world")
		br := &bytesReaderAt{data: data, offset: 3}

		newOffset, err := br.Seek(2, io.SeekCurrent)

		assert.NoError(t, err)
		assert.Equal(t, int64(5), newOffset)
	})

	t.Run("seeks from end", func(t *testing.T) {
		t.Parallel()

		data := []byte("hello world")
		br := &bytesReaderAt{data: data}

		newOffset, err := br.Seek(-5, io.SeekEnd)

		assert.NoError(t, err)
		assert.Equal(t, int64(6), newOffset)
	})

	t.Run("returns error for invalid whence", func(t *testing.T) {
		t.Parallel()

		data := []byte("hello")
		br := &bytesReaderAt{data: data}

		_, err := br.Seek(0, 999)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid whence")
	})

	t.Run("returns error for negative position", func(t *testing.T) {
		t.Parallel()

		data := []byte("hello")
		br := &bytesReaderAt{data: data}

		_, err := br.Seek(-10, io.SeekStart)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "negative position")
	})
}

func TestExtractValueFromArrowArray(t *testing.T) {
	t.Parallel()

	pool := memory.NewGoAllocator()

	t.Run("extracts int64 value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewInt64Builder(pool)
		defer builder.Release()
		builder.AppendValues([]int64{42, 100, -5}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "42", extractValueFromArrowArray(arr, 0))
		assert.Equal(t, "100", extractValueFromArrowArray(arr, 1))
		assert.Equal(t, "-5", extractValueFromArrowArray(arr, 2))
	})

	t.Run("extracts string value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewStringBuilder(pool)
		defer builder.Release()
		builder.AppendValues([]string{"hello", "world"}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "hello", extractValueFromArrowArray(arr, 0))
		assert.Equal(t, "world", extractValueFromArrowArray(arr, 1))
	})

	t.Run("extracts float64 value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewFloat64Builder(pool)
		defer builder.Release()
		builder.AppendValues([]float64{3.14, 2.71}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "3.14", extractValueFromArrowArray(arr, 0))
		assert.Equal(t, "2.71", extractValueFromArrowArray(arr, 1))
	})

	t.Run("extracts boolean value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewBooleanBuilder(pool)
		defer builder.Release()
		builder.AppendValues([]bool{true, false}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "true", extractValueFromArrowArray(arr, 0))
		assert.Equal(t, "false", extractValueFromArrowArray(arr, 1))
	})

	t.Run("returns empty string for null value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewInt64Builder(pool)
		defer builder.Release()
		builder.AppendNull()
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "", extractValueFromArrowArray(arr, 0))
	})

	t.Run("extracts int8 value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewInt8Builder(pool)
		defer builder.Release()
		builder.AppendValues([]int8{127, -128}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "127", extractValueFromArrowArray(arr, 0))
		assert.Equal(t, "-128", extractValueFromArrowArray(arr, 1))
	})

	t.Run("extracts int16 value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewInt16Builder(pool)
		defer builder.Release()
		builder.AppendValues([]int16{32767, -32768}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "32767", extractValueFromArrowArray(arr, 0))
		assert.Equal(t, "-32768", extractValueFromArrowArray(arr, 1))
	})

	t.Run("extracts int32 value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewInt32Builder(pool)
		defer builder.Release()
		builder.AppendValues([]int32{2147483647, -2147483648}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "2147483647", extractValueFromArrowArray(arr, 0))
		assert.Equal(t, "-2147483648", extractValueFromArrowArray(arr, 1))
	})

	t.Run("extracts uint8 value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewUint8Builder(pool)
		defer builder.Release()
		builder.AppendValues([]uint8{0, 255}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "0", extractValueFromArrowArray(arr, 0))
		assert.Equal(t, "255", extractValueFromArrowArray(arr, 1))
	})

	t.Run("extracts uint16 value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewUint16Builder(pool)
		defer builder.Release()
		builder.AppendValues([]uint16{0, 65535}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "0", extractValueFromArrowArray(arr, 0))
		assert.Equal(t, "65535", extractValueFromArrowArray(arr, 1))
	})

	t.Run("extracts uint32 value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewUint32Builder(pool)
		defer builder.Release()
		builder.AppendValues([]uint32{0, 4294967295}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "0", extractValueFromArrowArray(arr, 0))
		assert.Equal(t, "4294967295", extractValueFromArrowArray(arr, 1))
	})

	t.Run("extracts uint64 value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewUint64Builder(pool)
		defer builder.Release()
		builder.AppendValues([]uint64{0, 18446744073709551615}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "0", extractValueFromArrowArray(arr, 0))
		assert.Equal(t, "18446744073709551615", extractValueFromArrowArray(arr, 1))
	})

	t.Run("extracts float32 value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewFloat32Builder(pool)
		defer builder.Release()
		builder.AppendValues([]float32{1.5, 2.5}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "1.5", extractValueFromArrowArray(arr, 0))
		assert.Equal(t, "2.5", extractValueFromArrowArray(arr, 1))
	})

	// An infinity has no decimal spelling, and SQLite's REAL affinity cannot read
	// "+Inf": rendered that way it lands in a column declared REAL as text. The
	// literal below is one SQLite overflows to an infinity.
	t.Run("renders a float32 infinity as a literal SQLite reads as one", func(t *testing.T) {
		t.Parallel()

		builder := array.NewFloat32Builder(pool)
		defer builder.Release()
		builder.AppendValues([]float32{float32(math.Inf(1)), float32(math.Inf(-1)), float32(math.NaN()), 3.14159}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "9e999", extractValueFromArrowArray(arr, 0))
		assert.Equal(t, "-9e999", extractValueFromArrowArray(arr, 1))
		assert.Equal(t, "", extractValueFromArrowArray(arr, 2))
		// The width is the array's, not float64's: through a float64 this would be
		// its expansion, 3.141590118408203.
		assert.Equal(t, "3.14159", extractValueFromArrowArray(arr, 3))
	})

	t.Run("renders a float64 infinity the same way", func(t *testing.T) {
		t.Parallel()

		builder := array.NewFloat64Builder(pool)
		defer builder.Release()
		builder.AppendValues([]float64{math.Inf(1), math.Inf(-1), math.NaN(), 1.5}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "9e999", extractValueFromArrowArray(arr, 0))
		assert.Equal(t, "-9e999", extractValueFromArrowArray(arr, 1))
		assert.Equal(t, "", extractValueFromArrowArray(arr, 2))
		assert.Equal(t, "1.5", extractValueFromArrowArray(arr, 3))
	})

	t.Run("extracts binary value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary)
		defer builder.Release()
		builder.AppendValues([][]byte{[]byte("hello"), []byte("world")}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "hello", extractValueFromArrowArray(arr, 0))
		assert.Equal(t, "world", extractValueFromArrowArray(arr, 1))
	})

	t.Run("extracts date32 value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewDate32Builder(pool)
		defer builder.Release()
		builder.AppendValues([]arrow.Date32{19000, 19001}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "19000", extractValueFromArrowArray(arr, 0))
		assert.Equal(t, "19001", extractValueFromArrowArray(arr, 1))
	})

	t.Run("extracts date64 value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewDate64Builder(pool)
		defer builder.Release()
		builder.AppendValues([]arrow.Date64{1641024000000, 1641110400000}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "1641024000000", extractValueFromArrowArray(arr, 0))
		assert.Equal(t, "1641110400000", extractValueFromArrowArray(arr, 1))
	})

	t.Run("extracts timestamp value", func(t *testing.T) {
		t.Parallel()

		builder := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Millisecond})
		defer builder.Release()
		builder.AppendValues([]arrow.Timestamp{1641024000000, 1641110400000}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		assert.Equal(t, "1641024000000", extractValueFromArrowArray(arr, 0))
		assert.Equal(t, "1641110400000", extractValueFromArrowArray(arr, 1))
	})
}

func TestParseParquet_WithGeneratedData(t *testing.T) {
	t.Parallel()

	t.Run("parses parquet with empty records", func(t *testing.T) {
		t.Parallel()

		// Create a parquet file with headers only (no data rows)
		schema := arrow.NewSchema(
			[]arrow.Field{
				{Name: "col1", Type: arrow.PrimitiveTypes.Int64},
				{Name: "col2", Type: arrow.BinaryTypes.String},
			},
			nil,
		)

		pool := memory.NewGoAllocator()

		// Create empty arrays
		col1Builder := array.NewInt64Builder(pool)
		defer col1Builder.Release()
		col1Arr := col1Builder.NewArray()
		defer col1Arr.Release()

		col2Builder := array.NewStringBuilder(pool)
		defer col2Builder.Release()
		col2Arr := col2Builder.NewArray()
		defer col2Arr.Release()

		// Create record with 0 rows
		record := array.NewRecord(schema, []arrow.Array{col1Arr, col2Arr}, 0)
		defer record.Release()

		// Create table
		table := array.NewTableFromRecords(schema, []arrow.Record{record})
		defer table.Release()

		// Write to buffer
		var buf bytes.Buffer
		props := parquet.NewWriterProperties()
		arrProps := pqarrow.DefaultWriterProps()
		err := pqarrow.WriteTable(table, &buf, 1024, props, arrProps)
		require.NoError(t, err)

		// Parse the parquet data
		result, err := parseParquet(bytes.NewReader(buf.Bytes()))

		require.NoError(t, err)
		assert.Equal(t, []string{"col1", "col2"}, result.Headers)
		assert.Equal(t, 0, len(result.Records))
	})

	t.Run("parses parquet with multiple data types", func(t *testing.T) {
		t.Parallel()

		schema := arrow.NewSchema(
			[]arrow.Field{
				{Name: "int_col", Type: arrow.PrimitiveTypes.Int64},
				{Name: "str_col", Type: arrow.BinaryTypes.String},
				{Name: "float_col", Type: arrow.PrimitiveTypes.Float64},
				{Name: "bool_col", Type: arrow.FixedWidthTypes.Boolean},
			},
			nil,
		)

		pool := memory.NewGoAllocator()

		intBuilder := array.NewInt64Builder(pool)
		defer intBuilder.Release()
		intBuilder.AppendValues([]int64{1, 2, 3}, nil)

		strBuilder := array.NewStringBuilder(pool)
		defer strBuilder.Release()
		strBuilder.AppendValues([]string{"a", "b", "c"}, nil)

		floatBuilder := array.NewFloat64Builder(pool)
		defer floatBuilder.Release()
		floatBuilder.AppendValues([]float64{1.1, 2.2, 3.3}, nil)

		boolBuilder := array.NewBooleanBuilder(pool)
		defer boolBuilder.Release()
		boolBuilder.AppendValues([]bool{true, false, true}, nil)

		intArr := intBuilder.NewArray()
		defer intArr.Release()
		strArr := strBuilder.NewArray()
		defer strArr.Release()
		floatArr := floatBuilder.NewArray()
		defer floatArr.Release()
		boolArr := boolBuilder.NewArray()
		defer boolArr.Release()

		record := array.NewRecord(schema, []arrow.Array{intArr, strArr, floatArr, boolArr}, 3)
		defer record.Release()

		table := array.NewTableFromRecords(schema, []arrow.Record{record})
		defer table.Release()

		var buf bytes.Buffer
		props := parquet.NewWriterProperties()
		arrProps := pqarrow.DefaultWriterProps()
		err := pqarrow.WriteTable(table, &buf, 1024, props, arrProps)
		require.NoError(t, err)

		result, err := parseParquet(bytes.NewReader(buf.Bytes()))

		require.NoError(t, err)
		assert.Equal(t, []string{"int_col", "str_col", "float_col", "bool_col"}, result.Headers)
		assert.Equal(t, 3, len(result.Records))
		assert.Equal(t, []string{"1", "a", "1.1", "true"}, result.Records[0])
		assert.Equal(t, []string{"2", "b", "2.2", "false"}, result.Records[1])
		assert.Equal(t, []string{"3", "c", "3.3", "true"}, result.Records[2])
	})

	t.Run("parses parquet with null values", func(t *testing.T) {
		t.Parallel()

		schema := arrow.NewSchema(
			[]arrow.Field{
				{Name: "nullable_col", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			},
			nil,
		)

		pool := memory.NewGoAllocator()

		builder := array.NewInt64Builder(pool)
		defer builder.Release()
		builder.Append(1)
		builder.AppendNull()
		builder.Append(3)

		arr := builder.NewArray()
		defer arr.Release()

		record := array.NewRecord(schema, []arrow.Array{arr}, 3)
		defer record.Release()

		table := array.NewTableFromRecords(schema, []arrow.Record{record})
		defer table.Release()

		var buf bytes.Buffer
		props := parquet.NewWriterProperties()
		arrProps := pqarrow.DefaultWriterProps()
		err := pqarrow.WriteTable(table, &buf, 1024, props, arrProps)
		require.NoError(t, err)

		result, err := parseParquet(bytes.NewReader(buf.Bytes()))

		require.NoError(t, err)
		assert.Equal(t, 3, len(result.Records))
		assert.Equal(t, "1", result.Records[0][0])
		assert.Equal(t, "", result.Records[1][0]) // null becomes empty string
		assert.Equal(t, "3", result.Records[2][0])
	})
}

func TestParse_Parquet(t *testing.T) {
	t.Parallel()

	testdataDir := "testdata"

	t.Run("parses parquet through Parse function", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(filepath.Join(testdataDir, "products.parquet"))
		if os.IsNotExist(err) {
			t.Skip("testdata/products.parquet not found")
		}
		require.NoError(t, err)
		defer f.Close()

		result, err := Parse(f, Parquet)

		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name", "price"}, result.Headers)
		assert.Equal(t, 3, len(result.Records))
	})
}
