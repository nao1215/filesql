package parser

import (
	"bytes"
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
	"github.com/xuri/excelize/v2"
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

		result, err := Parse(f, Parquet)

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

		_, err := Parse(reader, Parquet)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "empty parquet file")
	})

	t.Run("returns error for invalid parquet data", func(t *testing.T) {
		t.Parallel()

		reader := bytes.NewReader([]byte("not a parquet file"))

		_, err := Parse(reader, Parquet)

		assert.Error(t, err)
		// The format begins with a four-byte mark, and these bytes do not, so
		// the file is refused before the reader is asked to make sense of it.
		assert.Contains(t, err.Error(), "not a parquet file")
		assert.Contains(t, err.Error(), "PAR1")
	})

	t.Run("returns error for data that only ends with the mark", func(t *testing.T) {
		t.Parallel()

		// The reader this package uses checks the trailing mark alone, so this
		// is the shape that reached its metadata parsing: damaged input that
		// panicked there, and input that allocated without stopping.
		reader := bytes.NewReader([]byte("\x00\x00\x00\x00 not parquet PAR1"))

		_, err := Parse(reader, Parquet)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not a parquet file")
	})

	t.Run("accepts a file that begins with the mark", func(t *testing.T) {
		t.Parallel()

		good, readErr := os.ReadFile(filepath.Join("testdata", "products.parquet"))
		if os.IsNotExist(readErr) {
			t.Skip("no parquet fixture")
		}
		require.NoError(t, readErr)

		result, err := Parse(bytes.NewReader(good), Parquet)

		require.NoError(t, err)
		assert.NotEmpty(t, result.Headers)
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
		result, err := Parse(bytes.NewReader(buf.Bytes()), Parquet)

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

		result, err := Parse(bytes.NewReader(buf.Bytes()), Parquet)

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

		result, err := Parse(bytes.NewReader(buf.Bytes()), Parquet)

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

// FuzzParseBinary throws damaged workbooks and Parquet files at the binary
// readers. The property is the one FuzzParseFormats holds for the text formats:
// a parse either returns a table or an error, and never panics. It found a
// panic in the Parquet path in under three seconds, raised inside the Arrow
// library on its own error path, which is why that call is wrapped.
func FuzzParseBinary(f *testing.F) {
	workbook := excelize.NewFile()
	if err := workbook.SetSheetRow("Sheet1", "A1", &[]any{"a", "b"}); err != nil {
		f.Fatal(err)
	}
	if err := workbook.SetCellValue("Sheet1", "A2", 1); err != nil {
		f.Fatal(err)
	}
	var wb bytes.Buffer
	if err := workbook.Write(&wb); err != nil {
		f.Fatal(err)
	}
	if err := workbook.Close(); err != nil {
		f.Fatal(err)
	}
	f.Add(wb.Bytes())

	if parquet, err := os.ReadFile(filepath.Join("testdata", "products.parquet")); err == nil {
		f.Add(parquet)
	}
	f.Add([]byte("PK\x03\x04"))
	f.Add([]byte("PAR1"))
	f.Add([]byte{})
	f.Add([]byte("not a file at all"))

	f.Fuzz(func(t *testing.T, data []byte) {
		for _, fileType := range []FileType{XLSX, Parquet} {
			result, err := Parse(bytes.NewReader(data), fileType)
			if err != nil {
				continue
			}
			if result == nil {
				t.Fatalf("%v: nil result with no error for %d bytes", fileType, len(data))
			}
			for i, record := range result.Records {
				if len(record) != len(result.Headers) {
					t.Fatalf("%v: record %d has %d cells, %d headers", fileType, i, len(record), len(result.Headers))
				}
			}
		}
	})
}
