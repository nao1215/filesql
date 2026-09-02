package parser

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
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

		// A parquet file with headers only (no data rows).
		type row struct {
			Col1 int64  `parquet:"col1"`
			Col2 string `parquet:"col2"`
		}
		var buf bytes.Buffer
		w := parquet.NewGenericWriter[row](&buf)
		require.NoError(t, w.Close())

		result, err := Parse(bytes.NewReader(buf.Bytes()), Parquet)

		require.NoError(t, err)
		assert.Equal(t, []string{"col1", "col2"}, result.Headers)
		assert.Equal(t, 0, len(result.Records))
	})

	t.Run("parses parquet with multiple data types", func(t *testing.T) {
		t.Parallel()

		type row struct {
			IntCol   int64   `parquet:"int_col"`
			StrCol   string  `parquet:"str_col"`
			FloatCol float64 `parquet:"float_col"`
			BoolCol  bool    `parquet:"bool_col"`
		}
		var buf bytes.Buffer
		w := parquet.NewGenericWriter[row](&buf)
		_, err := w.Write([]row{
			{IntCol: 1, StrCol: "a", FloatCol: 1.1, BoolCol: true},
			{IntCol: 2, StrCol: "b", FloatCol: 2.2, BoolCol: false},
			{IntCol: 3, StrCol: "c", FloatCol: 3.3, BoolCol: true},
		})
		require.NoError(t, err)
		require.NoError(t, w.Close())

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

		type row struct {
			NullableCol *int64 `parquet:"nullable_col,optional"`
		}
		one, three := int64(1), int64(3)
		var buf bytes.Buffer
		w := parquet.NewGenericWriter[row](&buf)
		_, err := w.Write([]row{{NullableCol: &one}, {NullableCol: nil}, {NullableCol: &three}})
		require.NoError(t, err)
		require.NoError(t, w.Close())

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
//
// It does not assert on allocation. The footer length a Parquet file declares
// is bounded by the reader, but a page header inside a column chunk declares
// sizes the library allocates without this package seeing them, so a ceiling
// here would fail on a file nothing in this repository can currently refuse.
// What the footer costs is pinned by a test of its own instead.
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
		if len(data) > 1<<16 {
			t.Skip()
		}
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

// TestParseParquetReportsNulls pins that a caller can tell a null from an empty
// string.
//
// Parquet is the one format this package reads that has a null of its own, and
// every spelling of "nothing" in a Go string is also a value a file can hold, so
// the rendered text cannot carry the distinction. The reader underneath knows it
// -- it is how a load stores a Parquet null as SQL NULL -- and Parse used to
// drop it, so a column of nulls and a column of empty strings came back the same
// and nothing said which was which.
func TestParseParquetReportsNulls(t *testing.T) {
	t.Parallel()

	type row struct {
		Label  *string `parquet:"label,optional"`
		Amount int64   `parquet:"amount"`
	}
	empty := ""
	present := "x"

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[row](&buf)
	_, err := w.Write([]row{
		{Label: &present, Amount: 1},
		{Label: nil, Amount: 2},
		{Label: &empty, Amount: 3},
	})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	result, err := Parse(bytes.NewReader(buf.Bytes()), Parquet)
	require.NoError(t, err)

	assert.Equal(t, [][]string{{"x", "1"}, {"", "2"}, {"", "3"}}, result.Records,
		"the rendered text is what it always was; a null renders empty")
	require.NotNil(t, result.Nulls, "a format with a null of its own reports which cells hold one")
	assert.Equal(t, [][]bool{{false, false}, {true, false}, {false, false}}, result.Nulls,
		"the second row's label is missing and the third row's is an empty string")
	require.Len(t, result.Nulls, len(result.Records), "the mask is parallel to the records")
	for i := range result.Nulls {
		assert.Len(t, result.Nulls[i], len(result.Records[i]), "row %d", i)
	}
}

// TestParseReportsNoNullsForAFormatWithout pins the other half: a caller can
// tell "this format has no nulls" from "this file has none". Every format here
// but Parquet spells a missing value as an empty field, which is a value, so the
// mask is nil rather than all false.
func TestParseReportsNoNullsForAFormatWithout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		kind  FileType
		input string
	}{
		{name: "csv", kind: CSV, input: "a,b\n1,\n"},
		{name: "tsv", kind: TSV, input: "a\tb\n1\t\n"},
		{name: "ltsv", kind: LTSV, input: "a:1\tb:\n"},
		{name: "json", kind: JSON, input: `[{"a":null}]`},
		{name: "jsonl", kind: JSONL, input: "{\"a\":null}\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := Parse(strings.NewReader(tt.input), tt.kind)
			require.NoError(t, err)
			assert.Nil(t, result.Nulls, "a format with no null of its own reports none")
		})
	}
}

// TestParseParquetSpellsValuesTheFormatsWay pins the other half of what a
// caller has to know: values come back spelled the way the format reads them
// rather than the way SQLite stores them.
//
// A whole float is "2" here and the real 2 after a load, and a boolean is
// "true" here and 1 there. Feeding Records into a CSV and loading that is
// therefore not the same as loading the Parquet file, which is worth pinning
// because nothing about a [][]string says which spelling it holds.
func TestParseParquetSpellsValuesTheFormatsWay(t *testing.T) {
	t.Parallel()

	type row struct {
		Whole float64 `parquet:"whole"`
		Part  float64 `parquet:"part"`
		Flag  bool    `parquet:"flag"`
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[row](&buf)
	_, err := w.Write([]row{{Whole: 2, Part: 1.5, Flag: true}})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	result, err := Parse(bytes.NewReader(buf.Bytes()), Parquet)
	require.NoError(t, err)
	assert.Equal(t, [][]string{{"2", "1.5", "true"}}, result.Records)
}
