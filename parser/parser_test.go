package parser

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func closeFileOnCleanup(t *testing.T, f *os.File, name string) {
	t.Helper()

	t.Cleanup(func() {
		if err := f.Close(); err != nil {
			t.Errorf("close %s: %v", name, err)
		}
	})
}

func TestParse_CSV(t *testing.T) {
	t.Parallel()

	t.Run("parses CSV with header and data", func(t *testing.T) {
		t.Parallel()

		input := "name,age,city\nAlice,30,Tokyo\nBob,25,Osaka"
		reader := strings.NewReader(input)

		result, err := Parse(reader, CSV)

		require.NoError(t, err)
		assert.Equal(t, []string{"name", "age", "city"}, result.Headers)
		assert.Equal(t, 2, len(result.Records))
		assert.Equal(t, []string{"Alice", "30", "Tokyo"}, result.Records[0])
		assert.Equal(t, []string{"Bob", "25", "Osaka"}, result.Records[1])
	})

	t.Run("infers integer column type", func(t *testing.T) {
		t.Parallel()

		input := "value\n42\n100\n-5"
		reader := strings.NewReader(input)

		result, err := Parse(reader, CSV)

		require.NoError(t, err)
		assert.Equal(t, TypeInteger, result.ColumnTypes[0])
	})

	t.Run("infers real column type", func(t *testing.T) {
		t.Parallel()

		input := "value\n3.14\n2.71\n1.0"
		reader := strings.NewReader(input)

		result, err := Parse(reader, CSV)

		require.NoError(t, err)
		assert.Equal(t, TypeReal, result.ColumnTypes[0])
	})

	t.Run("returns error for empty input", func(t *testing.T) {
		t.Parallel()

		reader := strings.NewReader("")

		_, err := Parse(reader, CSV)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty CSV data")
	})

	t.Run("returns error for nil reader", func(t *testing.T) {
		t.Parallel()

		_, err := Parse(nil, CSV)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "reader cannot be nil")
	})

	t.Run("returns error for duplicate column names", func(t *testing.T) {
		t.Parallel()

		input := "name,name,city\nAlice,30,Tokyo"
		reader := strings.NewReader(input)

		_, err := Parse(reader, CSV)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate column name")
	})

	// Surrounding whitespace does not make a second column: filesql compares
	// header names trimmed, so this is the same duplicate as the case above. It
	// is where this fork deliberately differs from the archived
	// github.com/nao1215/fileparser it came from,
	// which compares the names as they stand.
	t.Run("returns error for names that differ only by surrounding whitespace", func(t *testing.T) {
		t.Parallel()

		_, err := Parse(strings.NewReader("name, name ,city\nAlice,30,Tokyo"), CSV)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate column name")
	})
}

func TestParse_CROnlyLineEndings(t *testing.T) {
	t.Parallel()

	t.Run("a CR-terminated CSV keeps its rows", func(t *testing.T) {
		t.Parallel()

		result, err := Parse(strings.NewReader("name,age\rAlice,30\rBob,40\r"), CSV)

		require.NoError(t, err)
		assert.Equal(t, []string{"name", "age"}, result.Headers)
		assert.Equal(t, [][]string{{"Alice", "30"}, {"Bob", "40"}}, result.Records)
	})

	t.Run("a CR-terminated TSV keeps its rows", func(t *testing.T) {
		t.Parallel()

		result, err := Parse(strings.NewReader("name\tage\rAlice\t30\rBob\t40\r"), TSV)

		require.NoError(t, err)
		assert.Equal(t, []string{"name", "age"}, result.Headers)
		assert.Equal(t, [][]string{{"Alice", "30"}, {"Bob", "40"}}, result.Records)
	})

	t.Run("a CR-terminated LTSV keeps its rows", func(t *testing.T) {
		t.Parallel()

		result, err := Parse(strings.NewReader("name:Alice\tage:30\rname:Bob\tage:40\r"), LTSV)

		require.NoError(t, err)
		assert.Equal(t, []string{"name", "age"}, result.Headers)
		assert.Equal(t, [][]string{{"Alice", "30"}, {"Bob", "40"}}, result.Records)
	})

	t.Run("a CR inside a quoted field of an LF file is data, not a row boundary", func(t *testing.T) {
		t.Parallel()

		result, err := Parse(strings.NewReader("name,note\nAlice,\"a\rb\"\nBob,plain\n"), CSV)

		require.NoError(t, err)
		assert.Equal(t, [][]string{{"Alice", "a\rb"}, {"Bob", "plain"}}, result.Records)
	})

	t.Run("a CRLF-terminated CSV keeps its rows", func(t *testing.T) {
		t.Parallel()

		result, err := Parse(strings.NewReader("name,age\r\nAlice,30\r\nBob,40\r\n"), CSV)

		require.NoError(t, err)
		assert.Equal(t, []string{"name", "age"}, result.Headers)
		assert.Equal(t, [][]string{{"Alice", "30"}, {"Bob", "40"}}, result.Records)
	})

	t.Run("a single CR-terminated line is a header with no rows", func(t *testing.T) {
		t.Parallel()

		result, err := Parse(strings.NewReader("name,age\r"), CSV)

		require.NoError(t, err)
		assert.Equal(t, []string{"name", "age"}, result.Headers)
		assert.Empty(t, result.Records)
	})
}

func TestParse_TSV(t *testing.T) {
	t.Parallel()

	t.Run("parses TSV correctly", func(t *testing.T) {
		t.Parallel()

		input := "name\tage\tprice\nLaptop\t30\t1000"
		reader := strings.NewReader(input)

		result, err := Parse(reader, TSV)

		require.NoError(t, err)
		assert.Equal(t, []string{"name", "age", "price"}, result.Headers)
		assert.Equal(t, 1, len(result.Records))
		assert.Equal(t, []string{"Laptop", "30", "1000"}, result.Records[0])
	})
}

// TestParse_TSVRowShape pins a TSV row to its header. Everything downstream
// reads a record by header position, so a record of another length is a
// TableData nothing can use: prep called such a row valid and wrote it back,
// and the file it wrote failed to load.
func TestParse_TSVRowShape(t *testing.T) {
	t.Parallel()

	refused := []struct {
		name  string
		input string
	}{
		{name: "a row longer than the header", input: "a\n1\t2\n"},
		{name: "a row shorter than the header", input: "a\tb\n1\n"},
		{name: "an empty header and a row of two cells", input: "\n\t"},
		{name: "a long row after a good one", input: "a\tb\n1\t2\n3\t4\t5\n"},
	}
	for _, tt := range refused {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := Parse(strings.NewReader(tt.input), TSV)

			require.Error(t, err, "a row that does not fit its header must be refused")
			assert.ErrorIs(t, err, ErrTSVSyntax)
		})
	}

	accepted := []struct {
		name    string
		input   string
		headers []string
		records [][]string
	}{
		{
			name:    "a row of exactly the header's width",
			input:   "a\tb\n1\t2\n",
			headers: []string{"a", "b"},
			records: [][]string{{"1", "2"}},
		},
		{
			name:    "a row of empty cells",
			input:   "a\tb\n\t\n",
			headers: []string{"a", "b"},
			records: [][]string{{"", ""}},
		},
		{
			name:    "a header alone",
			input:   "a\tb\n",
			headers: []string{"a", "b"},
			records: [][]string{},
		},
	}
	for _, tt := range accepted {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := Parse(strings.NewReader(tt.input), TSV)

			require.NoError(t, err)
			assert.Equal(t, tt.headers, result.Headers)
			assert.Equal(t, tt.records, result.Records)
		})
	}
}

func TestParse_TSVTakesFieldsLiterally(t *testing.T) {
	t.Parallel()

	t.Run("a quote is data, not a quote character", func(t *testing.T) {
		t.Parallel()

		input := "name\tnote\nalice\t5'9\" tall\nbob\tsaid \"hi\" loudly\n"

		result, err := Parse(strings.NewReader(input), TSV)

		require.NoError(t, err)
		assert.Equal(t, []string{"name", "note"}, result.Headers)
		assert.Equal(t, [][]string{
			{"alice", `5'9" tall`},
			{"bob", `said "hi" loudly`},
		}, result.Records)
	})

	t.Run("a field that begins and ends with a quote keeps both", func(t *testing.T) {
		t.Parallel()

		input := "name\tnote\nalice\t\"quoted\"\n"

		result, err := Parse(strings.NewReader(input), TSV)

		require.NoError(t, err)
		assert.Equal(t, [][]string{{"alice", `"quoted"`}}, result.Records)
	})

	t.Run("a doubled quote is two characters", func(t *testing.T) {
		t.Parallel()

		input := "v\na\"\"b\n"

		result, err := Parse(strings.NewReader(input), TSV)

		require.NoError(t, err)
		assert.Equal(t, [][]string{{`a""b`}}, result.Records)
	})

	t.Run("a blank line is the empty value of a one-column file", func(t *testing.T) {
		t.Parallel()

		input := "v\nalice\n\nbob\n"

		result, err := Parse(strings.NewReader(input), TSV)

		require.NoError(t, err)
		assert.Equal(t, [][]string{{"alice"}, {""}, {"bob"}}, result.Records)
	})

	t.Run("a blank line between multi-column records is skipped", func(t *testing.T) {
		t.Parallel()

		input := "a\tb\n1\t2\n\n3\t4\n"

		result, err := Parse(strings.NewReader(input), TSV)

		require.NoError(t, err)
		assert.Equal(t, [][]string{{"1", "2"}, {"3", "4"}}, result.Records)
	})

	t.Run("CRLF line endings are stripped, not kept as data", func(t *testing.T) {
		t.Parallel()

		input := "a\tb\r\n1\t2\r\n"

		result, err := Parse(strings.NewReader(input), TSV)

		require.NoError(t, err)
		assert.Equal(t, []string{"a", "b"}, result.Headers)
		assert.Equal(t, [][]string{{"1", "2"}}, result.Records)
	})

	t.Run("CSV still reads a quoted field as one field", func(t *testing.T) {
		t.Parallel()

		input := "name,note\nalice,\"a,b\"\n"

		result, err := Parse(strings.NewReader(input), CSV)

		require.NoError(t, err)
		assert.Equal(t, [][]string{{"alice", "a,b"}}, result.Records)
	})
}

func TestParse_LTSV(t *testing.T) {
	t.Parallel()

	t.Run("parses LTSV correctly", func(t *testing.T) {
		t.Parallel()

		input := "host:192.168.0.1\tmethod:GET\tpath:/index.html\nhost:192.168.0.2\tmethod:POST\tpath:/api/users"
		reader := strings.NewReader(input)

		result, err := Parse(reader, LTSV)

		require.NoError(t, err)
		assert.Equal(t, 2, len(result.Records))
		assert.Contains(t, result.Headers, "host")
		assert.Contains(t, result.Headers, "method")
		assert.Contains(t, result.Headers, "path")
	})

	t.Run("returns error for empty LTSV", func(t *testing.T) {
		t.Parallel()

		reader := strings.NewReader("")

		_, err := Parse(reader, LTSV)

		assert.Error(t, err)
	})

	t.Run("returns error for duplicate labels within one record", func(t *testing.T) {
		t.Parallel()

		reader := strings.NewReader("name:Alice\tname:Bob\tage:30")

		_, err := Parse(reader, LTSV)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate column name")
	})
}

func TestParse_FromTestdata(t *testing.T) {
	t.Parallel()

	// Skip if testdata directory doesn't exist
	testdataDir := "testdata"
	if _, err := os.Stat(testdataDir); os.IsNotExist(err) {
		t.Skip("testdata directory not found")
	}

	t.Run("parses sample.csv", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(filepath.Join(testdataDir, "sample.csv"))
		require.NoError(t, err)
		closeFileOnCleanup(t, f, "sample.csv")

		result, err := Parse(f, CSV)

		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name", "age", "email"}, result.Headers)
		assert.Equal(t, 3, len(result.Records))
	})

	t.Run("parses sample.csv.gz", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(filepath.Join(testdataDir, "sample.csv.gz"))
		require.NoError(t, err)
		closeFileOnCleanup(t, f, "sample.csv.gz")

		result, err := Parse(f, CSVGZ)

		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name", "age", "email"}, result.Headers)
		assert.Equal(t, 3, len(result.Records))
	})

	t.Run("parses products.tsv", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(filepath.Join(testdataDir, "products.tsv"))
		require.NoError(t, err)
		closeFileOnCleanup(t, f, "products.tsv")

		result, err := Parse(f, TSV)

		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name", "price"}, result.Headers)
		assert.Equal(t, 3, len(result.Records))
	})

	t.Run("parses logs.ltsv", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(filepath.Join(testdataDir, "logs.ltsv"))
		require.NoError(t, err)
		closeFileOnCleanup(t, f, "logs.ltsv")

		result, err := Parse(f, LTSV)

		require.NoError(t, err)
		assert.Greater(t, len(result.Records), 0)
		assert.Greater(t, len(result.Headers), 0)
	})

	t.Run("parses excel/sample.xlsx", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(filepath.Join(testdataDir, "excel", "sample.xlsx"))
		require.NoError(t, err)
		closeFileOnCleanup(t, f, "excel/sample.xlsx")

		result, err := Parse(f, XLSX)

		require.NoError(t, err)
		assert.Greater(t, len(result.Records), 0)
		assert.Greater(t, len(result.Headers), 0)
	})
}

func TestBaseFileType(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		fileType FileType
		expected FileType
	}{
		// CSV variants
		{CSV, CSV},
		{CSVGZ, CSV},
		{CSVBZ2, CSV},
		{CSVXZ, CSV},
		{CSVZSTD, CSV},
		{CSVZLIB, CSV},
		{CSVSNAPPY, CSV},
		{CSVS2, CSV},
		{CSVLZ4, CSV},
		// TSV variants
		{TSV, TSV},
		{TSVGZ, TSV},
		{TSVBZ2, TSV},
		{TSVXZ, TSV},
		{TSVZSTD, TSV},
		{TSVZLIB, TSV},
		{TSVSNAPPY, TSV},
		{TSVS2, TSV},
		{TSVLZ4, TSV},
		// LTSV variants
		{LTSV, LTSV},
		{LTSVGZ, LTSV},
		{LTSVBZ2, LTSV},
		{LTSVXZ, LTSV},
		{LTSVZSTD, LTSV},
		{LTSVZLIB, LTSV},
		{LTSVSNAPPY, LTSV},
		{LTSVS2, LTSV},
		{LTSVLZ4, LTSV},
		// Parquet variants
		{Parquet, Parquet},
		{ParquetGZ, Parquet},
		{ParquetBZ2, Parquet},
		{ParquetXZ, Parquet},
		{ParquetZSTD, Parquet},
		{ParquetZLIB, Parquet},
		{ParquetSNAPPY, Parquet},
		{ParquetS2, Parquet},
		{ParquetLZ4, Parquet},
		// XLSX variants
		{XLSX, XLSX},
		{XLSXGZ, XLSX},
		{XLSXBZ2, XLSX},
		{XLSXXZ, XLSX},
		{XLSXZSTD, XLSX},
		{XLSXZLIB, XLSX},
		{XLSXSNAPPY, XLSX},
		{XLSXS2, XLSX},
		{XLSXLZ4, XLSX},
		// JSON variants
		{JSON, JSON},
		{JSONGZ, JSON},
		{JSONBZ2, JSON},
		{JSONXZ, JSON},
		{JSONZSTD, JSON},
		{JSONZLIB, JSON},
		{JSONSNAPPY, JSON},
		{JSONS2, JSON},
		{JSONLZ4, JSON},
		// JSONL variants
		{JSONL, JSONL},
		{JSONLGZ, JSONL},
		{JSONLBZ2, JSONL},
		{JSONLXZ, JSONL},
		{JSONLZSTD, JSONL},
		{JSONLZLIB, JSONL},
		{JSONLSNAPPY, JSONL},
		{JSONLS2, JSONL},
		{JSONLLZ4, JSONL},
		// Unsupported
		{Unsupported, Unsupported},
	}

	for _, tc := range testCases {
		t.Run(tc.fileType.String(), func(t *testing.T) {
			t.Parallel()

			result := BaseFileType(tc.fileType)

			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestColumnType_String(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		colType  ColumnType
		expected string
	}{
		{TypeText, "TEXT"},
		{TypeInteger, "INTEGER"},
		{TypeReal, "REAL"},
		{TypeDatetime, "DATETIME"},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			t.Parallel()

			result := tc.colType.String()

			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestFileType_String(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		fileType FileType
		expected string
	}{
		// Base types
		{CSV, "CSV"},
		{TSV, "TSV"},
		{LTSV, "LTSV"},
		{Parquet, "Parquet"},
		{XLSX, "XLSX"},
		// CSV compressed
		{CSVGZ, "CSV (gzip)"},
		{CSVBZ2, "CSV (bzip2)"},
		{CSVXZ, "CSV (xz)"},
		{CSVZSTD, "CSV (zstd)"},
		{CSVZLIB, "CSV (zlib)"},
		{CSVSNAPPY, "CSV (snappy)"},
		{CSVS2, "CSV (s2)"},
		{CSVLZ4, "CSV (lz4)"},
		// TSV compressed
		{TSVGZ, "TSV (gzip)"},
		{TSVBZ2, "TSV (bzip2)"},
		{TSVXZ, "TSV (xz)"},
		{TSVZSTD, "TSV (zstd)"},
		{TSVZLIB, "TSV (zlib)"},
		{TSVSNAPPY, "TSV (snappy)"},
		{TSVS2, "TSV (s2)"},
		{TSVLZ4, "TSV (lz4)"},
		// LTSV compressed
		{LTSVGZ, "LTSV (gzip)"},
		{LTSVBZ2, "LTSV (bzip2)"},
		{LTSVXZ, "LTSV (xz)"},
		{LTSVZSTD, "LTSV (zstd)"},
		{LTSVZLIB, "LTSV (zlib)"},
		{LTSVSNAPPY, "LTSV (snappy)"},
		{LTSVS2, "LTSV (s2)"},
		{LTSVLZ4, "LTSV (lz4)"},
		// Parquet compressed
		{ParquetGZ, "Parquet (gzip)"},
		{ParquetBZ2, "Parquet (bzip2)"},
		{ParquetXZ, "Parquet (xz)"},
		{ParquetZSTD, "Parquet (zstd)"},
		{ParquetZLIB, "Parquet (zlib)"},
		{ParquetSNAPPY, "Parquet (snappy)"},
		{ParquetS2, "Parquet (s2)"},
		{ParquetLZ4, "Parquet (lz4)"},
		// XLSX compressed
		{XLSXGZ, "XLSX (gzip)"},
		{XLSXBZ2, "XLSX (bzip2)"},
		{XLSXXZ, "XLSX (xz)"},
		{XLSXZSTD, "XLSX (zstd)"},
		{XLSXZLIB, "XLSX (zlib)"},
		{XLSXSNAPPY, "XLSX (snappy)"},
		{XLSXS2, "XLSX (s2)"},
		{XLSXLZ4, "XLSX (lz4)"},
		// JSON
		{JSON, "JSON"},
		{JSONL, "JSONL"},
		{JSONGZ, "JSON (gzip)"},
		{JSONBZ2, "JSON (bzip2)"},
		{JSONXZ, "JSON (xz)"},
		{JSONZSTD, "JSON (zstd)"},
		{JSONZLIB, "JSON (zlib)"},
		{JSONSNAPPY, "JSON (snappy)"},
		{JSONS2, "JSON (s2)"},
		{JSONLZ4, "JSON (lz4)"},
		// JSONL compressed
		{JSONLGZ, "JSONL (gzip)"},
		{JSONLBZ2, "JSONL (bzip2)"},
		{JSONLXZ, "JSONL (xz)"},
		{JSONLZSTD, "JSONL (zstd)"},
		{JSONLZLIB, "JSONL (zlib)"},
		{JSONLSNAPPY, "JSONL (snappy)"},
		{JSONLS2, "JSONL (s2)"},
		{JSONLLZ4, "JSONL (lz4)"},
		// Unsupported
		{Unsupported, "Unsupported"},
		{FileType(999), "Unsupported"},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			t.Parallel()

			result := tc.fileType.String()

			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestDetectFileType(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		path     string
		expected FileType
	}{
		// Base formats
		{"data.csv", CSV},
		{"data.tsv", TSV},
		{"data.ltsv", LTSV},
		{"data.parquet", Parquet},
		{"data.xlsx", XLSX},

		// Gzip compressed
		{"data.csv.gz", CSVGZ},
		{"data.tsv.gz", TSVGZ},
		{"data.ltsv.gz", LTSVGZ},
		{"data.parquet.gz", ParquetGZ},
		{"data.xlsx.gz", XLSXGZ},

		// Bzip2 compressed
		{"data.csv.bz2", CSVBZ2},
		{"data.tsv.bz2", TSVBZ2},
		{"data.ltsv.bz2", LTSVBZ2},
		{"data.parquet.bz2", ParquetBZ2},
		{"data.xlsx.bz2", XLSXBZ2},

		// XZ compressed
		{"data.csv.xz", CSVXZ},
		{"data.tsv.xz", TSVXZ},
		{"data.ltsv.xz", LTSVXZ},
		{"data.parquet.xz", ParquetXZ},
		{"data.xlsx.xz", XLSXXZ},

		// ZSTD compressed
		{"data.csv.zst", CSVZSTD},
		{"data.tsv.zst", TSVZSTD},
		{"data.ltsv.zst", LTSVZSTD},
		{"data.parquet.zst", ParquetZSTD},
		{"data.xlsx.zst", XLSXZSTD},

		// ZLIB compressed
		{"data.csv.z", CSVZLIB},
		{"data.tsv.z", TSVZLIB},
		{"data.ltsv.z", LTSVZLIB},
		{"data.parquet.z", ParquetZLIB},
		{"data.xlsx.z", XLSXZLIB},

		// Snappy compressed
		{"data.csv.snappy", CSVSNAPPY},
		{"data.tsv.snappy", TSVSNAPPY},
		{"data.ltsv.snappy", LTSVSNAPPY},
		{"data.parquet.snappy", ParquetSNAPPY},
		{"data.xlsx.snappy", XLSXSNAPPY},

		// S2 compressed
		{"data.csv.s2", CSVS2},
		{"data.tsv.s2", TSVS2},
		{"data.ltsv.s2", LTSVS2},
		{"data.parquet.s2", ParquetS2},
		{"data.xlsx.s2", XLSXS2},

		// LZ4 compressed
		{"data.csv.lz4", CSVLZ4},
		{"data.tsv.lz4", TSVLZ4},
		{"data.ltsv.lz4", LTSVLZ4},
		{"data.parquet.lz4", ParquetLZ4},
		{"data.xlsx.lz4", XLSXLZ4},

		// Case insensitive
		{"DATA.CSV", CSV},
		{"data.CSV.GZ", CSVGZ},
		{"DATA.TSV.BZ2", TSVBZ2},

		// With path
		{"/path/to/data.csv", CSV},
		{"./relative/path/data.tsv.gz", TSVGZ},

		// JSON
		{"data.json", JSON},
		{"data.json.gz", JSONGZ},
		{"data.json.bz2", JSONBZ2},
		{"data.json.xz", JSONXZ},
		{"data.json.zst", JSONZSTD},
		{"data.json.z", JSONZLIB},
		{"data.json.snappy", JSONSNAPPY},
		{"data.json.s2", JSONS2},
		{"data.json.lz4", JSONLZ4},

		// JSONL
		{"data.jsonl", JSONL},
		{"data.jsonl.gz", JSONLGZ},
		{"data.jsonl.bz2", JSONLBZ2},
		{"data.jsonl.xz", JSONLXZ},
		{"data.jsonl.zst", JSONLZSTD},
		{"data.jsonl.z", JSONLZLIB},
		{"data.jsonl.snappy", JSONLSNAPPY},
		{"data.jsonl.s2", JSONLS2},
		{"data.jsonl.lz4", JSONLLZ4},

		// Unsupported (.fed is handled by the wire subpackage, not by Parse)
		{"payment.fed", Unsupported},
		{"payment.FED", Unsupported},
		{"payment.fed.gz", Unsupported},
		{"payment.fed.zst", Unsupported},
		{"/path/to/payment.fed", Unsupported},
		{"data.txt", Unsupported},
		{"noextension", Unsupported},
		{"", Unsupported},
	}

	for _, tc := range testCases {
		t.Run(tc.path, func(t *testing.T) {
			t.Parallel()

			result := DetectFileType(tc.path)

			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestCreateDecompressedReader_NoCompression(t *testing.T) {
	t.Parallel()

	testCases := []FileType{CSV, TSV, LTSV, Parquet, XLSX, JSON, JSONL}

	for _, ft := range testCases {
		t.Run(ft.String(), func(t *testing.T) {
			t.Parallel()

			input := strings.NewReader("test data")
			reader, closeFunc, err := createDecompressedReader(input, ft)

			assert.NoError(t, err)
			assert.NotNil(t, reader)
			// The close function is never nil, so a caller can defer it without
			// asking whether this codec has anything to release.
			require.NotNil(t, closeFunc)
			assert.NoError(t, closeFunc())
		})
	}
}

func TestCreateDecompressedReader_InvalidGzip(t *testing.T) {
	t.Parallel()

	input := strings.NewReader("not gzip data")

	_, _, err := createDecompressedReader(input, CSVGZ)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "gzip")
}

func TestCreateDecompressedReader_InvalidXZ(t *testing.T) {
	t.Parallel()

	input := strings.NewReader("not xz data")

	_, _, err := createDecompressedReader(input, CSVXZ)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "xz")
}

func TestCreateDecompressedReader_InvalidZSTD(t *testing.T) {
	t.Parallel()

	input := strings.NewReader("not zstd data")

	// Note: zstd may not fail on invalid data until read,
	// so we just verify the reader is created
	reader, closeFunc, err := createDecompressedReader(input, CSVZSTD)

	require.NoError(t, err)
	assert.NotNil(t, reader)
	if closeFunc != nil {
		assert.NoError(t, closeFunc())
	}
}

func TestCreateDecompressedReader_Bzip2(t *testing.T) {
	t.Parallel()

	// bzip2 doesn't fail on creation, only on read
	input := strings.NewReader("not bzip2 data")

	reader, closeFunc, err := createDecompressedReader(input, CSVBZ2)

	assert.NoError(t, err)
	assert.NotNil(t, reader)
	// bzip2 has nothing to release, and still hands back a close function to
	// defer.
	require.NotNil(t, closeFunc)
	assert.NoError(t, closeFunc())
}

func TestParse_UnsupportedFileType(t *testing.T) {
	t.Parallel()

	input := strings.NewReader("test data")

	_, err := Parse(input, Unsupported)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported")
}

func TestParseLTSV_EmptyInput(t *testing.T) {
	t.Parallel()

	input := strings.NewReader("")

	_, err := Parse(input, LTSV)

	assert.Error(t, err)
}

func TestParseLTSV_PreservesColumnOrder(t *testing.T) {
	t.Parallel()

	// First record has columns in specific order
	input := "col_a:1\tcol_b:2\tcol_c:3\ncol_c:6\tcol_a:4\tcol_b:5"
	reader := strings.NewReader(input)

	result, err := Parse(reader, LTSV)

	require.NoError(t, err)
	// Headers should be in first-seen order: col_a, col_b, col_c
	assert.Equal(t, []string{"col_a", "col_b", "col_c"}, result.Headers)
	assert.Equal(t, []string{"1", "2", "3"}, result.Records[0])
	assert.Equal(t, []string{"4", "5", "6"}, result.Records[1])
}

func TestParseLTSV_MissingValues(t *testing.T) {
	t.Parallel()

	// Second record is missing col_b
	input := "col_a:1\tcol_b:2\ncol_a:3"
	reader := strings.NewReader(input)

	result, err := Parse(reader, LTSV)

	require.NoError(t, err)
	assert.Equal(t, []string{"col_a", "col_b"}, result.Headers)
	assert.Equal(t, []string{"1", "2"}, result.Records[0])
	assert.Equal(t, []string{"3", ""}, result.Records[1]) // missing col_b should be empty
}

func TestParse_NewCompressionFormats(t *testing.T) {
	t.Parallel()

	testdataDir := "testdata"
	if _, err := os.Stat(testdataDir); os.IsNotExist(err) {
		t.Skip("testdata directory not found")
	}

	t.Run("parses sample.csv.z (zlib)", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(filepath.Join(testdataDir, "sample.csv.z"))
		require.NoError(t, err)
		closeFileOnCleanup(t, f, "sample.csv.z")

		result, err := Parse(f, CSVZLIB)

		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name", "age", "email"}, result.Headers)
		assert.Equal(t, 3, len(result.Records))
	})

	t.Run("parses sample.csv.snappy", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(filepath.Join(testdataDir, "sample.csv.snappy"))
		require.NoError(t, err)
		closeFileOnCleanup(t, f, "sample.csv.snappy")

		result, err := Parse(f, CSVSNAPPY)

		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name", "age", "email"}, result.Headers)
		assert.Equal(t, 3, len(result.Records))
	})

	t.Run("parses sample.csv.s2", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(filepath.Join(testdataDir, "sample.csv.s2"))
		require.NoError(t, err)
		closeFileOnCleanup(t, f, "sample.csv.s2")

		result, err := Parse(f, CSVS2)

		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name", "age", "email"}, result.Headers)
		assert.Equal(t, 3, len(result.Records))
	})

	t.Run("parses sample.csv.lz4", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(filepath.Join(testdataDir, "sample.csv.lz4"))
		require.NoError(t, err)
		closeFileOnCleanup(t, f, "sample.csv.lz4")

		result, err := Parse(f, CSVLZ4)

		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name", "age", "email"}, result.Headers)
		assert.Equal(t, 3, len(result.Records))
	})

	// TSV compression tests
	t.Run("parses products.tsv.z (zlib)", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(filepath.Join(testdataDir, "products.tsv.z"))
		require.NoError(t, err)
		closeFileOnCleanup(t, f, "products.tsv.z")

		result, err := Parse(f, TSVZLIB)

		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name", "price"}, result.Headers)
		assert.Equal(t, 3, len(result.Records))
	})

	t.Run("parses products.tsv.snappy", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(filepath.Join(testdataDir, "products.tsv.snappy"))
		require.NoError(t, err)
		closeFileOnCleanup(t, f, "products.tsv.snappy")

		result, err := Parse(f, TSVSNAPPY)

		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name", "price"}, result.Headers)
		assert.Equal(t, 3, len(result.Records))
	})

	t.Run("parses products.tsv.s2", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(filepath.Join(testdataDir, "products.tsv.s2"))
		require.NoError(t, err)
		closeFileOnCleanup(t, f, "products.tsv.s2")

		result, err := Parse(f, TSVS2)

		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name", "price"}, result.Headers)
		assert.Equal(t, 3, len(result.Records))
	})

	t.Run("parses products.tsv.lz4", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(filepath.Join(testdataDir, "products.tsv.lz4"))
		require.NoError(t, err)
		closeFileOnCleanup(t, f, "products.tsv.lz4")

		result, err := Parse(f, TSVLZ4)

		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name", "price"}, result.Headers)
		assert.Equal(t, 3, len(result.Records))
	})

	// LTSV compression tests
	t.Run("parses logs.ltsv.z (zlib)", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(filepath.Join(testdataDir, "logs.ltsv.z"))
		require.NoError(t, err)
		closeFileOnCleanup(t, f, "logs.ltsv.z")

		result, err := Parse(f, LTSVZLIB)

		require.NoError(t, err)
		assert.Equal(t, []string{"time", "level", "message"}, result.Headers)
		assert.Equal(t, 3, len(result.Records))
	})

	t.Run("parses logs.ltsv.snappy", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(filepath.Join(testdataDir, "logs.ltsv.snappy"))
		require.NoError(t, err)
		closeFileOnCleanup(t, f, "logs.ltsv.snappy")

		result, err := Parse(f, LTSVSNAPPY)

		require.NoError(t, err)
		assert.Equal(t, []string{"time", "level", "message"}, result.Headers)
		assert.Equal(t, 3, len(result.Records))
	})

	t.Run("parses logs.ltsv.s2", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(filepath.Join(testdataDir, "logs.ltsv.s2"))
		require.NoError(t, err)
		closeFileOnCleanup(t, f, "logs.ltsv.s2")

		result, err := Parse(f, LTSVS2)

		require.NoError(t, err)
		assert.Equal(t, []string{"time", "level", "message"}, result.Headers)
		assert.Equal(t, 3, len(result.Records))
	})

	t.Run("parses logs.ltsv.lz4", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(filepath.Join(testdataDir, "logs.ltsv.lz4"))
		require.NoError(t, err)
		closeFileOnCleanup(t, f, "logs.ltsv.lz4")

		result, err := Parse(f, LTSVLZ4)

		require.NoError(t, err)
		assert.Equal(t, []string{"time", "level", "message"}, result.Headers)
		assert.Equal(t, 3, len(result.Records))
	})
}

func TestCreateDecompressedReader_InvalidZlib(t *testing.T) {
	t.Parallel()

	input := strings.NewReader("not zlib data")

	_, _, err := createDecompressedReader(input, CSVZLIB)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "zlib")
}

func TestParse_InvalidSnappy(t *testing.T) {
	t.Parallel()

	input := strings.NewReader("not snappy data")

	_, err := Parse(input, CSVSNAPPY)

	assert.Error(t, err)
}

func TestParse_InvalidS2(t *testing.T) {
	t.Parallel()

	input := strings.NewReader("not s2 data")

	_, err := Parse(input, CSVS2)

	assert.Error(t, err)
}

func TestParse_InvalidLZ4(t *testing.T) {
	t.Parallel()

	input := strings.NewReader("not lz4 data")

	_, err := Parse(input, CSVLZ4)

	assert.Error(t, err)
}

func TestIsCompressed(t *testing.T) {
	t.Parallel()

	compressedTypes := []FileType{
		CSVGZ, CSVBZ2, CSVXZ, CSVZSTD, CSVZLIB, CSVSNAPPY, CSVS2, CSVLZ4,
		TSVGZ, TSVBZ2, TSVXZ, TSVZSTD, TSVZLIB, TSVSNAPPY, TSVS2, TSVLZ4,
		LTSVGZ, LTSVBZ2, LTSVXZ, LTSVZSTD, LTSVZLIB, LTSVSNAPPY, LTSVS2, LTSVLZ4,
		ParquetGZ, ParquetBZ2, ParquetXZ, ParquetZSTD, ParquetZLIB, ParquetSNAPPY, ParquetS2, ParquetLZ4,
		XLSXGZ, XLSXBZ2, XLSXXZ, XLSXZSTD, XLSXZLIB, XLSXSNAPPY, XLSXS2, XLSXLZ4,
		JSONGZ, JSONBZ2, JSONXZ, JSONZSTD, JSONZLIB, JSONSNAPPY, JSONS2, JSONLZ4,
		JSONLGZ, JSONLBZ2, JSONLXZ, JSONLZSTD, JSONLZLIB, JSONLSNAPPY, JSONLS2, JSONLLZ4,
	}

	uncompressedTypes := []FileType{
		CSV, TSV, LTSV, Parquet, XLSX, JSON, JSONL, Unsupported,
	}

	for _, ft := range compressedTypes {
		t.Run(ft.String()+"_compressed", func(t *testing.T) {
			t.Parallel()
			assert.True(t, IsCompressed(ft))
		})
	}

	for _, ft := range uncompressedTypes {
		t.Run(ft.String()+"_uncompressed", func(t *testing.T) {
			t.Parallel()
			assert.False(t, IsCompressed(ft))
		})
	}
}

// TestParse_LeadingByteOrderMark requires the mark a spreadsheet writes in front
// of a file to be read as part of the encoding rather than as part of the first
// column's name.
func TestParse_LeadingByteOrderMark(t *testing.T) {
	t.Parallel()

	const bom = "\ufeff"

	tests := []struct {
		name        string
		fileType    FileType
		input       string
		wantHeaders []string
		wantFirst   []string
	}{
		{
			name:        "CSV",
			fileType:    CSV,
			input:       bom + "name,memo\na,b\n",
			wantHeaders: []string{"name", "memo"},
			wantFirst:   []string{"a", "b"},
		},
		{
			name:        "TSV",
			fileType:    TSV,
			input:       bom + "name\tmemo\na\tb\n",
			wantHeaders: []string{"name", "memo"},
			wantFirst:   []string{"a", "b"},
		},
		{
			name:        "LTSV",
			fileType:    LTSV,
			input:       bom + "name:a\tmemo:b\n",
			wantHeaders: []string{"name", "memo"},
			wantFirst:   []string{"a", "b"},
		},
		{
			name:        "JSONL",
			fileType:    JSONL,
			input:       bom + "{\"name\":\"a\",\"memo\":\"b\"}\n",
			wantHeaders: []string{"data"},
			wantFirst:   []string{"{\"name\":\"a\",\"memo\":\"b\"}"},
		},
		{
			name:        "CSV without a mark is unchanged",
			fileType:    CSV,
			input:       "name,memo\na,b\n",
			wantHeaders: []string{"name", "memo"},
			wantFirst:   []string{"a", "b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := Parse(strings.NewReader(tt.input), tt.fileType)
			require.NoError(t, err)
			assert.Equal(t, tt.wantHeaders, result.Headers)
			require.Len(t, result.Records, 1)
			assert.Equal(t, tt.wantFirst, result.Records[0])
		})
	}
}

// FuzzParseFormats holds the shape every text format's parse has to keep: a
// parse that succeeds describes a rectangle, so a record can be read by header
// position and a column type belongs to each header. TSV used to return a
// record of another width, which is a TableData nothing downstream can use.
func FuzzParseFormats(f *testing.F) {
	seeds := []string{
		"a,b\n1,2\n",
		"a\tb\n1\t2\n",
		"a:1\tb:2\n",
		`[{"a":1}]`,
		"{\"a\":1}\n{\"a\":2}\n",
		"a,b\n\"x\ny\",2\n",
		"\xef\xbb\xbfa,b\n1,2\n",
		"a,a\n1,2\n",
		"",
		"\n\n\n",
		"\n\t",
		"a,b\n1,2,3\n",
		`{"a":[1,2],"b":{"c":null}}`,
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	types := []FileType{CSV, TSV, LTSV, JSON, JSONL}
	f.Fuzz(func(t *testing.T, data string) {
		for _, fileType := range types {
			result, err := Parse(strings.NewReader(data), fileType)
			if err != nil {
				continue
			}
			if result == nil {
				t.Fatalf("%v: nil result with no error for %q", fileType, data)
			}
			if len(result.ColumnTypes) != len(result.Headers) {
				t.Fatalf("%v: %d column types for %d headers (input %q)",
					fileType, len(result.ColumnTypes), len(result.Headers), data)
			}
			for i, record := range result.Records {
				if len(record) != len(result.Headers) {
					t.Fatalf("%v: record %d has %d cells, %d headers (input %q)",
						fileType, i, len(record), len(result.Headers), data)
				}
			}
		}
	})
}

// TestNewCSVReader pins what the exported reader does when a caller uses it
// directly rather than through Parse. It is the reason this package has a CSV
// reader of its own: encoding/csv turns a CRLF inside a quoted field into an
// LF, which rewrites a value on the way through.
func TestNewCSVReader(t *testing.T) {
	t.Parallel()

	t.Run("a CRLF inside a quoted field survives", func(t *testing.T) {
		t.Parallel()

		records, err := NewCSVReader(strings.NewReader("a,b\n\"x\r\ny\",2\n")).ReadAll()

		require.NoError(t, err)
		assert.Equal(t, [][]string{{"a", "b"}, {"x\r\ny", "2"}}, records)
	})

	t.Run("a lone CR inside a quoted field survives", func(t *testing.T) {
		t.Parallel()

		records, err := NewCSVReader(strings.NewReader("a\n\"x\ry\"\n")).ReadAll()

		require.NoError(t, err)
		assert.Equal(t, [][]string{{"a"}, {"x\ry"}}, records)
	})

	t.Run("a doubled quote is one quote", func(t *testing.T) {
		t.Parallel()

		records, err := NewCSVReader(strings.NewReader("a\n\"x\"\"y\"\n")).ReadAll()

		require.NoError(t, err)
		assert.Equal(t, [][]string{{"a"}, {`x"y`}}, records)
	})

	t.Run("no input is no records", func(t *testing.T) {
		t.Parallel()

		records, err := NewCSVReader(strings.NewReader("")).ReadAll()

		require.NoError(t, err)
		assert.Empty(t, records)
	})

	t.Run("one line with no terminator is one record", func(t *testing.T) {
		t.Parallel()

		records, err := NewCSVReader(strings.NewReader("a,b")).ReadAll()

		require.NoError(t, err)
		assert.Equal(t, [][]string{{"a", "b"}}, records)
	})

	t.Run("a field is bytes, whatever script they spell", func(t *testing.T) {
		t.Parallel()

		records, err := NewCSVReader(strings.NewReader("名前,絵\n日本語,\U0001F600\n")).ReadAll()

		require.NoError(t, err)
		assert.Equal(t, [][]string{{"名前", "絵"}, {"日本語", "\U0001F600"}}, records)
	})

	t.Run("a quote that never closes is a syntax error", func(t *testing.T) {
		t.Parallel()

		_, err := NewCSVReader(strings.NewReader("a\n\"x\n")).ReadAll()

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrCSVSyntax)
	})

	t.Run("rows of differing widths need FieldsPerRecord set to -1", func(t *testing.T) {
		t.Parallel()

		reader := NewCSVReader(strings.NewReader("a,b\n1\n"))
		reader.FieldsPerRecord = -1

		records, err := reader.ReadAll()

		require.NoError(t, err)
		assert.Equal(t, [][]string{{"a", "b"}, {"1"}}, records)
	})

	t.Run("a file of many rows reads them all", func(t *testing.T) {
		t.Parallel()

		var input strings.Builder
		input.WriteString("i\n")
		const rows = 20000
		for i := range rows {
			fmt.Fprintf(&input, "%d\n", i)
		}

		records, err := NewCSVReader(strings.NewReader(input.String())).ReadAll()

		require.NoError(t, err)
		require.Len(t, records, rows+1)
		assert.Equal(t, []string{"19999"}, records[rows])
	})
}

// TestNormalizeLineEndings pins the wrapper a caller reaches for when it holds
// a file written the classic Mac OS way. The decision is made from the start of
// the file rather than per carriage return, because a carriage return inside a
// quoted field is data.
func TestNormalizeLineEndings(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		in   string
		want string
	}{
		{name: "CR-terminated lines become LF", in: "a\rb\rc\r", want: "a\nb\nc\n"},
		{name: "LF-terminated lines are untouched", in: "a\nb\n", want: "a\nb\n"},
		{name: "CRLF-terminated lines are untouched", in: "a\r\nb\r\n", want: "a\r\nb\r\n"},
		{name: "no input stays no input", in: "", want: ""},
		{name: "one line with no terminator is untouched", in: "a,b", want: "a,b"},
		{name: "a CR inside a quoted field of an LF file is data", in: "a\n\"x\ry\"\n", want: "a\n\"x\ry\"\n"},
		{name: "a lone CR at the end of an LF file is left alone", in: "a\nb\r", want: "a\nb\r"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := io.ReadAll(NormalizeLineEndings(strings.NewReader(tc.in)))

			require.NoError(t, err)
			assert.Equal(t, tc.want, string(got))
		})
	}
}

func TestParserSurfaceEdges(t *testing.T) {
	t.Parallel()

	t.Run("a column type outside the named ones prints as TEXT", func(t *testing.T) {
		t.Parallel()

		// TEXT is the type that holds anything, so it is the answer that cannot
		// misdescribe a value.
		assert.Equal(t, "TEXT", ColumnType(99).String())
	})

	t.Run("a value converts by the type it is asked for", func(t *testing.T) {
		t.Parallel()

		// A datetime keeps its text, since SQLite stores one as text, and a type
		// outside the named ones does the same.
		assert.Equal(t, "2023-03-15T00:00:00Z", ParseValue("2023-03-15T00:00:00Z", TypeDatetime))
		assert.Equal(t, "anything", ParseValue("anything", ColumnType(99)))
		// A cell of only spaces is missing rather than a value.
		assert.Nil(t, ParseValue("   ", TypeInteger))
		// A cell that spells no number keeps its text rather than becoming zero.
		assert.Equal(t, "n/a", ParseValue("n/a", TypeInteger))
		assert.Equal(t, "n/a", ParseValue("n/a", TypeReal))
	})

	t.Run("the exported extensions spell what a file is named", func(t *testing.T) {
		t.Parallel()

		// The compression ones are the codecs' own constants, so this holds the
		// value a caller sees rather than the spelling of the expression.
		assert.Equal(t, ".csv", ExtCSV)
		assert.Equal(t, ".tsv", ExtTSV)
		assert.Equal(t, ".ltsv", ExtLTSV)
		assert.Equal(t, ".parquet", ExtParquet)
		assert.Equal(t, ".xlsx", ExtXLSX)
		assert.Equal(t, ".json", ExtJSON)
		assert.Equal(t, ".jsonl", ExtJSONL)
		assert.Equal(t, ".gz", ExtGZ)
		assert.Equal(t, ".bz2", ExtBZ2)
		assert.Equal(t, ".xz", ExtXZ)
		assert.Equal(t, ".zst", ExtZSTD)
		assert.Equal(t, ".z", ExtZLIB)
		assert.Equal(t, ".snappy", ExtSNAPPY)
		assert.Equal(t, ".s2", ExtS2)
		assert.Equal(t, ".lz4", ExtLZ4)
	})
}
