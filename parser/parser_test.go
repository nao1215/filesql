package parser

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nao1215/filesql/internal/codec"
	"github.com/nao1215/filesql/internal/textin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
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
		{
			// A line with nothing on it is not a header, here as in a CSV,
			// where encoding/csv has always passed over one. The header is the
			// first line that holds something, and a line holding empty cells
			// holds that many columns.
			name:    "a blank line before the header",
			input:   "\n1\t2\n",
			headers: []string{"1", "2"},
			records: [][]string{},
		},
		{
			name:    "a blank line before a header of empty cells",
			input:   "\n\t",
			headers: []string{"column_1", "column_2"},
			records: [][]string{},
		},
		{
			// A line of whitespace carries nothing to name a column after
			// either, and it is what a hand-edited file leaves. Taken as the
			// header it named one column after itself and made every row that
			// followed the wrong width.
			name:    "a line of whitespace before the header",
			input:   "   \n1\t2\n",
			headers: []string{"1", "2"},
			records: [][]string{},
		},
		{
			name:    "a line of whitespace before the header of a one-column file",
			input:   "   \nv\n1\n",
			headers: []string{"v"},
			records: [][]string{{"1"}},
		},
		{
			// The width comes from the header, so a blank line further down is
			// still not a record of a file this wide.
			name:    "a blank line after a header the whitespace line preceded",
			input:   "   \na\tb\n1\t2\n\n3\t4\n",
			headers: []string{"a", "b"},
			records: [][]string{{"1", "2"}, {"3", "4"}},
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

		// Parse reads what it is given, so the codec comes off first.
		decompressed, closeCodec, err := codec.GZ.NewReader(f)
		require.NoError(t, err)
		defer closeCodec()

		result, err := Parse(decompressed, CSV)

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
		{CSV, "CSV"},
		{TSV, "TSV"},
		{LTSV, "LTSV"},
		{Parquet, "Parquet"},
		{XLSX, "XLSX"},
		{JSON, "JSON"},
		{JSONL, "JSONL"},
		{Unsupported, "Unsupported"},
		{FileType(99), "Unsupported"},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.expected, tc.fileType.String())
		})
	}
}

func TestDetectFileType(t *testing.T) {
	t.Parallel()

	formats := []struct {
		ext      string
		expected FileType
	}{
		{extCSV, CSV},
		{extTSV, TSV},
		{extLTSV, LTSV},
		{extParquet, Parquet},
		{extXLSX, XLSX},
		{extJSON, JSON},
		{extJSONL, JSONL},
	}

	t.Run("a format is itself under every codec", func(t *testing.T) {
		t.Parallel()

		// The answer names the format alone, so every codec over a given format
		// gives the same one. This is the whole cross product, which the enum
		// used to hold a constant for.
		for _, f := range formats {
			for _, c := range append([]codec.Codec{codec.None}, codec.All...) {
				path := "data" + f.ext + c.Extension()
				assert.Equal(t, f.expected, DetectFileType(path), "path %s", path)
			}
		}
	})

	t.Run("case is folded", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, CSV, DetectFileType("DATA.CSV"))
		assert.Equal(t, CSV, DetectFileType("DATA.CSV.GZ"))
		assert.Equal(t, Parquet, DetectFileType("Data.Parquet.Zst"))
	})

	t.Run("a path is read from its own name, not its directory", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, CSV, DetectFileType("/tmp/x.tsv/data.csv"))
		assert.Equal(t, TSV, DetectFileType("a.b.c.tsv.gz"))
	})

	t.Run("a path naming no format this package reads is unsupported", func(t *testing.T) {
		t.Parallel()

		for _, path := range []string{
			"data", "data.txt", "data.gz", "data.xml", "", "data.csv.gz.gz",
		} {
			assert.Equal(t, Unsupported, DetectFileType(path), "path %q", path)
		}
	})
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

// TestParse_CompressedFixtures reads every compressed fixture in testdata by
// taking the codec off its name and handing Parse the decompressed stream, which
// is what a caller now does: Parse reads the bytes it is given as the format it
// is told, and the codec is no longer part of that format.
//
// One loop covers what used to be a subtest per format and codec, because the
// file name says both.
func TestParse_CompressedFixtures(t *testing.T) {
	t.Parallel()

	testdataDir := "testdata"
	if _, err := os.Stat(testdataDir); os.IsNotExist(err) {
		t.Skip("testdata directory not found")
	}

	cases := []struct {
		stem    string
		headers []string
		records int
	}{
		{"sample.csv", []string{"id", "name", "age", "email"}, 3},
		{"products.tsv", []string{"id", "name", "price"}, 3},
		{"logs.ltsv", []string{"time", "level", "message"}, 3},
	}

	for _, tc := range cases {
		for _, c := range codec.All {
			file := tc.stem + c.Extension()
			if _, err := os.Stat(filepath.Join(testdataDir, file)); err != nil {
				continue // No fixture for this pairing.
			}
			t.Run(file, func(t *testing.T) {
				t.Parallel()

				f, err := os.Open(filepath.Join(testdataDir, file)) //nolint:gosec // fixed, in-repo fixture name
				require.NoError(t, err)
				closeFileOnCleanup(t, f, file)

				decompressed, closeCodec, err := c.NewReader(f)
				require.NoError(t, err)
				defer func() { _ = closeCodec() }()

				result, err := Parse(decompressed, DetectFileType(file))

				require.NoError(t, err)
				assert.Equal(t, tc.headers, result.Headers)
				assert.Len(t, result.Records, tc.records)
			})
		}
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

func TestParserSurfaceEdges(t *testing.T) {
	t.Parallel()

	t.Run("a column type outside the named ones prints as TEXT", func(t *testing.T) {
		t.Parallel()

		// TEXT is the type that holds anything, so it is the answer that cannot
		// misdescribe a value.
		assert.Equal(t, "TEXT", ColumnType(99).String())
	})

	t.Run("the extension constants spell what a file is named", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, ".csv", extCSV)
		assert.Equal(t, ".tsv", extTSV)
		assert.Equal(t, ".ltsv", extLTSV)
		assert.Equal(t, ".parquet", extParquet)
		assert.Equal(t, ".xlsx", extXLSX)
		assert.Equal(t, ".json", extJSON)
		assert.Equal(t, ".jsonl", extJSONL)
	})
}

// TestParseLTSV_FieldThatNamesNoLabelIsRefused pins that this package refuses an
// LTSV record carrying a field that is not a label and a value.
//
// Parse applies no malformed-row policy -- it has none to apply -- and is strict
// about a delimited record of the wrong width for that reason. A field with no
// label is the same event in the format that has no width: the record carries
// something no column can hold, and keeping it would drop that field in silence.
func TestParseLTSV_FieldThatNamesNoLabelIsRefused(t *testing.T) {
	t.Parallel()

	t.Run("a line holding no pair at all", func(t *testing.T) {
		t.Parallel()

		_, err := Parse(strings.NewReader("a:1\tb:2\nGARBAGE\na:3\tb:4\n"), LTSV)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "row 2")
		assert.Contains(t, err.Error(), "GARBAGE")
	})

	t.Run("one unlabeled field among pairs", func(t *testing.T) {
		t.Parallel()

		_, err := Parse(strings.NewReader("a:1\tJUNK\tb:2\n"), LTSV)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "JUNK")
	})

	t.Run("a record naming only some of the columns is kept", func(t *testing.T) {
		t.Parallel()

		result, err := Parse(strings.NewReader("a:1\tb:2\na:3\n"), LTSV)

		require.NoError(t, err)
		assert.Equal(t, [][]string{{"1", "2"}, {"3", ""}}, result.Records)
	})
}

// TestParse_ReadsTextTheWayALoadDoes pins that this package and
// filesql.OpenContext agree about what a text file holds. They did not: a
// Shift-JIS file parsed here with no error at all, into strings that are not
// characters, and a UTF-16 file was read as single-byte data and refused for a
// field count -- an error about the caller's data for a fault in its encoding.
func TestParse_ReadsTextTheWayALoadDoes(t *testing.T) {
	t.Parallel()

	const text = "a,b\n1,2\n"

	t.Run("a byte-order mark decides the encoding", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			name  string
			input []byte
		}{
			{name: "no mark", input: []byte(text)},
			{name: "a UTF-8 mark", input: append([]byte{0xEF, 0xBB, 0xBF}, text...)},
			{name: "a UTF-16LE mark", input: utf16Bytes(true, text)},
			{name: "a UTF-16BE mark", input: utf16Bytes(false, text)},
		} {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				result, err := Parse(bytes.NewReader(tt.input), CSV)

				require.NoError(t, err)
				assert.Equal(t, []string{"a", "b"}, result.Headers, "the mark belongs to the encoding, not to the first column name")
				assert.Equal(t, [][]string{{"1", "2"}}, result.Records)
			})
		}
	})

	t.Run("bytes that are not characters are refused", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			name  string
			input []byte
		}{
			// Shift-JIS carries no mark to detect and would otherwise be held
			// as bytes no consumer can decode.
			{name: "Shift-JIS", input: []byte("a,b\n\x82\xa0,2\n")},
			{name: "a stray byte", input: []byte("a,b\n\xff,2\n")},
		} {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				_, err := Parse(bytes.NewReader(tt.input), CSV)

				require.Error(t, err, "a text parser hands back characters or nothing")
				assert.ErrorIs(t, err, textin.ErrInvalidUTF8)
				assert.Contains(t, err.Error(), "offset", "the refusal says where the input stopped being text")
			})
		}
	})

	t.Run("a binary container is read as bytes", func(t *testing.T) {
		t.Parallel()

		// A Parquet file holds bytes that are not UTF-8 in its own framing, and
		// reading it through a text decoder would refuse every one of them.
		f, err := os.Open(filepath.Join("..", "testdata", "products.parquet"))
		require.NoError(t, err)
		closeFileOnCleanup(t, f, "products.parquet")

		result, err := Parse(f, Parquet)

		require.NoError(t, err)
		assert.NotEmpty(t, result.Records)
	})
}

// utf16Bytes writes s as UTF-16 with a leading byte-order mark.
func utf16Bytes(littleEndian bool, s string) []byte {
	order := unicode.BigEndian
	if littleEndian {
		order = unicode.LittleEndian
	}
	out, _, err := transform.Bytes(unicode.UTF16(order, unicode.UseBOM).NewEncoder(), []byte(s))
	if err != nil {
		panic(err)
	}
	return out
}
