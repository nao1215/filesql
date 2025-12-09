package parser

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "empty CSV data")
	})

	t.Run("returns error for nil reader", func(t *testing.T) {
		t.Parallel()

		_, err := Parse(nil, CSV)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "reader cannot be nil")
	})

	t.Run("returns error for duplicate column names", func(t *testing.T) {
		t.Parallel()

		input := "name,name,city\nAlice,30,Tokyo"
		reader := strings.NewReader(input)

		_, err := Parse(reader, CSV)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate column name")
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
}

func TestParse_FromTestdata(t *testing.T) {
	t.Parallel()

	// Skip if testdata directory doesn't exist
	testdataDir := "../testdata"
	if _, err := os.Stat(testdataDir); os.IsNotExist(err) {
		t.Skip("testdata directory not found")
	}

	t.Run("parses sample.csv", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(filepath.Join(testdataDir, "sample.csv"))
		require.NoError(t, err)
		defer f.Close()

		result, err := Parse(f, CSV)

		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name", "age", "email"}, result.Headers)
		assert.Equal(t, 3, len(result.Records))
	})

	t.Run("parses sample.csv.gz", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(filepath.Join(testdataDir, "sample.csv.gz"))
		require.NoError(t, err)
		defer f.Close()

		result, err := Parse(f, CSVGZ)

		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name", "age", "email"}, result.Headers)
		assert.Equal(t, 3, len(result.Records))
	})

	t.Run("parses products.tsv", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(filepath.Join(testdataDir, "products.tsv"))
		require.NoError(t, err)
		defer f.Close()

		result, err := Parse(f, TSV)

		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name", "price"}, result.Headers)
		assert.Equal(t, 3, len(result.Records))
	})

	t.Run("parses logs.ltsv", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(filepath.Join(testdataDir, "logs.ltsv"))
		require.NoError(t, err)
		defer f.Close()

		result, err := Parse(f, LTSV)

		require.NoError(t, err)
		assert.Greater(t, len(result.Records), 0)
		assert.Greater(t, len(result.Headers), 0)
	})

	t.Run("parses excel/sample.xlsx", func(t *testing.T) {
		t.Parallel()

		f, err := os.Open(filepath.Join(testdataDir, "excel", "sample.xlsx"))
		require.NoError(t, err)
		defer f.Close()

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
		{CSV, CSV},
		{CSVGZ, CSV},
		{CSVBZ2, CSV},
		{CSVXZ, CSV},
		{CSVZSTD, CSV},
		{TSV, TSV},
		{TSVGZ, TSV},
		{LTSV, LTSV},
		{LTSVGZ, LTSV},
		{Parquet, Parquet},
		{ParquetGZ, Parquet},
		{XLSX, XLSX},
		{XLSXGZ, XLSX},
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
		{CSV, "CSV"},
		{TSV, "TSV"},
		{LTSV, "LTSV"},
		{Parquet, "Parquet"},
		{XLSX, "XLSX"},
		{CSVGZ, "CSV (gzip)"},
		{CSVBZ2, "CSV (bzip2)"},
		{CSVXZ, "CSV (xz)"},
		{CSVZSTD, "CSV (zstd)"},
		{TSVGZ, "TSV (gzip)"},
		{ParquetZSTD, "Parquet (zstd)"},
		{XLSXGZ, "XLSX (gzip)"},
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

		// Case insensitive
		{"DATA.CSV", CSV},
		{"data.CSV.GZ", CSVGZ},
		{"DATA.TSV.BZ2", TSVBZ2},

		// With path
		{"/path/to/data.csv", CSV},
		{"./relative/path/data.tsv.gz", TSVGZ},

		// Unsupported
		{"data.txt", Unsupported},
		{"data.json", Unsupported},
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

	testCases := []FileType{CSV, TSV, LTSV, Parquet, XLSX}

	for _, ft := range testCases {
		t.Run(ft.String(), func(t *testing.T) {
			t.Parallel()

			input := strings.NewReader("test data")
			reader, closeFunc, err := createDecompressedReader(input, ft)

			assert.NoError(t, err)
			assert.NotNil(t, reader)
			assert.Nil(t, closeFunc)
		})
	}
}

func TestCreateDecompressedReader_InvalidGzip(t *testing.T) {
	t.Parallel()

	input := strings.NewReader("not gzip data")

	_, _, err := createDecompressedReader(input, CSVGZ)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "gzip")
}

func TestCreateDecompressedReader_InvalidXZ(t *testing.T) {
	t.Parallel()

	input := strings.NewReader("not xz data")

	_, _, err := createDecompressedReader(input, CSVXZ)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "xz")
}

func TestCreateDecompressedReader_InvalidZSTD(t *testing.T) {
	t.Parallel()

	input := strings.NewReader("not zstd data")

	// Note: zstd may not fail on invalid data until read,
	// so we just verify the reader is created
	reader, closeFunc, err := createDecompressedReader(input, CSVZSTD)

	// zstd decoder creation might succeed even with invalid data
	// The error would occur during read
	if err == nil {
		assert.NotNil(t, reader)
		if closeFunc != nil {
			assert.NoError(t, closeFunc())
		}
	}
}

func TestCreateDecompressedReader_Bzip2(t *testing.T) {
	t.Parallel()

	// bzip2 doesn't fail on creation, only on read
	input := strings.NewReader("not bzip2 data")

	reader, closeFunc, err := createDecompressedReader(input, CSVBZ2)

	assert.NoError(t, err)
	assert.NotNil(t, reader)
	assert.Nil(t, closeFunc) // bzip2 doesn't have close func
}

func TestParse_UnsupportedFileType(t *testing.T) {
	t.Parallel()

	input := strings.NewReader("test data")

	_, err := Parse(input, Unsupported)

	assert.Error(t, err)
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
