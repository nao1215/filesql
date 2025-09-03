package filesql

import (
	"compress/gzip"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xuri/excelize/v2"
)

func TestNewFile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		path     string
		expected FileType
	}{
		{
			name:     "CSV file",
			path:     "test.csv",
			expected: FileTypeCSV,
		},
		{
			name:     "TSV file",
			path:     "test.tsv",
			expected: FileTypeTSV,
		},
		{
			name:     "LTSV file",
			path:     "test.ltsv",
			expected: FileTypeLTSV,
		},
		{
			name:     "Compressed CSV file",
			path:     "test.csv.gz",
			expected: FileTypeCSVGZ,
		},
		{
			name:     "Compressed TSV file",
			path:     "test.tsv.bz2",
			expected: FileTypeTSVBZ2,
		},
		{
			name:     "Compressed LTSV file",
			path:     "test.ltsv.xz",
			expected: FileTypeLTSVXZ,
		},
		{
			name:     "Zstd compressed CSV file",
			path:     "test.csv.zst",
			expected: FileTypeCSVZSTD,
		},
		{
			name:     "XLSX file",
			path:     "test.xlsx",
			expected: FileTypeXLSX,
		},
		{
			name:     "Compressed XLSX file with gzip",
			path:     "test.xlsx.gz",
			expected: FileTypeXLSXGZ,
		},
		{
			name:     "Compressed XLSX file with bzip2",
			path:     "test.xlsx.bz2",
			expected: FileTypeXLSXBZ2,
		},
		{
			name:     "Compressed XLSX file with xz",
			path:     "test.xlsx.xz",
			expected: FileTypeXLSXXZ,
		},
		{
			name:     "Compressed XLSX file with zstd",
			path:     "test.xlsx.zst",
			expected: FileTypeXLSXZSTD,
		},
		{
			name:     "Unsupported file",
			path:     "test.txt",
			expected: FileTypeUnsupported,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			file := newFile(tt.path)
			assert.Equal(t, tt.expected, file.getFileType(), "File type mismatch")
			assert.Equal(t, tt.path, file.getPath(), "File path mismatch")
		})
	}
}

func TestFile_IsCompressed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		{
			name:     "Normal CSV file",
			path:     "test.csv",
			expected: false,
		},
		{
			name:     "Gzip compressed file",
			path:     "test.csv.gz",
			expected: true,
		},
		{
			name:     "Bzip2 compressed file",
			path:     "test.csv.bz2",
			expected: true,
		},
		{
			name:     "XZ compressed file",
			path:     "test.csv.xz",
			expected: true,
		},
		{
			name:     "Zstd compressed file",
			path:     "test.csv.zst",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			file := newFile(tt.path)
			assert.Equal(t, tt.expected, file.isCompressed(), "Compression check failed")
		})
	}
}

func TestFile_CompressionTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		isGZ   bool
		isBZ2  bool
		isXZ   bool
		isZSTD bool
	}{
		{
			name:   "Normal file",
			path:   "test.csv",
			isGZ:   false,
			isBZ2:  false,
			isXZ:   false,
			isZSTD: false,
		},
		{
			name:   "GZ file",
			path:   "test.csv.gz",
			isGZ:   true,
			isBZ2:  false,
			isXZ:   false,
			isZSTD: false,
		},
		{
			name:   "BZ2 file",
			path:   "test.csv.bz2",
			isGZ:   false,
			isBZ2:  true,
			isXZ:   false,
			isZSTD: false,
		},
		{
			name:   "XZ file",
			path:   "test.csv.xz",
			isGZ:   false,
			isBZ2:  false,
			isXZ:   true,
			isZSTD: false,
		},
		{
			name:   "ZSTD file",
			path:   "test.csv.zst",
			isGZ:   false,
			isBZ2:  false,
			isXZ:   false,
			isZSTD: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			file := newFile(tt.path)
			assert.Equal(t, tt.isGZ, file.isGZ(), "IsGZ() check failed")
			assert.Equal(t, tt.isBZ2, file.isBZ2(), "IsBZ2() check failed")
			assert.Equal(t, tt.isXZ, file.isXZ(), "IsXZ() check failed")
			assert.Equal(t, tt.isZSTD, file.isZSTD(), "IsZSTD() check failed")
		})
	}
}

func TestFile_ToTable_CSV(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	csvFile := filepath.Join(tmpDir, "test.csv")

	csvContent := `name,age,city
John,25,Tokyo
Alice,30,Osaka
Bob,35,Kyoto`

	err := os.WriteFile(csvFile, []byte(csvContent), 0600)
	require.NoError(t, err, "Failed to write CSV file")

	file := newFile(csvFile)
	table, err := file.toTable()
	require.NoError(t, err, "Failed to convert file to table")

	expectedHeader := header{"name", "age", "city"}
	assert.True(t, table.getHeader().equal(expectedHeader), "Header mismatch")

	assert.Len(t, table.getRecords(), 3, "Record count mismatch")

	expectedFirstRecord := Record{"John", "25", "Tokyo"}
	assert.True(t, table.getRecords()[0].equal(expectedFirstRecord), "First record mismatch")
}

func TestFile_ToTable_TSV(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	tsvFile := filepath.Join(tmpDir, "test.tsv")

	tsvContent := `name	age	city
John	25	Tokyo
Alice	30	Osaka
Bob	35	Kyoto`

	err := os.WriteFile(tsvFile, []byte(tsvContent), 0600)
	require.NoError(t, err, "Failed to write TSV file")

	file := newFile(tsvFile)
	table, err := file.toTable()
	require.NoError(t, err, "Failed to convert file to table")

	expectedHeader := header{"name", "age", "city"}
	assert.True(t, table.getHeader().equal(expectedHeader), "Header mismatch")

	assert.Len(t, table.getRecords(), 3, "Record count mismatch")

	expectedFirstRecord := Record{"John", "25", "Tokyo"}
	assert.True(t, table.getRecords()[0].equal(expectedFirstRecord), "First record mismatch")
}

func TestFile_ToTable_LTSV(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	ltsvFile := filepath.Join(tmpDir, "test.ltsv")

	ltsvContent := `name:John	age:25	city:Tokyo
name:Alice	age:30	city:Osaka
name:Bob	age:35	city:Kyoto`

	err := os.WriteFile(ltsvFile, []byte(ltsvContent), 0600)
	require.NoError(t, err, "Failed to write LTSV file")

	file := newFile(ltsvFile)
	table, err := file.toTable()
	require.NoError(t, err, "Failed to convert file to table")

	assert.Len(t, table.getRecords(), 3, "Record count mismatch")

	// LTSV header order may vary due to map iteration
	header := table.getHeader()
	if len(header) != 3 {
		t.Errorf("expected 3 columns, got %d", len(header))
	}

	// Check that all expected keys exist
	headerMap := make(map[string]bool)
	for _, h := range header {
		headerMap[h] = true
	}
	expectedKeys := []string{"name", "age", "city"}
	for _, key := range expectedKeys {
		if !headerMap[key] {
			t.Errorf("expected key %s not found in header", key)
		}
	}
}

func TestFile_ToTable_CompressedCSV(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	csvFile := filepath.Join(tmpDir, "test.csv.gz")

	csvContent := `name,age,city
John,25,Tokyo
Alice,30,Osaka`

	// Create gzip compressed file
	f, err := os.Create(csvFile) //nolint:gosec // Safe: csvFile is from controlled test temp dir
	if err != nil {
		t.Fatal(err)
	}

	gw := gzip.NewWriter(f)
	_, err = gw.Write([]byte(csvContent))
	if err != nil {
		t.Fatal(err)
	}
	_ = gw.Close() // Ignore close error in test cleanup
	_ = f.Close()  // Ignore close error in test cleanup

	file := newFile(csvFile)
	table, err := file.toTable()
	require.NoError(t, err, "Failed to convert file to table")

	expectedHeader := header{"name", "age", "city"}
	assert.True(t, table.getHeader().equal(expectedHeader), "Header mismatch")

	assert.Len(t, table.getRecords(), 2, "Expected 2 records")
}

func TestFile_ToTable_UnsupportedFormat(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	txtFile := filepath.Join(tmpDir, "test.txt")

	err := os.WriteFile(txtFile, []byte("some content"), 0600)
	if err != nil {
		t.Fatal(err)
	}

	file := newFile(txtFile)
	_, err = file.toTable()
	assert.Error(t, err, "Expected error for unsupported file format")
}

func TestFile_ToTable_EmptyFile(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	csvFile := filepath.Join(tmpDir, "empty.csv")

	err := os.WriteFile(csvFile, []byte(""), 0600)
	if err != nil {
		t.Fatal(err)
	}

	file := newFile(csvFile)
	_, err = file.toTable()
	assert.Error(t, err, "Expected error for empty file")
}

func TestTableFromFilePath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		filePath string
		expected string
	}{
		{
			name:     "Simple CSV file",
			filePath: "test.csv",
			expected: "test",
		},
		{
			name:     "Compressed CSV file",
			filePath: "data.csv.gz",
			expected: "data",
		},
		{
			name:     "Path with directory",
			filePath: filepath.Join("home", "user", "data.csv"),
			expected: "data",
		},
		{
			name:     "File without extension",
			filePath: "data",
			expected: "data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := tableFromFilePath(tt.filePath)
			assert.Equal(t, tt.expected, result, "tableFromFilePath failed")
		})
	}
}

func Test_FileTypeDetectionMethods(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		filePath     string
		expectedCSV  bool
		expectedTSV  bool
		expectedLTSV bool
	}{
		{
			name:         "CSV file",
			filePath:     "test.csv",
			expectedCSV:  true,
			expectedTSV:  false,
			expectedLTSV: false,
		},
		{
			name:         "TSV file",
			filePath:     "test.tsv",
			expectedCSV:  false,
			expectedTSV:  true,
			expectedLTSV: false,
		},
		{
			name:         "LTSV file",
			filePath:     "test.ltsv",
			expectedCSV:  false,
			expectedTSV:  false,
			expectedLTSV: true,
		},
		{
			name:         "Compressed CSV",
			filePath:     "test.csv.gz",
			expectedCSV:  true,
			expectedTSV:  false,
			expectedLTSV: false,
		},
		{
			name:         "Compressed TSV",
			filePath:     "test.tsv.bz2",
			expectedCSV:  false,
			expectedTSV:  true,
			expectedLTSV: false,
		},
		{
			name:         "Compressed LTSV",
			filePath:     "test.ltsv.xz",
			expectedCSV:  false,
			expectedTSV:  false,
			expectedLTSV: true,
		},
		{
			name:         "Unsupported file",
			filePath:     "test.txt",
			expectedCSV:  false,
			expectedTSV:  false,
			expectedLTSV: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			file := newFile(tc.filePath)

			assert.Equal(t, tc.expectedCSV, file.isCSV(), "IsCSV() check failed for %s", tc.filePath)
			assert.Equal(t, tc.expectedTSV, file.isTSV(), "IsTSV() check failed for %s", tc.filePath)
			assert.Equal(t, tc.expectedLTSV, file.isLTSV(), "IsLTSV() check failed for %s", tc.filePath)
		})
	}
}

func Test_isSupportedFile(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		fileName    string
		isSupported bool
	}{
		// Basic formats
		{"test.csv", true},
		{"test.tsv", true},
		{"test.ltsv", true},

		// Compressed formats
		{"test.csv.gz", true},
		{"test.csv.bz2", true},
		{"test.csv.xz", true},
		{"test.csv.zst", true},
		{"test.tsv.gz", true},
		{"test.tsv.bz2", true},
		{"test.tsv.xz", true},
		{"test.tsv.zst", true},
		{"test.ltsv.gz", true},
		{"test.ltsv.bz2", true},
		{"test.ltsv.xz", true},
		{"test.ltsv.zst", true},

		// Case insensitive
		{"test.CSV", true},
		{"test.TSV", true},
		{"test.LTSV", true},
		{"test.CSV.GZ", true},

		// Unsupported formats
		{"test.txt", false},
		{"test.json", false},
		{"test.xml", false},
		{"test.xlsx", true},
		{"test", false},
		{"", false},

		// Edge cases
		{"test.csv.txt", false},    // Wrong final extension
		{"test.gz", false},         // Compression only, no base format
		{"test.csv.gz.bz2", false}, // Double compression
		{".csv", true},             // Hidden file
		{"a.very.long.filename.with.many.dots.csv", true},
	}

	for _, tc := range testCases {
		t.Run(tc.fileName, func(t *testing.T) {
			result := isSupportedFile(tc.fileName)
			if result != tc.isSupported {
				t.Errorf("isSupportedFile(%q) = %v, expected %v", tc.fileName, result, tc.isSupported)
			}
		})
	}
}

func Test_OpenReaderEdgeCases(t *testing.T) {
	t.Parallel()

	// Test with non-existent file
	t.Run("Non-existent file", func(t *testing.T) {
		file := newFile("non_existent_file.csv")
		_, closer, err := file.openReader()
		if err == nil {
			_ = closer() //nolint:errcheck
			t.Error("Expected error for non-existent file, got nil")
		}
	})

	// Test with malformed compressed file
	t.Run("Invalid gzip file", func(t *testing.T) {
		// Create a file with .gz extension but invalid gzip content
		tmpFile, err := os.CreateTemp(t.TempDir(), "invalid_*.csv.gz")
		require.NoError(t, err, "Failed to create file or perform operation")
		defer os.Remove(tmpFile.Name())

		// Write non-gzip content
		if _, err := tmpFile.WriteString("This is not gzip content"); err != nil {
			t.Fatal(err)
		}
		_ = tmpFile.Close() // Ignore close error in test cleanup

		file := newFile(tmpFile.Name())
		_, closer, err := file.openReader()
		if err == nil {
			_ = closer() //nolint:errcheck
			t.Error("Expected error for invalid gzip file, got nil")
		}
	})

	// Test with valid compressed files
	compressionTypes := []struct {
		ext     string
		content []byte
	}{
		{".gz", []byte{0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0x4b, 0x4c, 0x52, 0xc8, 0x4b, 0xcc, 0x4d, 0xe5, 0x02, 0x00, 0x00, 0x00, 0xff, 0xff, 0x01, 0x00, 0x00, 0xff, 0xff, 0x09, 0x9e, 0x2d, 0x1b, 0x09, 0x00, 0x00, 0x00}}, // "id,name\n" gzipped
	}

	for _, ct := range compressionTypes {
		t.Run("Valid"+ct.ext, func(t *testing.T) {
			tmpFile, err := os.CreateTemp(t.TempDir(), "valid_*"+".csv"+ct.ext)
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(tmpFile.Name())

			if _, err := tmpFile.Write(ct.content); err != nil {
				t.Fatal(err)
			}
			_ = tmpFile.Close() // Ignore close error in test cleanup

			file := newFile(tmpFile.Name())
			reader, closer, err := file.openReader()
			if err != nil {
				t.Errorf("Unexpected error for valid %s file: %v", ct.ext, err)
				return
			}
			defer closer()

			if reader == nil {
				t.Error("Reader should not be nil for valid compressed file")
			}
		})
	}
}

func TestFile_ToTable_DuplicateColumns(t *testing.T) {
	t.Parallel()

	t.Run("CSV with duplicate column names", func(t *testing.T) {
		t.Parallel()

		tmpDir := t.TempDir()
		csvFile := filepath.Join(tmpDir, "duplicate_columns.csv")

		csvContent := `id,name,id,email
1,John,10,john@example.com
2,Jane,20,jane@example.com`

		err := os.WriteFile(csvFile, []byte(csvContent), 0600)
		require.NoError(t, err, "Failed to create file or perform operation")

		file := newFile(csvFile)
		_, err = file.toTable()
		if err == nil {
			t.Error("expected error for CSV with duplicate column names")
			return
		}

		if !errors.Is(err, errDuplicateColumnName) {
			t.Errorf("expected errDuplicateColumnName, got: %v", err)
		}

		// Verify error message contains the duplicate column name
		if !strings.Contains(err.Error(), "id") {
			t.Errorf("error message should contain duplicate column name 'id', got: %s", err.Error())
		}
	})

	t.Run("TSV with duplicate column names", func(t *testing.T) {
		t.Parallel()

		tmpDir := t.TempDir()
		tsvFile := filepath.Join(tmpDir, "duplicate_columns.tsv")

		tsvContent := `id	name	id	email
1	John	10	john@example.com
2	Jane	20	jane@example.com`

		err := os.WriteFile(tsvFile, []byte(tsvContent), 0600)
		require.NoError(t, err, "Failed to create file or perform operation")

		file := newFile(tsvFile)
		_, err = file.toTable()
		if err == nil {
			t.Error("expected error for TSV with duplicate column names")
			return
		}

		if !errors.Is(err, errDuplicateColumnName) {
			t.Errorf("expected errDuplicateColumnName, got: %v", err)
		}

		// Verify error message contains the duplicate column name
		if !strings.Contains(err.Error(), "id") {
			t.Errorf("error message should contain duplicate column name 'id', got: %s", err.Error())
		}
	})

	t.Run("CSV with multiple duplicate column names", func(t *testing.T) {
		t.Parallel()

		tmpDir := t.TempDir()
		csvFile := filepath.Join(tmpDir, "multiple_duplicates.csv")

		csvContent := `name,age,name,email,age
John,25,Doe,john@example.com,26`

		err := os.WriteFile(csvFile, []byte(csvContent), 0600)
		require.NoError(t, err, "Failed to create file or perform operation")

		file := newFile(csvFile)
		_, err = file.toTable()
		if err == nil {
			t.Error("expected error for CSV with multiple duplicate column names")
			return
		}

		if !errors.Is(err, errDuplicateColumnName) {
			t.Errorf("expected errDuplicateColumnName, got: %v", err)
		}
	})

	t.Run("CSV without duplicate column names", func(t *testing.T) {
		t.Parallel()

		tmpDir := t.TempDir()
		csvFile := filepath.Join(tmpDir, "valid.csv")

		csvContent := `id,name,age,email
1,John,25,john@example.com
2,Jane,30,jane@example.com`

		err := os.WriteFile(csvFile, []byte(csvContent), 0600)
		require.NoError(t, err, "Failed to create file or perform operation")

		file := newFile(csvFile)
		table, err := file.toTable()
		if err != nil {
			t.Errorf("expected no error for valid CSV, got: %v", err)
			return
		}

		if table == nil {
			t.Error("expected valid table, got nil")
		}
	})
}

// TestFileTypeExtension tests the extension() method for various FileTypes
func TestFileTypeExtension(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		fileType FileType
		expected string
	}{
		{"CSV", FileTypeCSV, ".csv"},
		{"TSV", FileTypeTSV, ".tsv"},
		{"LTSV", FileTypeLTSV, ".ltsv"},
		{"Parquet", FileTypeParquet, ".parquet"},
		{"CSV GZ", FileTypeCSVGZ, ".csv.gz"},
		{"TSV BZ2", FileTypeTSVBZ2, ".tsv.bz2"},
		{"LTSV XZ", FileTypeLTSVXZ, ".ltsv.xz"},
		{"CSV ZSTD", FileTypeCSVZSTD, ".csv.zst"},
		{"XLSX", FileTypeXLSX, ".xlsx"},
		{"XLSX GZ", FileTypeXLSXGZ, ".xlsx.gz"},
		{"XLSX BZ2", FileTypeXLSXBZ2, ".xlsx.bz2"},
		{"XLSX XZ", FileTypeXLSXXZ, ".xlsx.xz"},
		{"XLSX ZSTD", FileTypeXLSXZSTD, ".xlsx.zst"},
		{"Parquet GZ", FileTypeParquetGZ, ".parquet.gz"},
		{"Parquet BZ2", FileTypeParquetBZ2, ".parquet.bz2"},
		{"Parquet XZ", FileTypeParquetXZ, ".parquet.xz"},
		{"Parquet ZSTD", FileTypeParquetZSTD, ".parquet.zst"},
		{"Unsupported", FileTypeUnsupported, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.fileType.extension(); got != tt.expected {
				t.Errorf("FileType.extension() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// TestOpenReaderEdgeCases tests edge cases in openReader function
func TestOpenReaderEdgeCases(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	t.Run("openReader with non-existent file", func(t *testing.T) {
		t.Parallel()

		file := newFile(filepath.Join(tmpDir, "nonexistent.csv"))
		_, closer, err := file.openReader()
		if err == nil {
			_ = closer() //nolint:errcheck
			t.Error("expected error for non-existent file")
		}
	})

	t.Run("openReader with unsupported compressed file", func(t *testing.T) {
		t.Parallel()

		// Create a fake compressed file with invalid content
		fakeBz2 := filepath.Join(tmpDir, "fake.csv.bz2")
		if err := os.WriteFile(fakeBz2, []byte("not bz2 data"), 0600); err != nil {
			t.Fatal(err)
		}

		file := newFile(fakeBz2)
		reader, closer, err := file.openReader()
		if err == nil {
			defer closer()
			// Try to read from it - should fail
			buf := make([]byte, 10)
			_, readErr := reader.Read(buf)
			if readErr == nil {
				t.Error("expected error when reading invalid bz2 data")
			}
		}
	})
}

func TestGetSupportedFilePatterns(t *testing.T) {
	t.Parallel()

	patterns := supportedFileExtPatterns()

	// Should have 25 patterns: 5 base extensions × 5 compression variants (including none)
	expectedCount := 25
	if len(patterns) != expectedCount {
		t.Errorf("GetSupportedFilePatterns() returned %d patterns, want %d", len(patterns), expectedCount)
	}

	// Check that all expected patterns are present
	expectedPatterns := []string{
		"*.csv", "*.csv.gz", "*.csv.bz2", "*.csv.xz", "*.csv.zst",
		"*.tsv", "*.tsv.gz", "*.tsv.bz2", "*.tsv.xz", "*.tsv.zst",
		"*.ltsv", "*.ltsv.gz", "*.ltsv.bz2", "*.ltsv.xz", "*.ltsv.zst",
		"*.parquet", "*.parquet.gz", "*.parquet.bz2", "*.parquet.xz", "*.parquet.zst",
		"*.xlsx", "*.xlsx.gz", "*.xlsx.bz2", "*.xlsx.xz", "*.xlsx.zst",
	}

	for _, expected := range expectedPatterns {
		found := false
		for _, pattern := range patterns {
			if pattern == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("GetSupportedFilePatterns() missing pattern: %s", expected)
		}
	}
}

func TestFile_ToTable_ErrorCases(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	t.Run("corrupted CSV file", func(t *testing.T) {
		t.Parallel()

		// Create a file with invalid CSV format
		corruptedFile := filepath.Join(tmpDir, "corrupted.csv")
		if err := os.WriteFile(corruptedFile, []byte("name,age\n\"unclosed quote"), 0600); err != nil {
			t.Fatal(err)
		}

		file := newFile(corruptedFile)
		_, err := file.toTable()
		if err == nil {
			t.Error("expected error for corrupted CSV file")
		}
	})

	t.Run("empty CSV file", func(t *testing.T) {
		t.Parallel()

		emptyFile := filepath.Join(tmpDir, "empty.csv")
		if err := os.WriteFile(emptyFile, []byte(""), 0600); err != nil {
			t.Fatal(err)
		}

		file := newFile(emptyFile)
		_, err := file.toTable()
		if err == nil {
			t.Error("expected error for empty CSV file")
		}
	})

	t.Run("non-existent file", func(t *testing.T) {
		t.Parallel()

		nonExistentFile := filepath.Join(tmpDir, "nonexistent.csv")
		file := newFile(nonExistentFile)
		_, err := file.toTable()
		if err == nil {
			t.Error("expected error for non-existent file")
		}
	})

	t.Run("unsupported file type", func(t *testing.T) {
		t.Parallel()

		textFile := filepath.Join(tmpDir, "test.txt")
		if err := os.WriteFile(textFile, []byte("plain text"), 0600); err != nil {
			t.Fatal(err)
		}

		file := newFile(textFile)
		_, err := file.toTable()
		if err == nil {
			t.Error("expected error for unsupported file type")
		}
	})

	t.Run("corrupted compressed file", func(t *testing.T) {
		t.Parallel()

		// Create a file with .gz extension but invalid gzip content
		corruptedGzFile := filepath.Join(tmpDir, "corrupted.csv.gz")
		if err := os.WriteFile(corruptedGzFile, []byte("not gzip content"), 0600); err != nil {
			t.Fatal(err)
		}

		file := newFile(corruptedGzFile)
		_, err := file.toTable()
		if err == nil {
			t.Error("expected error for corrupted gzip file")
		}
	})

	t.Run("CSV with inconsistent columns", func(t *testing.T) {
		t.Parallel()

		inconsistentFile := filepath.Join(tmpDir, "inconsistent.csv")
		content := "name,age\nAlice,25\nBob,30,extra\n"
		if err := os.WriteFile(inconsistentFile, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}

		file := newFile(inconsistentFile)
		_, err := file.toTable()
		// CSV parser should return error for inconsistent column count
		if err == nil {
			t.Error("expected error for inconsistent CSV columns")
		}
	})

	t.Run("LTSV with partially invalid format", func(t *testing.T) {
		t.Parallel()

		partiallyInvalidLTSV := filepath.Join(tmpDir, "partially_invalid.ltsv")
		// LTSV with some valid and some invalid lines
		content := "name:Alice\tage:25\ninvalid_line_without_colon\nname:Bob\tage:30\n"
		if err := os.WriteFile(partiallyInvalidLTSV, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}

		file := newFile(partiallyInvalidLTSV)
		_, err := file.toTable()
		// This should succeed as the LTSV parser handles partially invalid data
		if err != nil {
			t.Errorf("LTSV parser should handle partially invalid data gracefully, got: %v", err)
		}
	})
}

func TestCompressionDetection_EdgeCases(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name               string
		filePath           string
		expectedCompressed bool
		expectedGZ         bool
		expectedBZ2        bool
		expectedXZ         bool
		expectedZSTD       bool
	}{
		{
			name:               "double compression extension",
			filePath:           "file.csv.gz.bz2",
			expectedCompressed: true,
			expectedGZ:         false,
			expectedBZ2:        true,
			expectedXZ:         false,
			expectedZSTD:       false,
		},
		{
			name:               "no compression",
			filePath:           "file.csv",
			expectedCompressed: false,
			expectedGZ:         false,
			expectedBZ2:        false,
			expectedXZ:         false,
			expectedZSTD:       false,
		},
		{
			name:               "compression in middle of filename",
			filePath:           "file.gz.csv",
			expectedCompressed: false,
			expectedGZ:         false,
			expectedBZ2:        false,
			expectedXZ:         false,
			expectedZSTD:       false,
		},
		{
			name:               "case sensitive compression",
			filePath:           "file.csv.GZ",
			expectedCompressed: false, // Should be case-sensitive
			expectedGZ:         false,
			expectedBZ2:        false,
			expectedXZ:         false,
			expectedZSTD:       false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			file := newFile(tc.filePath)

			if file.isCompressed() != tc.expectedCompressed {
				t.Errorf("IsCompressed() = %v, want %v", file.isCompressed(), tc.expectedCompressed)
			}
			if file.isGZ() != tc.expectedGZ {
				t.Errorf("IsGZ() = %v, want %v", file.isGZ(), tc.expectedGZ)
			}
			if file.isBZ2() != tc.expectedBZ2 {
				t.Errorf("IsBZ2() = %v, want %v", file.isBZ2(), tc.expectedBZ2)
			}
			if file.isXZ() != tc.expectedXZ {
				t.Errorf("IsXZ() = %v, want %v", file.isXZ(), tc.expectedXZ)
			}
			if file.isZSTD() != tc.expectedZSTD {
				t.Errorf("IsZSTD() = %v, want %v", file.isZSTD(), tc.expectedZSTD)
			}
		})
	}
}

// TestIsSupportedExtension tests the IsSupportedExtension function
func TestIsSupportedExtension(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ext      string
		expected bool
	}{
		{".csv", true},
		{".tsv", true},
		{".ltsv", true},
		{".csv.gz", true},
		{".tsv.bz2", true},
		{".ltsv.xz", true},
		{".xlsx", true},
		{".xlsx.gz", true},
		{".txt", false},
		{".json", false},
		{".CSV", true},    // Should work with uppercase
		{".TSV.GZ", true}, // Should work with uppercase
		{".XLSX", true},   // Should work with uppercase
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.ext, func(t *testing.T) {
			result := isSupportedExtension(tt.ext)
			if result != tt.expected {
				t.Errorf("isSupportedExtension(%q) = %v, want %v", tt.ext, result, tt.expected)
			}
		})
	}
}

// TestGetFileExtension tests the deprecated GetFileExtension function
func TestGetFileExtension(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fileType FileType
		expected string
	}{
		{FileTypeCSV, ".csv"},
		{FileTypeTSV, ".tsv"},
		{FileTypeLTSV, ".ltsv"},
		{FileTypeCSVGZ, ".csv.gz"},
		{FileTypeUnsupported, ""},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := getFileExtension(tt.fileType)
			if result != tt.expected {
				t.Errorf("getFileExtension(%v) = %s, want %s", tt.fileType, result, tt.expected)
			}
		})
	}
}

// TestGetBaseFileType tests the deprecated GetBaseFileType function
func TestGetBaseFileType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fileType FileType
		expected FileType
	}{
		{FileTypeCSV, FileTypeCSV},
		{FileTypeCSVGZ, FileTypeCSV},
		{FileTypeCSVBZ2, FileTypeCSV},
		{FileTypeTSV, FileTypeTSV},
		{FileTypeTSVGZ, FileTypeTSV},
		{FileTypeLTSV, FileTypeLTSV},
		{FileTypeLTSVXZ, FileTypeLTSV},
		{FileTypeUnsupported, FileTypeUnsupported},
	}

	for _, tt := range tests {
		t.Run(tt.fileType.extension(), func(t *testing.T) {
			result := getBaseFileType(tt.fileType)
			if result != tt.expected {
				t.Errorf("getBaseFileType(%v) = %v, want %v", tt.fileType, result, tt.expected)
			}
		})
	}
}

// TestCreateDecompressedReader tests the createDecompressedReader function
func TestCreateDecompressedReader(t *testing.T) {
	t.Parallel()

	t.Run("unsupported compression", func(t *testing.T) {
		t.Parallel()

		parser := newStreamingParser(FileTypeCSV, "test", 1024)
		reader := strings.NewReader("test data")

		// Test with unsupported compression (should return original reader)
		result, closeFunc, err := parser.createDecompressedReader(reader)
		if err != nil {
			t.Errorf("createDecompressedReader should not error for uncompressed data: %v", err)
		}

		if result != reader {
			t.Error("createDecompressedReader should return original reader for uncompressed data")
		}

		if closeFunc != nil {
			t.Error("createDecompressedReader should not return close function for uncompressed data")
		}
	})

	t.Run("gzip compression", func(t *testing.T) {
		t.Parallel()

		// Create gzip compressed data
		originalData := "name,age\nAlice,30\nBob,25\n"
		var buf strings.Builder
		gz := gzip.NewWriter(&buf)
		if _, err := gz.Write([]byte(originalData)); err != nil {
			t.Fatalf("Failed to write to gzip: %v", err)
		}
		if err := gz.Close(); err != nil {
			t.Fatalf("Failed to close gzip writer: %v", err)
		}

		parser := newStreamingParser(FileTypeCSVGZ, "test", 1024)
		reader := strings.NewReader(buf.String())

		result, closeFunc, err := parser.createDecompressedReader(reader)
		if err != nil {
			t.Fatalf("createDecompressedReader failed for gzip: %v", err)
		}

		if result == reader {
			t.Error("createDecompressedReader should return different reader for compressed data")
		}

		if closeFunc == nil {
			t.Error("createDecompressedReader should return close function for compressed data")
		}

		// Clean up
		if closeFunc != nil {
			if err := closeFunc(); err != nil {
				t.Errorf("Failed to close decompressor: %v", err)
			}
		}
	})

	t.Run("invalid gzip data", func(t *testing.T) {
		t.Parallel()

		parser := newStreamingParser(FileTypeCSVGZ, "test", 1024)
		reader := strings.NewReader("invalid gzip data")

		_, _, err := parser.createDecompressedReader(reader)
		if err == nil {
			t.Error("createDecompressedReader should error for invalid gzip data")
		}
	})
}

// TestFile_ParseXLSX tests XLSX parsing functionality
func TestFile_ParseXLSX(t *testing.T) {
	t.Parallel()

	t.Run("Simple XLSX file with multiple sheets", func(t *testing.T) {
		t.Parallel()

		// Create a temporary XLSX file for testing
		tmpDir := t.TempDir()
		xlsxFile := filepath.Join(tmpDir, "test.xlsx")

		// Create XLSX file with test data
		f := excelize.NewFile()

		// Create Sheet1 with some data
		if err := f.SetCellValue("Sheet1", "A1", "Alice"); err != nil {
			t.Fatal(err)
		}
		if err := f.SetCellValue("Sheet1", "A2", "Bob"); err != nil {
			t.Fatal(err)
		}
		if err := f.SetCellValue("Sheet1", "A3", "Charlie"); err != nil {
			t.Fatal(err)
		}

		// Create Sheet2 with some data
		if _, err := f.NewSheet("Sheet2"); err != nil {
			t.Fatal(err)
		}
		if err := f.SetCellValue("Sheet2", "A1", "Data1"); err != nil {
			t.Fatal(err)
		}
		if err := f.SetCellValue("Sheet2", "A2", "Data2"); err != nil {
			t.Fatal(err)
		}

		// Create Sheet3 with some data
		if _, err := f.NewSheet("Sheet3"); err != nil {
			t.Fatal(err)
		}
		if err := f.SetCellValue("Sheet3", "A1", "Value1"); err != nil {
			t.Fatal(err)
		}
		if err := f.SetCellValue("Sheet3", "A2", "Value2"); err != nil {
			t.Fatal(err)
		}
		if err := f.SetCellValue("Sheet3", "A3", "Value3"); err != nil {
			t.Fatal(err)
		}

		// Save the file
		if err := f.SaveAs(xlsxFile); err != nil {
			t.Fatal(err)
		}
		_ = f.Close() // Ignore close error in test

		// Parse the file - should process first sheet only
		file := newFile(xlsxFile)
		table, err := file.toTable()
		if err != nil {
			t.Fatalf("Failed to parse XLSX file: %v", err)
		}

		if table == nil {
			t.Fatal("Table should not be nil")
		}

		// Check headers (should be from first row of first sheet)
		headers := table.getHeader()
		expectedHeaders := []string{"Alice"} // First row of Sheet1
		if len(headers) != len(expectedHeaders) {
			t.Errorf("Expected %d headers, got %d", len(expectedHeaders), len(headers))
		}

		for i, expected := range expectedHeaders {
			if i < len(headers) && headers[i] != expected {
				t.Errorf("Expected header[%d] to be %s, got %s", i, expected, headers[i])
			}
		}

		// Check records (should be from remaining rows of first sheet only)
		records := table.getRecords()
		if len(records) != 2 { // "Bob", "Charlie"
			t.Errorf("Expected 2 records, got %d", len(records))
		}
	})

	t.Run("Empty XLSX file", func(t *testing.T) {
		t.Parallel()

		tmpDir := t.TempDir()
		xlsxFile := filepath.Join(tmpDir, "empty.xlsx")

		// Create empty XLSX file
		f := excelize.NewFile()
		if err := f.SaveAs(xlsxFile); err != nil {
			t.Fatal(err)
		}
		_ = f.Close() // Ignore close error in test

		file := newFile(xlsxFile)
		_, err := file.toTable()
		if err == nil {
			t.Error("Expected error for empty XLSX file")
		}
	})

	t.Run("XLSX file with single sheet", func(t *testing.T) {
		t.Parallel()

		tmpDir := t.TempDir()
		xlsxFile := filepath.Join(tmpDir, "single_sheet.xlsx")

		// Create XLSX file with single sheet
		f := excelize.NewFile()
		if err := f.SetCellValue("Sheet1", "A1", "Header1"); err != nil {
			t.Fatal(err)
		}
		if err := f.SetCellValue("Sheet1", "A2", "Row1"); err != nil {
			t.Fatal(err)
		}
		if err := f.SetCellValue("Sheet1", "A3", "Row2"); err != nil {
			t.Fatal(err)
		}

		if err := f.SaveAs(xlsxFile); err != nil {
			t.Fatal(err)
		}
		_ = f.Close() // Ignore close error in test

		file := newFile(xlsxFile)
		table, err := file.toTable()
		if err != nil {
			t.Fatalf("Failed to parse single sheet XLSX file: %v", err)
		}

		headers := table.getHeader()
		if len(headers) != 1 || headers[0] != "Header1" {
			t.Errorf("Expected single header 'Header1', got %v", headers)
		}

		records := table.getRecords()
		if len(records) != 2 {
			t.Errorf("Expected 2 records, got %d", len(records))
		}

		// Check that records contain the data rows (Row1, Row2)
		expectedRecords := [][]string{{"Row1"}, {"Row2"}}
		for i, expectedRecord := range expectedRecords {
			if i < len(records) {
				for j, expectedValue := range expectedRecord {
					if j < len(records[i]) && records[i][j] != expectedValue {
						t.Errorf("Record[%d][%d]: expected %s, got %s", i, j, expectedValue, records[i][j])
					}
				}
			}
		}
	})
}

// TestFile_IsXLSX tests the isXLSX method
func TestFile_IsXLSX(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		filePath string
		expected bool
	}{
		{
			name:     "XLSX file",
			filePath: "test.xlsx",
			expected: true,
		},
		{
			name:     "Compressed XLSX file",
			filePath: "test.xlsx.gz",
			expected: true,
		},
		{
			name:     "CSV file",
			filePath: "test.csv",
			expected: false,
		},
		{
			name:     "TSV file",
			filePath: "test.tsv",
			expected: false,
		},
		{
			name:     "Unsupported file",
			filePath: "test.txt",
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			file := newFile(tc.filePath)
			result := file.isXLSX()
			if result != tc.expected {
				t.Errorf("IsXLSX() = %v, expected %v for %s", result, tc.expected, tc.filePath)
			}
		})
	}
}

// TestConvertXLSXRowsToTable tests the convertXLSXRowsToTable function
func TestConvertXLSXRowsToTable(t *testing.T) {
	t.Parallel()

	t.Run("Normal conversion", func(t *testing.T) {
		t.Parallel()

		rows := [][]string{
			{"Name", "Age", "City"},
			{"Alice", "25", "Tokyo"},
			{"Bob", "30", "Osaka"},
		}

		headers, records := convertXLSXRowsToTable(rows)

		// Check headers
		expectedHeaders := []string{"Name", "Age", "City"}
		if len(headers) != len(expectedHeaders) {
			t.Errorf("Expected %d headers, got %d", len(expectedHeaders), len(headers))
		}
		for i, expected := range expectedHeaders {
			if headers[i] != expected {
				t.Errorf("Header[%d] = %s, expected %s", i, headers[i], expected)
			}
		}

		// Check records
		expectedRecords := [][]string{
			{"Alice", "25", "Tokyo"},
			{"Bob", "30", "Osaka"},
		}
		if len(records) != len(expectedRecords) {
			t.Errorf("Expected %d records, got %d", len(expectedRecords), len(records))
		}
		for i, expectedRecord := range expectedRecords {
			if len(records[i]) != len(expectedRecord) {
				t.Errorf("Record[%d] length = %d, expected %d", i, len(records[i]), len(expectedRecord))
			}
			for j, expected := range expectedRecord {
				if records[i][j] != expected {
					t.Errorf("Record[%d][%d] = %s, expected %s", i, j, records[i][j], expected)
				}
			}
		}
	})

	t.Run("Empty rows", func(t *testing.T) {
		t.Parallel()

		var rows [][]string
		headers, records := convertXLSXRowsToTable(rows)

		if len(headers) != 0 {
			t.Errorf("Expected empty headers, got %d", len(headers))
		}
		if len(records) != 0 {
			t.Errorf("Expected empty records, got %d", len(records))
		}
	})

	t.Run("Only headers", func(t *testing.T) {
		t.Parallel()

		rows := [][]string{
			{"Name", "Age"},
		}

		headers, records := convertXLSXRowsToTable(rows)

		expectedHeaders := []string{"Name", "Age"}
		if len(headers) != len(expectedHeaders) {
			t.Errorf("Expected %d headers, got %d", len(expectedHeaders), len(headers))
		}
		if len(records) != 0 {
			t.Errorf("Expected no records, got %d", len(records))
		}
	})

	t.Run("Uneven rows (padding)", func(t *testing.T) {
		t.Parallel()

		rows := [][]string{
			{"Name", "Age", "City"},
			{"Alice", "25"},                 // Missing city
			{"Bob", "30", "Osaka", "Extra"}, // Extra field (ignored)
		}

		headers, records := convertXLSXRowsToTable(rows)

		// Check headers length
		if len(headers) != 3 {
			t.Errorf("Expected 3 headers, got %d", len(headers))
		}

		// Check that missing fields are padded
		if records[0][2] != "" {
			t.Errorf("Expected empty padding for missing field, got %s", records[0][2])
		}
		// Check that extra fields don't cause issues
		if len(records[1]) != 3 {
			t.Errorf("Expected record length 3, got %d", len(records[1]))
		}
	})
}
