package filesql

import (
	"compress/gzip"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
			expected: FileTypeCSV,
		},
		{
			name:     "Compressed TSV file",
			path:     "test.tsv.bz2",
			expected: FileTypeTSV,
		},
		{
			name:     "Compressed LTSV file",
			path:     "test.ltsv.xz",
			expected: FileTypeLTSV,
		},
		{
			name:     "Zstd compressed CSV file",
			path:     "test.csv.zst",
			expected: FileTypeCSV,
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
			if file.getFileType() != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, file.getFileType())
			}
			if file.getPath() != tt.path {
				t.Errorf("expected %s, got %s", tt.path, file.getPath())
			}
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
			if file.isCompressed() != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, file.isCompressed())
			}
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
			if file.isGZ() != tt.isGZ {
				t.Errorf("IsGZ() expected %v, got %v", tt.isGZ, file.isGZ())
			}
			if file.isBZ2() != tt.isBZ2 {
				t.Errorf("IsBZ2() expected %v, got %v", tt.isBZ2, file.isBZ2())
			}
			if file.isXZ() != tt.isXZ {
				t.Errorf("IsXZ() expected %v, got %v", tt.isXZ, file.isXZ())
			}
			if file.isZSTD() != tt.isZSTD {
				t.Errorf("IsZSTD() expected %v, got %v", tt.isZSTD, file.isZSTD())
			}
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
	if err != nil {
		t.Fatal(err)
	}

	file := newFile(csvFile)
	table, err := file.toTable()
	if err != nil {
		t.Fatal(err)
	}

	expectedHeader := header{"name", "age", "city"}
	if !table.getHeader().equal(expectedHeader) {
		t.Errorf("expected header %v, got %v", expectedHeader, table.getHeader())
	}

	if len(table.getRecords()) != 3 {
		t.Errorf("expected 3 records, got %d", len(table.getRecords()))
	}

	expectedFirstRecord := record{"John", "25", "Tokyo"}
	if !table.getRecords()[0].equal(expectedFirstRecord) {
		t.Errorf("expected first record %v, got %v", expectedFirstRecord, table.getRecords()[0])
	}
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
	if err != nil {
		t.Fatal(err)
	}

	file := newFile(tsvFile)
	table, err := file.toTable()
	if err != nil {
		t.Fatal(err)
	}

	expectedHeader := header{"name", "age", "city"}
	if !table.getHeader().equal(expectedHeader) {
		t.Errorf("expected header %v, got %v", expectedHeader, table.getHeader())
	}

	if len(table.getRecords()) != 3 {
		t.Errorf("expected 3 records, got %d", len(table.getRecords()))
	}

	expectedFirstRecord := record{"John", "25", "Tokyo"}
	if !table.getRecords()[0].equal(expectedFirstRecord) {
		t.Errorf("expected first record %v, got %v", expectedFirstRecord, table.getRecords()[0])
	}
}

func TestFile_ToTable_LTSV(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	ltsvFile := filepath.Join(tmpDir, "test.ltsv")

	ltsvContent := `name:John	age:25	city:Tokyo
name:Alice	age:30	city:Osaka
name:Bob	age:35	city:Kyoto`

	err := os.WriteFile(ltsvFile, []byte(ltsvContent), 0600)
	if err != nil {
		t.Fatal(err)
	}

	file := newFile(ltsvFile)
	table, err := file.toTable()
	if err != nil {
		t.Fatal(err)
	}

	if len(table.getRecords()) != 3 {
		t.Errorf("expected 3 records, got %d", len(table.getRecords()))
	}

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
	if err != nil {
		t.Fatal(err)
	}

	expectedHeader := header{"name", "age", "city"}
	if !table.getHeader().equal(expectedHeader) {
		t.Errorf("expected header %v, got %v", expectedHeader, table.getHeader())
	}

	if len(table.getRecords()) != 2 {
		t.Errorf("expected 2 records, got %d", len(table.getRecords()))
	}
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
	if err == nil {
		t.Error("expected error for unsupported file format")
	}
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
	if err == nil {
		t.Error("expected error for empty file")
	}
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
			filePath: "/home/user/data.csv",
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
			if result != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
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

			if file.isCSV() != tc.expectedCSV {
				t.Errorf("IsCSV() = %v, expected %v for %s", file.isCSV(), tc.expectedCSV, tc.filePath)
			}

			if file.isTSV() != tc.expectedTSV {
				t.Errorf("IsTSV() = %v, expected %v for %s", file.isTSV(), tc.expectedTSV, tc.filePath)
			}

			if file.isLTSV() != tc.expectedLTSV {
				t.Errorf("IsLTSV() = %v, expected %v for %s", file.isLTSV(), tc.expectedLTSV, tc.filePath)
			}
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
		{"test.xlsx", false},
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
		if err != nil {
			t.Fatal(err)
		}
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
		if err != nil {
			t.Fatal(err)
		}

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
		if err != nil {
			t.Fatal(err)
		}

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
		if err != nil {
			t.Fatal(err)
		}

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
		if err != nil {
			t.Fatal(err)
		}

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

func TestGetSupportedFilePatterns(t *testing.T) {
	t.Parallel()

	patterns := supportedFileExtPatterns()

	// Should have 15 patterns: 3 base extensions × 5 compression variants (including none)
	expectedCount := 15
	if len(patterns) != expectedCount {
		t.Errorf("GetSupportedFilePatterns() returned %d patterns, want %d", len(patterns), expectedCount)
	}

	// Check that all expected patterns are present
	expectedPatterns := []string{
		"*.csv", "*.csv.gz", "*.csv.bz2", "*.csv.xz", "*.csv.zst",
		"*.tsv", "*.tsv.gz", "*.tsv.bz2", "*.tsv.xz", "*.tsv.zst",
		"*.ltsv", "*.ltsv.gz", "*.ltsv.bz2", "*.ltsv.xz", "*.ltsv.zst",
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
		{".txt", false},
		{".json", false},
		{".CSV", true},    // Should work with uppercase
		{".TSV.GZ", true}, // Should work with uppercase
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
