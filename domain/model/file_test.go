package model

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

			file := NewFile(tt.path)
			if file.Type() != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, file.Type())
			}
			if file.Path() != tt.path {
				t.Errorf("expected %s, got %s", tt.path, file.Path())
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

			file := NewFile(tt.path)
			if file.IsCompressed() != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, file.IsCompressed())
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

			file := NewFile(tt.path)
			if file.IsGZ() != tt.isGZ {
				t.Errorf("IsGZ() expected %v, got %v", tt.isGZ, file.IsGZ())
			}
			if file.IsBZ2() != tt.isBZ2 {
				t.Errorf("IsBZ2() expected %v, got %v", tt.isBZ2, file.IsBZ2())
			}
			if file.IsXZ() != tt.isXZ {
				t.Errorf("IsXZ() expected %v, got %v", tt.isXZ, file.IsXZ())
			}
			if file.IsZSTD() != tt.isZSTD {
				t.Errorf("IsZSTD() expected %v, got %v", tt.isZSTD, file.IsZSTD())
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

	err := os.WriteFile(csvFile, []byte(csvContent), 0644)
	if err != nil {
		t.Fatal(err)
	}

	file := NewFile(csvFile)
	table, err := file.ToTable()
	if err != nil {
		t.Fatal(err)
	}

	expectedHeader := Header{"name", "age", "city"}
	if !table.Header().Equal(expectedHeader) {
		t.Errorf("expected header %v, got %v", expectedHeader, table.Header())
	}

	if len(table.Records()) != 3 {
		t.Errorf("expected 3 records, got %d", len(table.Records()))
	}

	expectedFirstRecord := Record{"John", "25", "Tokyo"}
	if !table.Records()[0].Equal(expectedFirstRecord) {
		t.Errorf("expected first record %v, got %v", expectedFirstRecord, table.Records()[0])
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

	err := os.WriteFile(tsvFile, []byte(tsvContent), 0644)
	if err != nil {
		t.Fatal(err)
	}

	file := NewFile(tsvFile)
	table, err := file.ToTable()
	if err != nil {
		t.Fatal(err)
	}

	expectedHeader := Header{"name", "age", "city"}
	if !table.Header().Equal(expectedHeader) {
		t.Errorf("expected header %v, got %v", expectedHeader, table.Header())
	}

	if len(table.Records()) != 3 {
		t.Errorf("expected 3 records, got %d", len(table.Records()))
	}

	expectedFirstRecord := Record{"John", "25", "Tokyo"}
	if !table.Records()[0].Equal(expectedFirstRecord) {
		t.Errorf("expected first record %v, got %v", expectedFirstRecord, table.Records()[0])
	}
}

func TestFile_ToTable_LTSV(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	ltsvFile := filepath.Join(tmpDir, "test.ltsv")

	ltsvContent := `name:John	age:25	city:Tokyo
name:Alice	age:30	city:Osaka
name:Bob	age:35	city:Kyoto`

	err := os.WriteFile(ltsvFile, []byte(ltsvContent), 0644)
	if err != nil {
		t.Fatal(err)
	}

	file := NewFile(ltsvFile)
	table, err := file.ToTable()
	if err != nil {
		t.Fatal(err)
	}

	if len(table.Records()) != 3 {
		t.Errorf("expected 3 records, got %d", len(table.Records()))
	}

	// LTSV header order may vary due to map iteration
	header := table.Header()
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
	f, err := os.Create(csvFile)
	if err != nil {
		t.Fatal(err)
	}

	gw := gzip.NewWriter(f)
	_, err = gw.Write([]byte(csvContent))
	if err != nil {
		t.Fatal(err)
	}
	gw.Close()
	f.Close()

	file := NewFile(csvFile)
	table, err := file.ToTable()
	if err != nil {
		t.Fatal(err)
	}

	expectedHeader := Header{"name", "age", "city"}
	if !table.Header().Equal(expectedHeader) {
		t.Errorf("expected header %v, got %v", expectedHeader, table.Header())
	}

	if len(table.Records()) != 2 {
		t.Errorf("expected 2 records, got %d", len(table.Records()))
	}
}

func TestFile_ToTable_UnsupportedFormat(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	txtFile := filepath.Join(tmpDir, "test.txt")

	err := os.WriteFile(txtFile, []byte("some content"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	file := NewFile(txtFile)
	_, err = file.ToTable()
	if err == nil {
		t.Error("expected error for unsupported file format")
	}
}

func TestFile_ToTable_EmptyFile(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	csvFile := filepath.Join(tmpDir, "empty.csv")

	err := os.WriteFile(csvFile, []byte(""), 0644)
	if err != nil {
		t.Fatal(err)
	}

	file := NewFile(csvFile)
	_, err = file.ToTable()
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

			result := TableFromFilePath(tt.filePath)
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
			file := NewFile(tc.filePath)

			if file.IsCSV() != tc.expectedCSV {
				t.Errorf("IsCSV() = %v, expected %v for %s", file.IsCSV(), tc.expectedCSV, tc.filePath)
			}

			if file.IsTSV() != tc.expectedTSV {
				t.Errorf("IsTSV() = %v, expected %v for %s", file.IsTSV(), tc.expectedTSV, tc.filePath)
			}

			if file.IsLTSV() != tc.expectedLTSV {
				t.Errorf("IsLTSV() = %v, expected %v for %s", file.IsLTSV(), tc.expectedLTSV, tc.filePath)
			}
		})
	}
}

func Test_IsSupportedFile(t *testing.T) {
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
			result := IsSupportedFile(tc.fileName)
			if result != tc.isSupported {
				t.Errorf("IsSupportedFile(%q) = %v, expected %v", tc.fileName, result, tc.isSupported)
			}
		})
	}
}

func Test_OpenReaderEdgeCases(t *testing.T) {
	t.Parallel()

	// Test with non-existent file
	t.Run("Non-existent file", func(t *testing.T) {
		file := NewFile("non_existent_file.csv")
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
		tmpFile.Close()

		file := NewFile(tmpFile.Name())
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
			tmpFile.Close()

			file := NewFile(tmpFile.Name())
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

		err := os.WriteFile(csvFile, []byte(csvContent), 0644)
		if err != nil {
			t.Fatal(err)
		}

		file := NewFile(csvFile)
		_, err = file.ToTable()
		if err == nil {
			t.Error("expected error for CSV with duplicate column names")
			return
		}

		if !errors.Is(err, ErrDuplicateColumnName) {
			t.Errorf("expected ErrDuplicateColumnName, got: %v", err)
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

		err := os.WriteFile(tsvFile, []byte(tsvContent), 0644)
		if err != nil {
			t.Fatal(err)
		}

		file := NewFile(tsvFile)
		_, err = file.ToTable()
		if err == nil {
			t.Error("expected error for TSV with duplicate column names")
			return
		}

		if !errors.Is(err, ErrDuplicateColumnName) {
			t.Errorf("expected ErrDuplicateColumnName, got: %v", err)
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

		err := os.WriteFile(csvFile, []byte(csvContent), 0644)
		if err != nil {
			t.Fatal(err)
		}

		file := NewFile(csvFile)
		_, err = file.ToTable()
		if err == nil {
			t.Error("expected error for CSV with multiple duplicate column names")
			return
		}

		if !errors.Is(err, ErrDuplicateColumnName) {
			t.Errorf("expected ErrDuplicateColumnName, got: %v", err)
		}
	})

	t.Run("CSV without duplicate column names", func(t *testing.T) {
		t.Parallel()

		tmpDir := t.TempDir()
		csvFile := filepath.Join(tmpDir, "valid.csv")

		csvContent := `id,name,age,email
1,John,25,john@example.com
2,Jane,30,jane@example.com`

		err := os.WriteFile(csvFile, []byte(csvContent), 0644)
		if err != nil {
			t.Fatal(err)
		}

		file := NewFile(csvFile)
		table, err := file.ToTable()
		if err != nil {
			t.Errorf("expected no error for valid CSV, got: %v", err)
			return
		}

		if table == nil {
			t.Error("expected valid table, got nil")
		}
	})
}
