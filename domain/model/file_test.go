package model

import (
	"compress/gzip"
	"os"
	"path/filepath"
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
		tt := tt
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
		tt := tt
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
		tt := tt
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
			expected: "data.csv",
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
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := TableFromFilePath(tt.filePath)
			if result != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
		})
	}
}
