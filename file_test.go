package filesql

import (
	"compress/gzip"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewFile(t *testing.T) {
	t.Parallel()

	// FileType names the format, and the codec is a separate axis. A compressed
	// path answers the same FileType as its uncompressed form, and the codec is
	// read off the same path by the compression factory.
	tests := []struct {
		name        string
		path        string
		expected    FileType
		compression CompressionType
	}{
		{
			name:        "CSV file",
			path:        "test.csv",
			expected:    FileTypeCSV,
			compression: CompressionNone,
		},
		{
			name:        "TSV file",
			path:        "test.tsv",
			expected:    FileTypeTSV,
			compression: CompressionNone,
		},
		{
			name:        "LTSV file",
			path:        "test.ltsv",
			expected:    FileTypeLTSV,
			compression: CompressionNone,
		},
		{
			name:        "Compressed CSV file",
			path:        "test.csv.gz",
			expected:    FileTypeCSV,
			compression: CompressionGZ,
		},
		{
			name:        "Compressed TSV file",
			path:        "test.tsv.bz2",
			expected:    FileTypeTSV,
			compression: CompressionBZ2,
		},
		{
			name:        "Compressed LTSV file",
			path:        "test.ltsv.xz",
			expected:    FileTypeLTSV,
			compression: CompressionXZ,
		},
		{
			name:        "Zstd compressed CSV file",
			path:        "test.csv.zst",
			expected:    FileTypeCSV,
			compression: CompressionZSTD,
		},
		{
			name:        "XLSX file",
			path:        "test.xlsx",
			expected:    FileTypeXLSX,
			compression: CompressionNone,
		},
		{
			name:        "Compressed XLSX file with gzip",
			path:        "test.xlsx.gz",
			expected:    FileTypeXLSX,
			compression: CompressionGZ,
		},
		{
			name:        "Compressed XLSX file with bzip2",
			path:        "test.xlsx.bz2",
			expected:    FileTypeXLSX,
			compression: CompressionBZ2,
		},
		{
			name:        "Compressed XLSX file with xz",
			path:        "test.xlsx.xz",
			expected:    FileTypeXLSX,
			compression: CompressionXZ,
		},
		{
			name:        "Compressed XLSX file with zstd",
			path:        "test.xlsx.zst",
			expected:    FileTypeXLSX,
			compression: CompressionZSTD,
		},
		{
			name:        "Zlib compressed CSV file",
			path:        "test.csv.z",
			expected:    FileTypeCSV,
			compression: CompressionZLIB,
		},
		{
			name:        "Snappy compressed CSV file",
			path:        "test.csv.snappy",
			expected:    FileTypeCSV,
			compression: CompressionSNAPPY,
		},
		{
			name:        "S2 compressed CSV file",
			path:        "test.csv.s2",
			expected:    FileTypeCSV,
			compression: CompressionS2,
		},
		{
			name:        "LZ4 compressed CSV file",
			path:        "test.csv.lz4",
			expected:    FileTypeCSV,
			compression: CompressionLZ4,
		},
		{
			name:        "JSON file",
			path:        "test.json",
			expected:    FileTypeJSON,
			compression: CompressionNone,
		},
		{
			name:        "Compressed JSONL file",
			path:        "test.jsonl.gz",
			expected:    FileTypeJSONL,
			compression: CompressionGZ,
		},
		{
			name:        "Unsupported file",
			path:        "test.txt",
			expected:    FileTypeUnsupported,
			compression: CompressionNone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			file := newFile(tt.path)
			assert.Equal(t, tt.expected, file.getFileType(), "File type mismatch")
			assert.Equal(t, tt.path, file.getPath(), "File path mismatch")
			assert.Equal(t, tt.compression, detectCompressionType(tt.path),
				"Compression mismatch")
		})
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
		{"test.csv.z", true},
		{"test.csv.snappy", true},
		{"test.csv.s2", true},
		{"test.csv.lz4", true},
		{"test.tsv.gz", true},
		{"test.tsv.bz2", true},
		{"test.tsv.xz", true},
		{"test.tsv.zst", true},
		{"test.tsv.z", true},
		{"test.tsv.snappy", true},
		{"test.tsv.s2", true},
		{"test.tsv.lz4", true},
		{"test.ltsv.gz", true},
		{"test.ltsv.bz2", true},
		{"test.ltsv.xz", true},
		{"test.ltsv.zst", true},
		{"test.ltsv.z", true},
		{"test.ltsv.snappy", true},
		{"test.ltsv.s2", true},
		{"test.ltsv.lz4", true},

		// Case insensitive
		{"test.CSV", true},
		{"test.TSV", true},
		{"test.LTSV", true},
		{"test.CSV.GZ", true},

		// Supported JSON/JSONL formats
		{"test.json", true},
		{"test.jsonl", true},
		{"test.json.gz", true},
		{"test.jsonl.bz2", true},

		// Unsupported formats
		{"test.txt", false},
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
		{".json", true},
		{".jsonl", true},
		{".json.gz", true},
		{".jsonl.zst", true},
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
func TestCreateDecompressedReader(t *testing.T) {
	t.Parallel()

	t.Run("no compression", func(t *testing.T) {
		t.Parallel()

		parser := newStreamingParser(FileTypeCSV, CompressionNone, "test", 1024)
		reader := strings.NewReader("test data")

		result, closeFunc, err := parser.createDecompressedReader(reader)
		if err != nil {
			t.Errorf("createDecompressedReader should not error for uncompressed data: %v", err)
		}

		if result != reader {
			t.Error("createDecompressedReader should return original reader for uncompressed data")
		}

		// newDecompressor never hands back a nil close function,
		// so an uncompressed source gets a no-op one rather than nil. Callers can
		// therefore call it unconditionally.
		if closeFunc == nil {
			t.Fatal("createDecompressedReader should return a no-op close function, not nil")
		}
		if err := closeFunc(); err != nil {
			t.Errorf("the no-op close function should not error: %v", err)
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

		parser := newStreamingParser(FileTypeCSV, CompressionGZ, "test", 1024)
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

		parser := newStreamingParser(FileTypeCSV, CompressionGZ, "test", 1024)
		reader := strings.NewReader("invalid gzip data")

		_, _, err := parser.createDecompressedReader(reader)
		if err == nil {
			t.Error("createDecompressedReader should error for invalid gzip data")
		}
	})
}
func TestDetectFileType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		path     string
		expected FileType
	}{
		{"data.csv", FileTypeCSV},
		{"data.tsv", FileTypeTSV},
		{"data.ltsv", FileTypeLTSV},
		{"data.parquet", FileTypeParquet},
		{"data.xlsx", FileTypeXLSX}, // CSV with all compression types
		{"data.csv.gz", FileTypeCSV},
		{"data.csv.bz2", FileTypeCSV},
		{"data.csv.xz", FileTypeCSV},
		{"data.csv.zst", FileTypeCSV},
		{"data.csv.z", FileTypeCSV},
		{"data.csv.snappy", FileTypeCSV},
		{"data.csv.s2", FileTypeCSV},
		{"data.csv.lz4", FileTypeCSV}, // TSV with all compression types
		{"data.tsv.gz", FileTypeTSV},
		{"data.tsv.bz2", FileTypeTSV},
		{"data.tsv.xz", FileTypeTSV},
		{"data.tsv.zst", FileTypeTSV},
		{"data.tsv.z", FileTypeTSV},
		{"data.tsv.snappy", FileTypeTSV},
		{"data.tsv.s2", FileTypeTSV},
		{"data.tsv.lz4", FileTypeTSV}, // LTSV with all compression types
		{"data.ltsv.gz", FileTypeLTSV},
		{"data.ltsv.bz2", FileTypeLTSV},
		{"data.ltsv.xz", FileTypeLTSV},
		{"data.ltsv.zst", FileTypeLTSV},
		{"data.ltsv.z", FileTypeLTSV},
		{"data.ltsv.snappy", FileTypeLTSV},
		{"data.ltsv.s2", FileTypeLTSV},
		{"data.ltsv.lz4", FileTypeLTSV}, // Parquet with all compression types
		{"data.parquet.gz", FileTypeParquet},
		{"data.parquet.bz2", FileTypeParquet},
		{"data.parquet.xz", FileTypeParquet},
		{"data.parquet.zst", FileTypeParquet},
		{"data.parquet.z", FileTypeParquet},
		{"data.parquet.snappy", FileTypeParquet},
		{"data.parquet.s2", FileTypeParquet},
		{"data.parquet.lz4", FileTypeParquet}, // XLSX with all compression types
		{"data.xlsx.gz", FileTypeXLSX},
		{"data.xlsx.bz2", FileTypeXLSX},
		{"data.xlsx.xz", FileTypeXLSX},
		{"data.xlsx.zst", FileTypeXLSX},
		{"data.xlsx.z", FileTypeXLSX},
		{"data.xlsx.snappy", FileTypeXLSX},
		{"data.xlsx.s2", FileTypeXLSX},
		{"data.xlsx.lz4", FileTypeXLSX}, // JSON/JSONL types
		{"data.json", FileTypeJSON},
		{"data.jsonl", FileTypeJSONL},
		{"data.json.gz", FileTypeJSON},
		{"data.json.bz2", FileTypeJSON},
		{"data.json.xz", FileTypeJSON},
		{"data.json.zst", FileTypeJSON},
		{"data.json.z", FileTypeJSON},
		{"data.json.snappy", FileTypeJSON},
		{"data.json.s2", FileTypeJSON},
		{"data.json.lz4", FileTypeJSON},
		{"data.jsonl.gz", FileTypeJSONL},
		{"data.jsonl.bz2", FileTypeJSONL},
		{"data.jsonl.xz", FileTypeJSONL},
		{"data.jsonl.zst", FileTypeJSONL},
		{"data.jsonl.z", FileTypeJSONL},
		{"data.jsonl.snappy", FileTypeJSONL},
		{"data.jsonl.s2", FileTypeJSONL},
		{"data.jsonl.lz4", FileTypeJSONL}, // ACH and Fedwire types
		{"payment.ach", FileTypeACH},
		{"payment.ACH", FileTypeACH},
		{"payment.fed", FileTypeFedWire},
		{"payment.FED", FileTypeFedWire},
		{".ach", FileTypeUnsupported}, // extension only
		{".fed", FileTypeUnsupported}, // extension only
		{"data.txt", FileTypeUnsupported},
		{"data.xml", FileTypeUnsupported},
		{"data", FileTypeUnsupported},
		{"", FileTypeUnsupported}, // Paths with directories
		{"/path/to/data.csv", FileTypeCSV},
		{"/path/to/data.csv.gz", FileTypeCSV},
		{"./relative/path/data.tsv.bz2", FileTypeTSV}, // Files with multiple dots in name
		{"my.data.file.csv", FileTypeCSV},
		{"my.data.file.csv.gz", FileTypeCSV}, // Edge cases
		{".csv", FileTypeCSV},                // Hidden file with just extension
		{".csv.gz", FileTypeCSV},             // Hidden compressed file
		{"file.gz", FileTypeUnsupported},     // Compression only, no base format
		{"DATA.CSV", FileTypeCSV},
		{"DATA.CSV.GZ", FileTypeCSV},
		{"DATA.JSONL.SNAPPY", FileTypeJSONL},
		{"DATA.PARQUET.LZ4", FileTypeParquet},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			t.Parallel()

			result := detectFileType(tt.path)
			if result != tt.expected {
				t.Errorf("detectFileType(%q) = %v (%s), want %v (%s)",
					tt.path, result, result.String(), tt.expected, tt.expected.String())
			}
		})
	}
}

// TestFileTypeString tests the String() method for all FileType values
func TestFileTypeString(t *testing.T) {
	t.Parallel()

	// String() names the format. It never carried a codec suffix of its own —
	// the "CSV (gzip)" spellings came from the fused constants, and a codec is
	// now described by CompressionType.String().
	tests := []struct {
		fileType FileType
		expected string
	}{
		{FileTypeCSV, "CSV"},
		{FileTypeTSV, "TSV"},
		{FileTypeLTSV, "LTSV"},
		{FileTypeParquet, "Parquet"},
		{FileTypeXLSX, "XLSX"},
		{FileTypeJSON, "JSON"},
		{FileTypeJSONL, "JSONL"},
		{FileTypeACH, "ACH"},
		{FileTypeFedWire, "FedWire"},

		// Unsupported type
		{FileTypeUnsupported, "Unsupported"},

		// Unknown type (should return "Unsupported")
		{FileType(999), "Unsupported"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			t.Parallel()

			result := tt.fileType.String()
			if result != tt.expected {
				t.Errorf("FileType(%d).String() = %q, want %q", tt.fileType, result, tt.expected)
			}
		})
	}
}
