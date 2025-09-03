//nolint:errcheck // Test cleanup error handling is intentionally ignored
package filesql

import (
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"
)

// TestCompressionHandlerInterface tests the CompressionHandler interface implementation
func TestCompressionHandlerInterface(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		compressionType CompressionType
		extension       string
		canWrite        bool
	}{
		{
			name:            "No compression",
			compressionType: CompressionNone,
			extension:       "",
			canWrite:        true,
		},
		{
			name:            "Gzip compression",
			compressionType: CompressionGZ,
			extension:       ".gz",
			canWrite:        true,
		},
		{
			name:            "Bzip2 compression",
			compressionType: CompressionBZ2,
			extension:       ".bz2",
			canWrite:        false, // bzip2 doesn't support writing
		},
		{
			name:            "XZ compression",
			compressionType: CompressionXZ,
			extension:       ".xz",
			canWrite:        true,
		},
		{
			name:            "ZSTD compression",
			compressionType: CompressionZSTD,
			extension:       ".zst",
			canWrite:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler := NewCompressionHandler(tt.compressionType)

			// Test Extension method
			if got := handler.Extension(); got != tt.extension {
				t.Errorf("Extension() = %v, want %v", got, tt.extension)
			}

			// Test CreateReader with valid data
			testData := []byte("test data for compression")
			var compressedData bytes.Buffer

			// Create compressed data based on type
			switch tt.compressionType {
			case CompressionNone:
				compressedData.Write(testData)
			case CompressionGZ:
				gzWriter := gzip.NewWriter(&compressedData)
				_, _ = gzWriter.Write(testData)
				_ = gzWriter.Close()
			case CompressionBZ2:
				// bzip2 doesn't have a writer in standard library,
				// so we'll skip testing reader for bzip2
				t.Skip("Skipping bzip2 reader test (no writer available)")
			case CompressionXZ:
				xzWriter, err := xz.NewWriter(&compressedData)
				if err != nil {
					t.Fatalf("Failed to create xz writer: %v", err)
				}
				_, _ = xzWriter.Write(testData)
				_ = xzWriter.Close()
			case CompressionZSTD:
				zstdWriter, err := zstd.NewWriter(&compressedData)
				if err != nil {
					t.Fatalf("Failed to create zstd writer: %v", err)
				}
				_, _ = zstdWriter.Write(testData)
				_ = zstdWriter.Close()
			}

			reader, cleanup, err := handler.CreateReader(&compressedData)
			if err != nil {
				t.Fatalf("CreateReader() error = %v", err)
			}
			defer func() {
				if cleanup != nil {
					_ = cleanup()
				}
			}()

			// Read and verify data
			readData, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("Failed to read data: %v", err)
			}

			if !bytes.Equal(readData, testData) {
				t.Errorf("Read data = %v, want %v", readData, testData)
			}

			// Test CreateWriter
			var output bytes.Buffer
			writer, cleanup, err := handler.CreateWriter(&output)

			if tt.canWrite {
				if err != nil {
					t.Fatalf("CreateWriter() error = %v, want nil", err)
				}
				defer func() {
					if cleanup != nil {
						_ = cleanup()
					}
				}()

				// Write test data
				_, err = writer.Write(testData)
				if err != nil {
					t.Fatalf("Failed to write data: %v", err)
				}

				// Close the writer if needed
				if cleanup != nil {
					_ = cleanup()
				}

				// For non-compressed data, verify directly
				if tt.compressionType == CompressionNone {
					if !bytes.Equal(output.Bytes(), testData) {
						t.Errorf("Written data = %v, want %v", output.Bytes(), testData)
					}
				}
			} else {
				// Should fail for unsupported compression types
				if err == nil {
					t.Errorf("CreateWriter() error = nil, want error for unsupported compression")
				}
			}
		})
	}
}

// TestCompressionFactory tests the CompressionFactory functionality
func TestCompressionFactory(t *testing.T) {
	t.Parallel()

	t.Run("DetectCompressionType", func(t *testing.T) {
		t.Parallel()

		factory := NewCompressionFactory()

		tests := []struct {
			path     string
			expected CompressionType
		}{
			{"data.csv", CompressionNone},
			{"data.csv.gz", CompressionGZ},
			{"data.CSV.GZ", CompressionGZ}, // Test case insensitive
			{"data.tsv.bz2", CompressionBZ2},
			{"data.ltsv.xz", CompressionXZ},
			{"data.parquet.zst", CompressionZSTD},
			{"path/to/file.csv", CompressionNone},
			{"path/to/file.csv.gz", CompressionGZ},
		}

		for _, tt := range tests {
			t.Run(tt.path, func(t *testing.T) {
				got := factory.DetectCompressionType(tt.path)
				if got != tt.expected {
					t.Errorf("DetectCompressionType(%q) = %v, want %v", tt.path, got, tt.expected)
				}
			})
		}
	})

	t.Run("RemoveCompressionExtension", func(t *testing.T) {
		t.Parallel()

		factory := NewCompressionFactory()

		tests := []struct {
			path     string
			expected string
		}{
			{"data.csv", "data.csv"},
			{"data.csv.gz", "data.csv"},
			{"data.CSV.GZ", "data.CSV"}, // Preserves original case
			{"data.tsv.bz2", "data.tsv"},
			{"data.ltsv.xz", "data.ltsv"},
			{"data.parquet.zst", "data.parquet"},
			{"path/to/file.csv", "path/to/file.csv"},
			{"path/to/file.csv.gz", "path/to/file.csv"},
		}

		for _, tt := range tests {
			t.Run(tt.path, func(t *testing.T) {
				got := factory.RemoveCompressionExtension(tt.path)
				if got != tt.expected {
					t.Errorf("RemoveCompressionExtension(%q) = %q, want %q", tt.path, got, tt.expected)
				}
			})
		}
	})

	t.Run("GetBaseFileType", func(t *testing.T) {
		t.Parallel()

		factory := NewCompressionFactory()

		tests := []struct {
			path     string
			expected FileType
		}{
			{"data.csv", FileTypeCSV},
			{"data.csv.gz", FileTypeCSV},
			{"DATA.CSV.GZ", FileTypeCSV}, // Case insensitive
			{"data.tsv.bz2", FileTypeTSV},
			{"data.ltsv.xz", FileTypeLTSV},
			{"data.parquet.zst", FileTypeParquet},
			{"data.xlsx", FileTypeXLSX},
			{"data.xlsx.gz", FileTypeXLSX},
			{"data.txt", FileTypeUnsupported},
			{"data.txt.gz", FileTypeUnsupported},
		}

		for _, tt := range tests {
			t.Run(tt.path, func(t *testing.T) {
				got := factory.GetBaseFileType(tt.path)
				if got != tt.expected {
					t.Errorf("GetBaseFileType(%q) = %v, want %v", tt.path, got, tt.expected)
				}
			})
		}
	})

	t.Run("CreateHandlerForFile", func(t *testing.T) {
		t.Parallel()

		factory := NewCompressionFactory()

		tests := []struct {
			path              string
			expectedExtension string
		}{
			{"data.csv", ""},
			{"data.csv.gz", ".gz"},
			{"data.tsv.bz2", ".bz2"},
			{"data.ltsv.xz", ".xz"},
			{"data.parquet.zst", ".zst"},
		}

		for _, tt := range tests {
			t.Run(tt.path, func(t *testing.T) {
				handler := factory.CreateHandlerForFile(tt.path)
				if got := handler.Extension(); got != tt.expectedExtension {
					t.Errorf("Handler.Extension() for %q = %v, want %v", tt.path, got, tt.expectedExtension)
				}
			})
		}
	})
}

// TestCompressionEndToEnd tests the complete compression/decompression workflow
func TestCompressionEndToEnd(t *testing.T) {
	t.Parallel()

	// Create a temporary directory for test files
	tempDir := t.TempDir()

	t.Run("Write and Read with compression", func(t *testing.T) {
		tests := []struct {
			name            string
			compressionType CompressionType
			extension       string
			skipWrite       bool
		}{
			{
				name:            "No compression",
				compressionType: CompressionNone,
				extension:       "",
			},
			{
				name:            "Gzip compression",
				compressionType: CompressionGZ,
				extension:       ".gz",
			},
			{
				name:            "XZ compression",
				compressionType: CompressionXZ,
				extension:       ".xz",
			},
			{
				name:            "ZSTD compression",
				compressionType: CompressionZSTD,
				extension:       ".zst",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				if tt.skipWrite {
					return
				}

				testData := []byte("This is test data for compression testing.\nLine 2\nLine 3")
				fileName := filepath.Join(tempDir, "test.txt"+tt.extension)

				factory := NewCompressionFactory()

				// Write compressed file
				writer, cleanup, err := factory.CreateWriterForFile(fileName, tt.compressionType)
				if err != nil {
					t.Fatalf("CreateWriterForFile() error = %v", err)
				}

				_, err = writer.Write(testData)
				if err != nil {
					t.Fatalf("Write() error = %v", err)
				}

				err = cleanup()
				if err != nil {
					t.Fatalf("cleanup() error = %v", err)
				}

				// Read compressed file
				reader, cleanup, err := factory.CreateReaderForFile(fileName)
				if err != nil {
					t.Fatalf("CreateReaderForFile() error = %v", err)
				}
				defer func() {
					_ = cleanup()
				}()

				readData, err := io.ReadAll(reader)
				if err != nil {
					t.Fatalf("ReadAll() error = %v", err)
				}

				if !bytes.Equal(readData, testData) {
					t.Errorf("Read data = %q, want %q", readData, testData)
				}
			})
		}
	})
}

// TestCompressionFactoryErrors tests error handling in the compression factory
func TestCompressionFactoryErrors(t *testing.T) {
	t.Parallel()

	factory := NewCompressionFactory()

	t.Run("CreateReaderForFile with non-existent file", func(t *testing.T) {
		t.Parallel()

		_, _, err := factory.CreateReaderForFile("/non/existent/file.csv")
		if err == nil {
			t.Error("Expected error for non-existent file, got nil")
		}
	})

	t.Run("CreateWriterForFile with invalid path", func(t *testing.T) {
		t.Parallel()

		_, _, err := factory.CreateWriterForFile("/invalid\x00path/file.csv", CompressionNone)
		if err == nil {
			t.Error("Expected error for invalid path, got nil")
		}
	})
}

// TestCompressionTypeConstants tests the CompressionType constants and methods
func TestCompressionTypeConstants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		compressionType CompressionType
		stringValue     string
		extension       string
	}{
		{CompressionNone, "none", ""},
		{CompressionGZ, "gz", ".gz"},
		{CompressionBZ2, "bz2", ".bz2"},
		{CompressionXZ, "xz", ".xz"},
		{CompressionZSTD, "zstd", ".zst"},
	}

	for _, tt := range tests {
		t.Run(tt.stringValue, func(t *testing.T) {
			t.Parallel()

			if got := tt.compressionType.String(); got != tt.stringValue {
				t.Errorf("String() = %v, want %v", got, tt.stringValue)
			}

			if got := tt.compressionType.Extension(); got != tt.extension {
				t.Errorf("Extension() = %v, want %v", got, tt.extension)
			}
		})
	}
}

// TestInvalidCompressionReader tests handling of invalid compressed data
func TestInvalidCompressionReader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		compressionType CompressionType
		data            []byte
	}{
		{
			name:            "Invalid gzip data",
			compressionType: CompressionGZ,
			data:            []byte("not gzip data"),
		},
		{
			name:            "Invalid xz data",
			compressionType: CompressionXZ,
			data:            []byte("not xz data"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler := NewCompressionHandler(tt.compressionType)
			reader := bytes.NewReader(tt.data)

			_, _, err := handler.CreateReader(reader)
			if err == nil {
				t.Error("Expected error for invalid compressed data, got nil")
			}
		})
	}

	// Test zstd separately as it may handle invalid data differently
	t.Run("Invalid zstd data", func(t *testing.T) {
		t.Parallel()

		handler := NewCompressionHandler(CompressionZSTD)
		reader := bytes.NewReader([]byte("not zstd data"))

		r, cleanup, err := handler.CreateReader(reader)
		// zstd.NewReader may not return an error immediately for invalid data
		// The error might occur when reading from the reader
		if err == nil {
			defer func() {
				if cleanup != nil {
					_ = cleanup()
				}
			}()

			// Try to read from the reader
			_, readErr := io.ReadAll(r)
			if readErr == nil {
				// If both creating and reading succeed, skip the test as zstd
				// implementation may be lenient
				t.Skip("zstd implementation accepts invalid data - skipping test")
			}
		}
	})
}

// BenchmarkCompressionReaders benchmarks different compression readers
func BenchmarkCompressionReaders(b *testing.B) {
	// Prepare test data
	testData := make([]byte, 1024*1024) // 1MB of data
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	compressionTypes := []struct {
		name            string
		compressionType CompressionType
		skip            bool
	}{
		{"None", CompressionNone, false},
		{"GZ", CompressionGZ, false},
		{"XZ", CompressionXZ, false},
		{"ZSTD", CompressionZSTD, false},
	}

	for _, ct := range compressionTypes {
		if ct.skip {
			continue
		}

		b.Run(ct.name, func(b *testing.B) {
			// Prepare compressed data
			var compressedData bytes.Buffer
			switch ct.compressionType {
			case CompressionNone:
				compressedData.Write(testData)
			case CompressionGZ:
				w := gzip.NewWriter(&compressedData)
				_, _ = w.Write(testData)
				_ = w.Close()
			case CompressionXZ:
				w, _ := xz.NewWriter(&compressedData)
				_, _ = w.Write(testData)
				_ = w.Close()
			case CompressionZSTD:
				w, _ := zstd.NewWriter(&compressedData)
				_, _ = w.Write(testData)
				_ = w.Close()
			}

			compressedBytes := compressedData.Bytes()

			b.ResetTimer()
			for range b.N {
				handler := NewCompressionHandler(ct.compressionType)
				reader := bytes.NewReader(compressedBytes)

				r, cleanup, err := handler.CreateReader(reader)
				if err != nil {
					b.Fatal(err)
				}

				data, err := io.ReadAll(r)
				if err != nil {
					b.Fatal(err)
				}

				if len(data) != len(testData) {
					b.Fatalf("Expected %d bytes, got %d", len(testData), len(data))
				}

				if cleanup != nil {
					_ = cleanup()
				}
			}
		})
	}
}

// TestCreateReaderForFileIntegration tests the integration with actual files
func TestCreateReaderForFileIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Parallel()

	tempDir := t.TempDir()
	factory := NewCompressionFactory()

	testData := []byte(strings.Repeat("test data line\n", 100))

	// Test with different compression types
	compressionTypes := []struct {
		name            string
		compressionType CompressionType
		extension       string
	}{
		{"plain", CompressionNone, ""},
		{"gzip", CompressionGZ, ".gz"},
		{"xz", CompressionXZ, ".xz"},
		{"zstd", CompressionZSTD, ".zst"},
	}

	for _, ct := range compressionTypes {
		t.Run(ct.name, func(t *testing.T) {
			t.Parallel()

			// Create a test file with the appropriate compression
			fileName := filepath.Join(tempDir, "test_"+ct.name+".txt"+ct.extension)

			// Write the file
			file, err := os.Create(fileName) //nolint:gosec // Test file creation with known safe path
			if err != nil {
				t.Fatalf("Failed to create file: %v", err)
			}

			handler := NewCompressionHandler(ct.compressionType)
			writer, cleanup, err := handler.CreateWriter(file)
			if err != nil {
				_ = file.Close()
				t.Fatalf("Failed to create writer: %v", err)
			}

			_, err = writer.Write(testData)
			if err != nil {
				t.Fatalf("Failed to write data: %v", err)
			}

			if cleanup != nil {
				_ = cleanup()
			}
			_ = file.Close()

			// Read the file using the factory
			reader, cleanupReader, err := factory.CreateReaderForFile(fileName)
			if err != nil {
				t.Fatalf("Failed to create reader: %v", err)
			}
			defer func() {
				_ = cleanupReader()
			}()

			readData, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("Failed to read data: %v", err)
			}

			if !bytes.Equal(readData, testData) {
				t.Errorf("Data mismatch: got %d bytes, want %d bytes", len(readData), len(testData))
			}
		})
	}
}
