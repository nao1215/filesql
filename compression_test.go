//nolint:errcheck,gosec // Test cleanup errors are intentionally ignored, and the hand-built headers have fixed widths
package filesql

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/nao1215/filesql/internal/codec"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		{
			name:            "ZLIB compression",
			compressionType: CompressionZLIB,
			extension:       ".z",
			canWrite:        true,
		},
		{
			name:            "Snappy compression",
			compressionType: CompressionSNAPPY,
			extension:       ".snappy",
			canWrite:        true,
		},
		{
			name:            "S2 compression",
			compressionType: CompressionS2,
			extension:       ".s2",
			canWrite:        true,
		},
		{
			name:            "LZ4 compression",
			compressionType: CompressionLZ4,
			extension:       ".lz4",
			canWrite:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler := NewCompressionHandler(tt.compressionType)

			// The extension the handler was built for
			if got := tt.compressionType.Extension(); got != tt.extension {
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
			case CompressionZLIB:
				zlibWriter := zlib.NewWriter(&compressedData)
				_, _ = zlibWriter.Write(testData)
				_ = zlibWriter.Close()
			case CompressionSNAPPY:
				snappyWriter := snappy.NewBufferedWriter(&compressedData)
				_, _ = snappyWriter.Write(testData)
				_ = snappyWriter.Close()
			case CompressionS2:
				s2Writer := s2.NewWriter(&compressedData)
				_, _ = s2Writer.Write(testData)
				_ = s2Writer.Close()
			case CompressionLZ4:
				lz4Writer := lz4.NewWriter(&compressedData)
				_, _ = lz4Writer.Write(testData)
				_ = lz4Writer.Close()
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

	t.Run("detectCompressionType", func(t *testing.T) {
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
			{"data.csv.z", CompressionZLIB},
			{"data.csv.snappy", CompressionSNAPPY},
			{"data.csv.s2", CompressionS2},
			{"data.csv.lz4", CompressionLZ4},
			{"path/to/file.csv", CompressionNone},
			{"path/to/file.csv.gz", CompressionGZ},
		}

		for _, tt := range tests {
			t.Run(tt.path, func(t *testing.T) {
				got := factory.detectCompressionType(tt.path)
				if got != tt.expected {
					t.Errorf("detectCompressionType(%q) = %v, want %v", tt.path, got, tt.expected)
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
			{"data.csv.z", "data.csv"},
			{"data.csv.snappy", "data.csv"},
			{"data.csv.s2", "data.csv"},
			{"data.csv.lz4", "data.csv"},
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

	t.Run("getBaseFileType", func(t *testing.T) {
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
				got := factory.getBaseFileType(tt.path)
				if got != tt.expected {
					t.Errorf("getBaseFileType(%q) = %v, want %v", tt.path, got, tt.expected)
				}
			})
		}
	})

	t.Run("createHandlerForFile", func(t *testing.T) {
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
				handler, ok := factory.createHandlerForFile(tt.path).(*compressionHandlerImpl)
				if !ok {
					t.Fatalf("createHandlerForFile(%q) returned %T, want *compressionHandlerImpl", tt.path, handler)
				}
				if got := handler.compressionType.Extension(); got != tt.expectedExtension {
					t.Errorf("handler for %q has extension %v, want %v", tt.path, got, tt.expectedExtension)
				}
			})
		}
	})
}

// TestCompressionEndToEnd tests the complete compression/decompression workflow
func TestCompressionEndToEnd(t *testing.T) {
	t.Parallel()

	if os.Getenv("GITHUB_ACTIONS") == "" {
		t.Skip("Skipping slow compression end-to-end test in local development")
	}

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

				// Write compressed file the way a caller does: create the file,
				// then wrap it in the codec's writer.
				file, err := os.Create(fileName) //nolint:gosec // fileName is under t.TempDir()
				if err != nil {
					t.Fatalf("os.Create() error = %v", err)
				}
				writer, cleanup, err := NewCompressionHandler(tt.compressionType).CreateWriter(file)
				if err != nil {
					t.Fatalf("CreateWriter() error = %v", err)
				}

				_, err = writer.Write(testData)
				if err != nil {
					t.Fatalf("Write() error = %v", err)
				}

				if err := cleanup(); err != nil {
					t.Fatalf("cleanup() error = %v", err)
				}
				if err := file.Close(); err != nil {
					t.Fatalf("Close() error = %v", err)
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
// TestCreateReaderForFile_RefusesWhatIsNotAFile pins that the one place every
// read of a path goes through knows what it is opening.
//
// It did not: opening a named pipe for reading blocks until a writer opens the
// other end, inside the syscall where no context reaches, so ExcelSheetsInFile
// on a pipe never returned and neither did an in-place save whose source had
// been replaced by one -- and Close takes no context at all. A load refuses
// such a path in its collection; this is the floor under that, for the calls
// that reach a path without going through one.
func TestCreateReaderForFile_RefusesWhatIsNotAFile(t *testing.T) {
	t.Parallel()

	t.Run("a named pipe", func(t *testing.T) {
		t.Parallel()

		pipe := filepath.Join(t.TempDir(), "data.csv")
		makeFIFO(t, pipe)

		done := make(chan error, 1)
		go func() {
			_, cleanup, err := NewCompressionFactory().CreateReaderForFile(pipe)
			if cleanup != nil {
				_ = cleanup()
			}
			done <- err
		}()
		select {
		case err := <-done:
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrUnsupportedFormat)
			assert.Contains(t, err.Error(), "a named pipe")
		case <-time.After(30 * time.Second):
			t.Fatal("CreateReaderForFile did not return: it is waiting for a writer on the pipe")
		}
	})

	t.Run("a directory", func(t *testing.T) {
		t.Parallel()

		dir := filepath.Join(t.TempDir(), "data.csv")
		require.NoError(t, os.Mkdir(dir, 0o750))

		_, cleanup, err := NewCompressionFactory().CreateReaderForFile(dir)
		if cleanup != nil {
			_ = cleanup()
		}
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrUnsupportedFormat)
		assert.Contains(t, err.Error(), "a directory")
	})

	t.Run("a regular file, plain and compressed, still opens", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		plain := filepath.Join(dir, "plain.csv")
		require.NoError(t, os.WriteFile(plain, []byte("id\n1\n"), 0o600))

		zipped := filepath.Join(dir, "zipped.csv.gz")
		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		_, writeErr := gz.Write([]byte("id\n1\n"))
		require.NoError(t, writeErr)
		require.NoError(t, gz.Close())
		require.NoError(t, os.WriteFile(zipped, buf.Bytes(), 0o600))

		for _, path := range []string{plain, zipped} {
			reader, cleanup, err := NewCompressionFactory().CreateReaderForFile(path)
			require.NoError(t, err, path)
			body, readErr := io.ReadAll(reader)
			require.NoError(t, readErr)
			assert.Equal(t, "id\n1\n", string(body))
			require.NoError(t, cleanup())
		}
	})

	t.Run("a symlink to a regular file still opens", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		target := filepath.Join(dir, "target.csv")
		require.NoError(t, os.WriteFile(target, []byte("id\n1\n"), 0o600))
		link := filepath.Join(dir, "link.csv")
		if err := os.Symlink(target, link); err != nil {
			t.Skipf("this process cannot create a symlink here: %v", err)
		}

		reader, cleanup, err := NewCompressionFactory().CreateReaderForFile(link)
		require.NoError(t, err)
		body, readErr := io.ReadAll(reader)
		require.NoError(t, readErr)
		assert.Equal(t, "id\n1\n", string(body))
		require.NoError(t, cleanup())
	})
}

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
		{CompressionZLIB, "zlib", ".z"},
		{CompressionSNAPPY, "snappy", ".snappy"},
		{CompressionS2, "s2", ".s2"},
		{CompressionLZ4, "lz4", ".lz4"},
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

// TestCompressionTypeCoversEveryCodec pins the bridge between this package's
// CompressionType and the codec package's Codec.
//
// The two are separate iota enums, and the bridge between them is the bare
// conversion codec.Codec(c), written at five call sites. That conversion is
// correct only while both declare the same codecs in the same order, and
// nothing said so: a codec inserted into the middle of either list would leave
// every conversion naming a different codec, with no build error and no failing
// test. The table above pins each value this package declares; this pins that
// there are no others on either side.
func TestCompressionTypeCoversEveryCodec(t *testing.T) {
	t.Parallel()

	// CompressionNone is the absence of a codec, which codec.All leaves out.
	if got, want := int(CompressionLZ4), len(codec.All); got != want {
		t.Fatalf("the last CompressionType is %d but the codec package knows %d codecs; the two enums have drifted", got, want)
	}

	for i, c := range codec.All {
		// The conversion is the one the package makes, so this fails wherever
		// the package would silently use the wrong codec.
		ct := CompressionType(i + 1)
		if codec.Codec(ct) != c {
			t.Errorf("CompressionType(%d) converts to codec %s, want %s", i+1, codec.Codec(ct), c)
		}
		if ct.String() != c.String() || ct.Extension() != c.Extension() {
			t.Errorf("CompressionType(%d) reads as (%q, %q), want (%q, %q)",
				i+1, ct.String(), ct.Extension(), c.String(), c.Extension())
		}
	}

	if codec.Codec(CompressionNone) != codec.None {
		t.Errorf("CompressionNone converts to codec %s, want none", codec.Codec(CompressionNone))
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
		{"zlib", CompressionZLIB, ".z"},
		{"snappy", CompressionSNAPPY, ".snappy"},
		{"s2", CompressionS2, ".s2"},
		{"lz4", CompressionLZ4, ".lz4"},
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

// xzStreamDeclaring builds a minimal xz stream whose LZMA2 filter property asks
// for the dictionary the prop byte encodes: dict = (2 | (prop & 1)) << (prop/2 + 11),
// so 20 is 4 MiB, 28 is what "xz -9" writes, and 40 is the format's maximum of
// 4 GiB. The payload is deliberately truncated; what is under test is what the
// header alone costs.
func xzStreamDeclaring(prop byte) []byte {
	var b bytes.Buffer
	b.Write([]byte{0xFD, '7', 'z', 'X', 'Z', 0x00})
	flags := []byte{0x00, 0x00}
	b.Write(flags)
	_ = binary.Write(&b, binary.LittleEndian, crc32.ChecksumIEEE(flags))

	fields := []byte{0x00, 0x00, 0x21, 0x01, prop}
	total := len(fields) + 4 // the CRC is part of Block Header Size
	for total%4 != 0 {
		fields = append(fields, 0x00)
		total = len(fields) + 4
	}
	fields[0] = byte(total/4 - 1)
	b.Write(fields)
	_ = binary.Write(&b, binary.LittleEndian, crc32.ChecksumIEEE(fields))
	b.Write([]byte{0x01, 0x00, 0x00, 'A'})
	return b.Bytes()
}

// zstdFrameDeclaring builds a minimal zstd frame holding body in one raw block,
// whose window descriptor asks for the window the exponent selects:
// windowLog = 10 + exp, so 10 is 1 MiB and 19 is 512 MiB.
func zstdFrameDeclaring(exp byte, body []byte) []byte {
	b := []byte{0x28, 0xB5, 0x2F, 0xFD, 0x00, exp << 3}
	v := uint32(1) | (uint32(len(body)) << 3) // last block, raw
	b = append(b, byte(v), byte(v>>8), byte(v>>16))
	return append(b, body...)
}

// TestCompressionRefusesAnImplausibleDeclaredSize pins that what a damaged or
// hostile stream costs is bounded by this package's own limit rather than by the
// number the stream asserts.
//
// It was not. An xz file declares its dictionary size in one byte and a zstd
// frame declares its window in one byte, and both were honored as written: a
// 28-byte xz stream asking for 4 GiB allocated 4 GiB before failing, and a
// 14-byte zstd frame asking for 512 MiB allocated 513 MiB and then loaded its
// one row. Twenty opens took peak resident memory to 8224 MiB and 1050 MiB
// respectively.
//
// The oracle is bytes allocated rather than wall time: the allocation is what
// costs, and a large one only becomes resident once the runtime reuses the span,
// so a stopwatch reports a fresh process as fast while the harm is real. The
// test does not run in parallel because ReadMemStats would otherwise count what
// the tests beside it allocate.
func TestCompressionRefusesAnImplausibleDeclaredSize(t *testing.T) {
	for _, tc := range []struct {
		name string
		ct   CompressionType
		data []byte
	}{
		{name: "xz asking for a 4 GiB dictionary", ct: CompressionXZ, data: xzStreamDeclaring(40)},
		{name: "xz asking for a 1 GiB dictionary", ct: CompressionXZ, data: xzStreamDeclaring(36)},
		{name: "zstd asking for a 512 MiB window", ct: CompressionZSTD, data: zstdFrameDeclaring(19, []byte("id\n1\n"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)

			reader, cleanup, err := NewCompressionHandler(tc.ct).CreateReader(bytes.NewReader(tc.data))
			if err == nil {
				_, err = io.Copy(io.Discard, reader)
				cleanup()
			}
			runtime.ReadMemStats(&after)
			allocated := after.TotalAlloc - before.TotalAlloc

			require.Error(t, err, "a stream declaring more than the limit must be refused")
			assert.ErrorIs(t, err, ErrCompression)
			assert.Less(t, allocated, uint64(32<<20),
				"the refusal must not pay for the size the stream asked for; allocated %d MiB", allocated>>20)
		})
	}
}

// TestCompressionAcceptsTheSizesRealFilesDeclare is the other side of the
// boundary: the limit cannot be tightened into refusing files people have.
// "xz -6" declares 8 MiB, "xz -9" declares 64 MiB, this package's own writer
// declares 8 MiB, and the zstd CLI declares 2 MiB at -3 and 8 MiB at -19.
func TestCompressionAcceptsTheSizesRealFilesDeclare(t *testing.T) {
	t.Parallel()

	t.Run("xz dictionaries up to what xz -9 writes", func(t *testing.T) {
		t.Parallel()

		for _, prop := range []byte{20, 22, 28} { // 4 MiB, 8 MiB, 64 MiB
			reader, cleanup, err := NewCompressionHandler(CompressionXZ).CreateReader(
				bytes.NewReader(xzStreamDeclaring(prop)))
			require.NoError(t, err, "prop %d is a dictionary real files declare", prop)
			_, err = io.Copy(io.Discard, reader)
			// The payload is truncated on purpose, so the read fails on the
			// data rather than on the declared size.
			assert.NotErrorIs(t, err, ErrCompression, "prop %d must not be refused for its size", prop)
			cleanup()
		}
	})

	t.Run("zstd windows up to the limit", func(t *testing.T) {
		t.Parallel()

		for _, exp := range []byte{10, 13, 17} { // 1 MiB, 8 MiB, 128 MiB
			reader, cleanup, err := NewCompressionHandler(CompressionZSTD).CreateReader(
				bytes.NewReader(zstdFrameDeclaring(exp, []byte("id\n1\n"))))
			require.NoError(t, err)
			got, err := io.ReadAll(reader)
			require.NoError(t, err, "exp %d is a window real files declare", exp)
			assert.Equal(t, "id\n1\n", string(got))
			cleanup()
		}
	})

	t.Run("a round trip through this package's own writers", func(t *testing.T) {
		t.Parallel()

		payload := []byte("id,name\n1,alice\n2,bob\n")
		for _, ct := range []CompressionType{CompressionXZ, CompressionZSTD} {
			var buf bytes.Buffer
			w, closeWriter, err := NewCompressionHandler(ct).CreateWriter(&buf)
			require.NoError(t, err)
			_, err = w.Write(payload)
			require.NoError(t, err)
			require.NoError(t, closeWriter())

			reader, cleanup, err := NewCompressionHandler(ct).CreateReader(bytes.NewReader(buf.Bytes()))
			require.NoError(t, err)
			got, err := io.ReadAll(reader)
			require.NoError(t, err)
			assert.Equal(t, payload, got)
			cleanup()
		}
	})
}

// TestEveryCodecHoldsADamagedStreamToABudget is the invariant that would have
// caught both: whatever a stream's header says, a codec that cannot read it
// costs a fixed budget rather than one proportional to the number the header
// asserts.
//
// The oracle is bytes allocated rather than wall time, for the reason given on
// the test above and because a wall-clock budget over this many cases would
// fail on a loaded runner for reasons it does not measure. Neither this test nor
// its subtests run in parallel, so ReadMemStats counts only what they allocate.
func TestEveryCodecHoldsADamagedStreamToABudget(t *testing.T) {
	payload := bytes.Repeat([]byte("id,name,amount\n1,alice,2.5\n"), 200)
	for _, ct := range []CompressionType{
		CompressionGZ, CompressionXZ, CompressionZSTD, CompressionZLIB,
		CompressionSNAPPY, CompressionS2, CompressionLZ4,
	} {
		t.Run(ct.Extension(), func(t *testing.T) {
			var buf bytes.Buffer
			w, closeWriter, err := NewCompressionHandler(ct).CreateWriter(&buf)
			require.NoError(t, err)
			_, err = w.Write(payload)
			require.NoError(t, err)
			require.NoError(t, closeWriter())
			good := buf.Bytes()

			// Sixteen prefixes of the stream, and the stream with each of its
			// first sixteen bytes set to 0xFF and to zero: the header fields
			// that drive an allocation all live there.
			cases := make([][]byte, 0, 48)
			for i := 1; i <= 16; i++ {
				cases = append(cases, append([]byte(nil), good[:len(good)*i/17]...))
			}
			for i := 0; i < 16 && i < len(good); i++ {
				m := append([]byte(nil), good...)
				m[i] = 0xFF
				cases = append(cases, m)
				m2 := append([]byte(nil), good...)
				m2[i] = 0x00
				cases = append(cases, m2)
			}

			for idx, data := range cases {
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				reader, cleanup, err := NewCompressionHandler(ct).CreateReader(bytes.NewReader(data))
				if err == nil {
					_, _ = io.Copy(io.Discard, io.LimitReader(reader, 1<<20))
					cleanup()
				}
				runtime.ReadMemStats(&after)
				// Above the 256 MiB an accepted header may still legitimately
				// ask for, and far below the 513 MiB and 4096 MiB the
				// unbounded readers took.
				assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(300<<20),
					"case %d of %s allocated too much, which is what a header-driven allocation looks like", idx, ct.Extension())
			}
		})
	}
}

// unknownCompression is a value outside the set this package defines. A caller
// can produce one by converting an int, so both directions have to answer rather
// than fall through with a nil reader or writer.
const unknownCompression CompressionType = 99

// TestCompressionHandler_UnknownType covers the refusal of a compression type
// this package does not know.
func TestCompressionHandler_UnknownType(t *testing.T) {
	t.Parallel()

	handler := NewCompressionHandler(unknownCompression)

	t.Run("reading", func(t *testing.T) {
		t.Parallel()

		reader, cleanup, err := handler.CreateReader(bytes.NewReader(nil))
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrCompression)
		assert.Nil(t, reader)
		assert.Nil(t, cleanup)
	})

	t.Run("writing", func(t *testing.T) {
		t.Parallel()

		writer, cleanup, err := handler.CreateWriter(&bytes.Buffer{})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrCompression)
		assert.Nil(t, writer)
		assert.Nil(t, cleanup)
	})
}

// TestCompressionFailureSaysItOnce pins what a codec's failure reports.
//
// Two layers wrapped a read failure and both added the same sentinel, so a
// broken compressed file said "compression operation failed" twice and named
// the path twice in one line. The write side did worse than repeat itself: the
// handler separates a codec that has no writer from a compressor that failed to
// start, and the caller wrapped whatever came back in ErrCompression regardless,
// so asking for an output this build cannot write reported a compression
// failure and matched both sentinels at once.
func TestCompressionFailureSaysItOnce(t *testing.T) {
	t.Parallel()

	t.Run("a broken compressed file names the sentinel and the path once", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "a.csv.gz")
		require.NoError(t, os.WriteFile(path, []byte("not gzip at all"), 0o600))

		_, err := OpenContext(t.Context(), path)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrCompression)
		assert.Equal(t, 1, strings.Count(err.Error(), ErrCompression.Error()),
			"one failure names its sentinel once")
		assert.Equal(t, 1, strings.Count(err.Error(), path),
			"and names the file once, the load having already put it in front")
	})

	t.Run("a codec with no writer is an unsupported format, not a compression failure", func(t *testing.T) {
		t.Parallel()

		db := openWithTable(t, "CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('x')")

		err := DumpDatabase(db, t.TempDir(), NewDumpOptions().WithCompression(CompressionBZ2))
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrUnsupportedFormat)
		assert.NotErrorIs(t, err, ErrCompression,
			"the handler classified this and the caller must not relabel it")
	})

	// Which sentinel a broken stream carries is the codec's own business: gzip
	// and zlib read their header when the reader is made and fail there, while
	// the rest decode lazily and fail as a parse. What none of them may do is
	// say one sentinel twice, and there are two layers that could.
	t.Run("no codec names its sentinel twice", func(t *testing.T) {
		t.Parallel()

		for _, extension := range []string{".gz", ".bz2", ".xz", ".zst", ".z", ".snappy", ".s2", ".lz4"} {
			t.Run(extension, func(t *testing.T) {
				t.Parallel()

				path := filepath.Join(t.TempDir(), "a.csv"+extension)
				require.NoError(t, os.WriteFile(path, []byte("this is not compressed at all"), 0o600))

				_, err := OpenContext(t.Context(), path)
				require.Error(t, err)
				assert.LessOrEqual(t, strings.Count(err.Error(), ErrCompression.Error()), 1,
					"one failure names its sentinel at most once")
				assert.LessOrEqual(t, strings.Count(err.Error(), path), 1,
					"and names the file at most once")
			})
		}
	})
}
