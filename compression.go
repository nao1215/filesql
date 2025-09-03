package filesql

import (
	"compress/bzip2"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"
)

// CompressionHandler defines the interface for handling file compression/decompression
type CompressionHandler interface {
	// CreateReader wraps an io.Reader with a decompression reader if needed
	CreateReader(reader io.Reader) (io.Reader, func() error, error)
	// CreateWriter wraps an io.Writer with a compression writer if needed
	CreateWriter(writer io.Writer) (io.Writer, func() error, error)
	// Extension returns the file extension for this compression type (e.g., ".gz")
	Extension() string
}

// compressionHandlerImpl implements the CompressionHandler interface
type compressionHandlerImpl struct {
	compressionType CompressionType
}

// CreateReader creates a decompression reader based on the compression type
func (h *compressionHandlerImpl) CreateReader(reader io.Reader) (io.Reader, func() error, error) {
	switch h.compressionType {
	case CompressionNone:
		return reader, func() error { return nil }, nil

	case CompressionGZ:
		gzReader, err := gzip.NewReader(reader)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		return gzReader, gzReader.Close, nil

	case CompressionBZ2:
		// bzip2.NewReader doesn't need closing
		return bzip2.NewReader(reader), func() error { return nil }, nil

	case CompressionXZ:
		xzReader, err := xz.NewReader(reader)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create xz reader: %w", err)
		}
		// xz.Reader doesn't have a Close method
		return xzReader, func() error { return nil }, nil

	case CompressionZSTD:
		decoder, err := zstd.NewReader(reader)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create zstd reader: %w", err)
		}
		return decoder, func() error {
			decoder.Close()
			return nil
		}, nil

	default:
		return nil, nil, fmt.Errorf("unsupported compression type for reading: %v", h.compressionType)
	}
}

// CreateWriter creates a compression writer based on the compression type
func (h *compressionHandlerImpl) CreateWriter(writer io.Writer) (io.Writer, func() error, error) {
	switch h.compressionType {
	case CompressionNone:
		return writer, func() error { return nil }, nil

	case CompressionGZ:
		gzWriter := gzip.NewWriter(writer)
		return gzWriter, gzWriter.Close, nil

	case CompressionBZ2:
		// bzip2 doesn't have a writer in the standard library
		return nil, nil, errors.New("bzip2 compression is not supported for writing")

	case CompressionXZ:
		xzWriter, err := xz.NewWriter(writer)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create xz writer: %w", err)
		}
		return xzWriter, xzWriter.Close, nil

	case CompressionZSTD:
		zstdWriter, err := zstd.NewWriter(writer)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create zstd writer: %w", err)
		}
		return zstdWriter, zstdWriter.Close, nil

	default:
		return nil, nil, fmt.Errorf("unsupported compression type for writing: %v", h.compressionType)
	}
}

// Extension returns the file extension for this compression type
func (h *compressionHandlerImpl) Extension() string {
	return h.compressionType.Extension()
}

// NewCompressionHandler creates a new compression handler for the given compression type
func NewCompressionHandler(compressionType CompressionType) CompressionHandler {
	return &compressionHandlerImpl{
		compressionType: compressionType,
	}
}

// CompressionFactory provides factory methods for compression handling
type CompressionFactory struct{}

// NewCompressionFactory creates a new compression factory
func NewCompressionFactory() *CompressionFactory {
	return &CompressionFactory{}
}

// DetectCompressionType detects the compression type from a file path
func (f *CompressionFactory) DetectCompressionType(path string) CompressionType {
	path = strings.ToLower(path)

	switch {
	case strings.HasSuffix(path, extGZ):
		return CompressionGZ
	case strings.HasSuffix(path, extBZ2):
		return CompressionBZ2
	case strings.HasSuffix(path, extXZ):
		return CompressionXZ
	case strings.HasSuffix(path, extZSTD):
		return CompressionZSTD
	default:
		return CompressionNone
	}
}

// CreateHandlerForFile creates an appropriate compression handler for a given file path
func (f *CompressionFactory) CreateHandlerForFile(path string) CompressionHandler {
	compressionType := f.DetectCompressionType(path)
	return NewCompressionHandler(compressionType)
}

// CreateReaderForFile opens a file and returns a reader that handles decompression
func (f *CompressionFactory) CreateReaderForFile(path string) (io.Reader, func() error, error) {
	file, err := os.Open(path) //nolint:gosec // User-provided path is necessary for file operations
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open file: %w", err)
	}

	handler := f.CreateHandlerForFile(path)
	reader, cleanup, err := handler.CreateReader(file)
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}

	// Create a composite cleanup function
	compositeCleanup := func() error {
		var cleanupErr error
		if cleanup != nil {
			cleanupErr = cleanup()
		}
		if closeErr := file.Close(); closeErr != nil && cleanupErr == nil {
			cleanupErr = closeErr
		}
		return cleanupErr
	}

	return reader, compositeCleanup, nil
}

// CreateWriterForFile creates a file and returns a writer that handles compression
func (f *CompressionFactory) CreateWriterForFile(path string, compressionType CompressionType) (io.Writer, func() error, error) {
	file, err := os.Create(path) //nolint:gosec // User-provided path is necessary for file operations
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create file: %w", err)
	}

	handler := NewCompressionHandler(compressionType)
	writer, cleanup, err := handler.CreateWriter(file)
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}

	// Create a composite cleanup function
	compositeCleanup := func() error {
		var cleanupErr error
		if cleanup != nil {
			cleanupErr = cleanup()
		}
		if syncErr := file.Sync(); syncErr != nil && cleanupErr == nil {
			cleanupErr = syncErr
		}
		if closeErr := file.Close(); closeErr != nil && cleanupErr == nil {
			cleanupErr = closeErr
		}
		return cleanupErr
	}

	return writer, compositeCleanup, nil
}

// RemoveCompressionExtension removes the compression extension from a file path if present
func (f *CompressionFactory) RemoveCompressionExtension(path string) string {
	for _, ext := range []string{extGZ, extBZ2, extXZ, extZSTD} {
		if strings.HasSuffix(strings.ToLower(path), ext) {
			return path[:len(path)-len(ext)]
		}
	}
	return path
}

// GetBaseFileType determines the base file type after removing compression extensions
func (f *CompressionFactory) GetBaseFileType(path string) FileType {
	basePath := f.RemoveCompressionExtension(path)
	ext := strings.ToLower(filepath.Ext(basePath))

	switch ext {
	case extCSV:
		return FileTypeCSV
	case extTSV:
		return FileTypeTSV
	case extLTSV:
		return FileTypeLTSV
	case extParquet:
		return FileTypeParquet
	case extXLSX:
		return FileTypeXLSX
	default:
		return FileTypeUnsupported
	}
}
