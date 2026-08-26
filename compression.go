package filesql

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/nao1215/filesql/internal/codec"
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
	decompressed, closeFunc, err := codec.Codec(h.compressionType).NewReader(reader)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %w", ErrCompression, err)
	}
	return decompressed, closeFunc, nil
}

// CreateWriter creates a compression writer based on the compression type
func (h *compressionHandlerImpl) CreateWriter(writer io.Writer) (io.Writer, func() error, error) {
	compressed, closeFunc, err := codec.Codec(h.compressionType).NewWriter(writer)
	if err != nil {
		// A codec that has no writer at all is an unsupported format, not a
		// compressor that failed to start.
		if errors.Is(err, codec.ErrNoBZ2Writer) {
			return nil, nil, fmt.Errorf("%w: %w", ErrUnsupportedFormat, err)
		}
		return nil, nil, fmt.Errorf("%w: %w", ErrCompression, err)
	}
	return compressed, closeFunc, nil
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
	case strings.HasSuffix(path, extZLIB):
		return CompressionZLIB
	case strings.HasSuffix(path, extSNAPPY):
		return CompressionSNAPPY
	case strings.HasSuffix(path, extS2):
		return CompressionS2
	case strings.HasSuffix(path, extLZ4):
		return CompressionLZ4
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
		return nil, nil, fmt.Errorf("%w: failed to open file: %w", ErrIOOperation, err)
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

// RemoveCompressionExtension removes the compression extension from a file path if present
func (f *CompressionFactory) RemoveCompressionExtension(path string) string {
	for _, ext := range []string{extGZ, extBZ2, extXZ, extZSTD, extZLIB, extSNAPPY, extS2, extLZ4} {
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
	case extJSON:
		return FileTypeJSON
	case extJSONL:
		return FileTypeJSONL
	default:
		return FileTypeUnsupported
	}
}
