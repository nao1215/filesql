package filesql

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/nao1215/filesql/internal/codec"
)

// compressionHandler wraps a reader or a writer in one codec. It is what
// NewCompressionHandler returns.
//
// It is a type rather than an interface because nothing here takes one as an
// argument: an interface only a caller can hold, never hand back, gives them
// nothing to substitute and makes every method added to it a breaking change.
type compressionHandler struct {
	compressionType CompressionType
}

// CreateReader creates a decompression reader based on the compression type
func (h *compressionHandler) CreateReader(reader io.Reader) (io.Reader, func() error, error) {
	decompressed, closeFunc, err := codec.Codec(h.compressionType).NewReader(reader)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %w", ErrCompression, err)
	}
	return decompressed, closeFunc, nil
}

// CreateWriter creates a compression writer based on the compression type
func (h *compressionHandler) CreateWriter(writer io.Writer) (io.Writer, func() error, error) {
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

// NewCompressionHandler returns a handler that wraps a reader or a writer in
// the given codec. CompressionNone hands the stream back untouched, so a caller
// that does not know whether a stream is compressed can use one handler either
// way.
func NewCompressionHandler(compressionType CompressionType) *compressionHandler { //nolint:revive // the type is deliberately unexported: nothing here accepts one, so only the constructor is API
	return &compressionHandler{
		compressionType: compressionType,
	}
}

// CompressionFactory provides factory methods for compression handling
type CompressionFactory struct{}

// NewCompressionFactory creates a new compression factory
func NewCompressionFactory() *CompressionFactory {
	return &CompressionFactory{}
}

// detectCompressionType detects the compression type from a file path
func (f *CompressionFactory) detectCompressionType(path string) CompressionType {
	found, _ := codec.FromPath(path)
	return CompressionType(found)
}

// createHandlerForFile creates an appropriate compression handler for a given file path
func (f *CompressionFactory) createHandlerForFile(path string) *compressionHandler {
	compressionType := f.detectCompressionType(path)
	return NewCompressionHandler(compressionType)
}

// CreateReaderForFile opens a file and returns a reader that handles
// decompression.
//
// A path that is not a regular file is refused rather than opened. Opening a
// named pipe for reading blocks until a writer opens the other end, inside the
// syscall where no deadline and no cancellation reach it, and this is the one
// place every read of a path in this package goes through: a load of a
// compressed source, the read an in-place save does for the source's
// compression, encoding and line terminator, and ExcelSheetsInFile. A load
// refuses such a path earlier still, while collecting, where it can name the
// entry before anything is opened; this is the floor under that, for the calls
// that reach a path without going through a collection. A caller who means to
// read a pipe opens it themselves and passes the reader to
// NewCompressionHandler(...).CreateReader or to DBBuilder.AddReader, where the blocking
// is their own.
func (f *CompressionFactory) CreateReaderForFile(path string) (io.Reader, func() error, error) {
	file, err := openRegularFile(path)
	if err != nil {
		if errors.Is(err, ErrUnsupportedFormat) {
			return nil, nil, err
		}
		return nil, nil, fmt.Errorf("%w: failed to open file: %w", ErrIOOperation, err)
	}

	handler := f.createHandlerForFile(path)
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
	_, base := codec.FromPath(path)
	return base
}

// getBaseFileType determines the base file type after removing compression extensions
func (f *CompressionFactory) getBaseFileType(path string) FileType {
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
