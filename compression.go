package filesql

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/nao1215/filesql/internal/codec"
)

// NewReader returns a reader that decompresses reader with this codec.
// CompressionNone hands the stream back untouched, so a caller that does not
// know whether a stream is compressed can use the same call either way.
//
// Closing the result releases the codec; it does not close reader, which is
// still the caller's. A stream that declares more working memory than this
// package will hold for it is refused with ErrCompression before any of that
// memory is allocated.
func (c CompressionType) NewReader(reader io.Reader) (io.ReadCloser, error) {
	decompressed, closeFunc, err := newDecompressor(c, reader)
	if err != nil {
		return nil, err
	}
	return &codecReader{Reader: decompressed, closeFunc: closeFunc}, nil
}

// NewWriter returns a writer that compresses what is written to it with this
// codec and writes the result to writer. CompressionNone hands writer back
// untouched.
//
// Closing the result flushes and finalizes the stream; it does not close
// writer, which is still the caller's. Bzip2 has no writer, and asking for one
// is refused with ErrUnsupportedFormat.
func (c CompressionType) NewWriter(writer io.Writer) (io.WriteCloser, error) {
	compressed, closeFunc, err := newCompressor(c, writer)
	if err != nil {
		return nil, err
	}
	return &codecWriter{Writer: compressed, closeFunc: closeFunc}, nil
}

// OpenReader opens the file at path and returns a reader over its
// decompressed bytes, with the codec taken from the path's extension: "a.csv.gz"
// is read through gzip and "a.csv" as it is. Closing the result closes the file.
//
// A path that is not a regular file is refused with ErrUnsupportedFormat rather
// than opened. Opening a named pipe for reading blocks until a writer opens the
// other end, inside the syscall where no deadline and no cancellation reach it,
// and this is the one place every read of a path in this package goes through:
// a load of a compressed source, the read an in-place save does for the
// source's compression, encoding and line terminator, and ExcelSheetsInFile. A
// caller who means to read a pipe opens it themselves and passes the reader to
// CompressionType.NewReader or to DBBuilder.AddReader, where the blocking is
// their own.
func OpenReader(path string) (io.ReadCloser, error) {
	reader, closeFunc, err := openDecompressed(path)
	if err != nil {
		return nil, err
	}
	return &codecReader{Reader: reader, closeFunc: closeFunc}, nil
}

// RemoveCompressionExtension returns path without its compression extension,
// so "orders.csv.gz" becomes "orders.csv". A path with no compression extension
// is returned as it is.
func RemoveCompressionExtension(path string) string {
	_, base := codec.FromPath(path)
	return base
}

// codecReader is what NewReader and OpenReader hand back: the decompressed
// stream and the release the codec asked for, under io.ReadCloser so a caller
// holds one value rather than a reader and a function.
type codecReader struct {
	io.Reader
	closeFunc func() error
}

// Close releases the codec, and whatever else the reader was opened over.
func (r *codecReader) Close() error { return r.closeFunc() }

// codecWriter is codecReader's counterpart for NewWriter.
type codecWriter struct {
	io.Writer
	closeFunc func() error
}

// Close flushes and finalizes the compressed stream.
func (w *codecWriter) Close() error { return w.closeFunc() }

// newDecompressor is NewReader in the shape this package's own readers use: a
// reader and a release function that is never nil.
func newDecompressor(compression CompressionType, reader io.Reader) (io.Reader, func() error, error) {
	decompressed, closeFunc, err := codec.Codec(compression).NewReader(reader)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %w", ErrCompression, err)
	}
	return decompressed, closeFunc, nil
}

// newCompressor is NewWriter in the same shape.
func newCompressor(compression CompressionType, writer io.Writer) (io.Writer, func() error, error) {
	compressed, closeFunc, err := codec.Codec(compression).NewWriter(writer)
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

// openDecompressed is OpenReader in the same shape. The release closes the
// codec and then the file.
func openDecompressed(path string) (io.Reader, func() error, error) {
	file, err := openRegularFile(path)
	if err != nil {
		if errors.Is(err, ErrUnsupportedFormat) {
			return nil, nil, err
		}
		return nil, nil, fmt.Errorf("%w: failed to open file: %w", ErrIOOperation, err)
	}

	reader, closeCodec, err := newDecompressor(detectCompressionType(path), file)
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}

	return reader, func() error {
		codecErr := closeCodec()
		if closeErr := file.Close(); closeErr != nil && codecErr == nil {
			return closeErr
		}
		return codecErr
	}, nil
}

// detectCompressionType is the codec a path's extension names, or
// CompressionNone when it names none.
func detectCompressionType(path string) CompressionType {
	found, _ := codec.FromPath(path)
	return CompressionType(found)
}

// baseFileType is the format a path names once its compression extension is
// looked through.
func baseFileType(path string) FileType {
	ext := strings.ToLower(filepath.Ext(RemoveCompressionExtension(path)))

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
