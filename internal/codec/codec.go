// Package codec wraps a stream in the compression the source uses.
//
// It is the one place a codec is chosen. The package had two switches over the
// same nine codecs, one reached through filesql.CompressionType and one through
// the parser package's fused file types, and a codec added to either was
// missing from the other.
package codec

import (
	"compress/bzip2"
	"compress/gzip"
	"compress/zlib"
	"errors"
	"fmt"
	"io"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/ulikunitz/xz"
)

// ErrNoBZ2Writer reports a request to write bzip2, which the standard library
// can read but not write. It is a sentinel so a caller can report it as an
// unsupported format rather than as a failed compressor.
var ErrNoBZ2Writer = errors.New("bzip2 compression is not supported for writing")

// Codec is a compression format.
type Codec int

const (
	// None is an uncompressed stream, which passes through unchanged.
	None Codec = iota
	// GZ is gzip.
	GZ
	// BZ2 is bzip2, which can be read but not written: the standard library
	// has no bzip2 writer.
	BZ2
	// XZ is xz.
	XZ
	// ZSTD is Zstandard.
	ZSTD
	// ZLIB is zlib.
	ZLIB
	// SNAPPY is the Snappy framing format.
	SNAPPY
	// S2 is S2, Snappy's extension.
	S2
	// LZ4 is LZ4.
	LZ4
)

// File extensions, one per codec.
const (
	ExtGZ     = ".gz"
	ExtBZ2    = ".bz2"
	ExtXZ     = ".xz"
	ExtZSTD   = ".zst"
	ExtZLIB   = ".z"
	ExtSNAPPY = ".snappy"
	ExtS2     = ".s2"
	ExtLZ4    = ".lz4"
)

// String returns the codec's short name, which is the name the command line
// and the file-type constants use for it.
func (c Codec) String() string {
	switch c {
	case None:
		return "none"
	case GZ:
		return "gz"
	case BZ2:
		return "bz2"
	case XZ:
		return "xz"
	case ZSTD:
		return "zstd"
	case ZLIB:
		return "zlib"
	case SNAPPY:
		return "snappy"
	case S2:
		return "s2"
	case LZ4:
		return "lz4"
	default:
		return "none"
	}
}

// Extension returns the file extension for the codec, empty for None.
func (c Codec) Extension() string {
	switch c {
	case None:
		return ""
	case GZ:
		return ExtGZ
	case BZ2:
		return ExtBZ2
	case XZ:
		return ExtXZ
	case ZSTD:
		return ExtZSTD
	case ZLIB:
		return ExtZLIB
	case SNAPPY:
		return ExtSNAPPY
	case S2:
		return ExtS2
	case LZ4:
		return ExtLZ4
	default:
		return ""
	}
}

// NewReader wraps reader so it reads the decompressed bytes, and returns the
// function that releases the decompressor.
//
// The close function is never nil, so a caller can defer it without asking
// whether this codec has anything to release.
func (c Codec) NewReader(reader io.Reader) (io.Reader, func() error, error) {
	noClose := func() error { return nil }
	switch c {
	case None:
		return reader, noClose, nil

	case GZ:
		gzReader, err := gzip.NewReader(reader)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		return gzReader, gzReader.Close, nil

	case BZ2:
		// bzip2.NewReader has nothing to release.
		return bzip2.NewReader(reader), noClose, nil

	case XZ:
		xzReader, err := xz.NewReader(reader)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create xz reader: %w", err)
		}
		// xz.Reader has no Close.
		return xzReader, noClose, nil

	case ZSTD:
		decoder, err := zstd.NewReader(reader)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create zstd reader: %w", err)
		}
		return decoder, func() error {
			decoder.Close()
			return nil
		}, nil

	case ZLIB:
		zlibReader, err := zlib.NewReader(reader)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create zlib reader: %w", err)
		}
		return zlibReader, zlibReader.Close, nil

	case SNAPPY:
		return snappy.NewReader(reader), noClose, nil

	case S2:
		return s2.NewReader(reader), noClose, nil

	case LZ4:
		return lz4.NewReader(reader), noClose, nil

	default:
		return nil, nil, fmt.Errorf("unsupported compression type for reading: %v", c)
	}
}

// NewWriter wraps writer so what is written to it is compressed, and returns
// the function that flushes and releases the compressor. The close function is
// never nil.
func (c Codec) NewWriter(writer io.Writer) (io.Writer, func() error, error) {
	switch c {
	case None:
		return writer, func() error { return nil }, nil

	case GZ:
		gzWriter := gzip.NewWriter(writer)
		return gzWriter, gzWriter.Close, nil

	case BZ2:
		return nil, nil, ErrNoBZ2Writer

	case XZ:
		xzWriter, err := xz.NewWriter(writer)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create xz writer: %w", err)
		}
		return xzWriter, xzWriter.Close, nil

	case ZSTD:
		zstdWriter, err := zstd.NewWriter(writer)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create zstd writer: %w", err)
		}
		return zstdWriter, zstdWriter.Close, nil

	case ZLIB:
		zlibWriter := zlib.NewWriter(writer)
		return zlibWriter, zlibWriter.Close, nil

	case SNAPPY:
		snappyWriter := snappy.NewBufferedWriter(writer)
		return snappyWriter, snappyWriter.Close, nil

	case S2:
		s2Writer := s2.NewWriter(writer)
		return s2Writer, s2Writer.Close, nil

	case LZ4:
		lz4Writer := lz4.NewWriter(writer)
		return lz4Writer, lz4Writer.Close, nil

	default:
		return nil, nil, fmt.Errorf("unsupported compression type for writing: %v", c)
	}
}
