// Package codec wraps a stream in the compression the source uses.
//
// It is the one place a codec is chosen. The package had two switches over the
// same nine codecs, one reached through filesql.CompressionType and one through
// the parser package's fused file types, and a codec added to either was
// missing from the other.
package codec

import (
	"bufio"
	"bytes"
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

// ErrDeclaredSizeTooLarge reports a stream whose header asks for more working
// memory than this package agrees to hold for it.
//
// xz and zstd both put the size of the buffer their decoder must allocate in
// the header, and both decoders allocate exactly what the header asks for
// before reading a byte of compressed data. The number is therefore set by what
// the file asserts rather than by anything it contains: a 28-byte xz stream can
// ask for a 4 GiB dictionary and a 14-byte zstd frame for a 512 MiB window, and
// a caller that opens such a file repeatedly pays it every time. What the
// bound gives is not the Parquet reader's rule, that a file costs no more than
// its own size; it is weaker, a fixed ceiling in place of whatever the header
// names.
var ErrDeclaredSizeTooLarge = errors.New("the stream asks for more working memory than this package will hold for it")

// The ceilings, set against what real files declare.
//
// The xz limit has room: the CLI's default -6 declares an 8 MiB dictionary, -9
// declares 64 MiB, and this package's own writer declares 8 MiB, against a
// format maximum of nearly 4 GiB. The zstd limit has none: 2 MiB at -3 and
// 8 MiB at -19, but exactly 128 MiB at --ultra -22 and --long=27, so it passes
// only because the comparison is strict. What that refuses is a deliberately
// raised --long=28 or above. Note that the CLI writes a window descriptor only
// when it does not know the content size, which is what a pipe gives it; a file
// whose size it knows becomes a single-segment frame with no descriptor at all.
const (
	maxXZDictionary = 256 << 20
	maxZstdWindow   = 128 << 20
)

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
		checked, err := xzWithinDictionaryLimit(reader)
		if err != nil {
			return nil, nil, err
		}
		xzReader, err := xz.NewReader(checked)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create xz reader: %w", err)
		}
		// xz.Reader has no Close.
		return xzReader, noClose, nil

	case ZSTD:
		checked, err := zstdWithinWindowLimit(reader)
		if err != nil {
			return nil, nil, err
		}
		// The option is the enforcement, because it holds for every frame in
		// the file rather than only the first one; the check above exists so
		// the common case answers with this package's own message instead of
		// the library's bare "window size exceeded".
		decoder, err := zstd.NewReader(checked, zstd.WithDecoderMaxWindow(maxZstdWindow))
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

// headerPeekSize is the buffer the declared-size checks read through. It is set
// by the largest header they look at: an xz stream header of twelve bytes plus a
// block header, whose size field counts in units of four with a one-byte
// encoding and so reaches 1024.
const headerPeekSize = 12 + 1024

// lzma2FilterID is the filter whose one property byte encodes the dictionary
// size the decoder must allocate.
const lzma2FilterID = 0x21

// xzWithinDictionaryLimit reads far enough into reader to learn the dictionary
// the first block asks for, and refuses the stream when it asks for more than
// maxXZDictionary. It returns a reader that still yields everything, including
// the bytes it read.
//
// This is filesql's own check because github.com/ulikunitz/xz offers no ceiling
// to pass: ReaderConfig.DictCap is a floor rather than a limit, raised to
// whatever the file declares.
//
// The buffered reader it returns is also what the decoder then reads through,
// which is worth keeping: the library reads its input in small pieces, and
// loading a 100,000-row xz through the buffer measured 2.5 times faster than
// reading straight from the file.
//
// What it covers is the first block of the first stream, which is where a
// damaged file and a plain hostile one declare their size. It does not cover a
// later block, whose header is not reachable without decoding the blocks before
// it, nor a second stream concatenated after the first, which the library opens
// on its own: a 116-byte file made by concatenating an ordinary xz with a
// 28-byte one declaring 2 GiB still allocates 2056 MiB. Closing those needs a
// decoder that takes a dictionary ceiling, which is also the condition for
// removing this: if ulikunitz/xz grows one, the limit can be passed to the
// library instead.
//
// Anything the header does not clearly say is left to the library to report:
// a truncated header, an unreadable one, a size field out of range. This
// function exists to refuse one specific claim, not to validate the format.
func xzWithinDictionaryLimit(reader io.Reader) (io.Reader, error) {
	buffered := bufio.NewReaderSize(reader, headerPeekSize)
	header, err := buffered.Peek(13)
	if err != nil {
		return buffered, nil //nolint:nilerr // Too short to hold a block header; the library reports it
	}
	// Without this, a file named .xz that is not one could still have a
	// thirteenth byte that reads as a block header size and bytes after it that
	// parse as an LZMA2 filter, and would then be refused for a dictionary it
	// never declared instead of for not being an xz stream.
	if !bytes.Equal(header[:6], xzStreamMagic) {
		return buffered, nil
	}

	// The byte after the stream header gives the block header's length in units
	// of four. Zero marks the index, which means the stream holds no blocks.
	blockHeaderLen := (int(header[12]) + 1) * 4
	if header[12] == 0 {
		return buffered, nil
	}

	block, err := buffered.Peek(12 + blockHeaderLen)
	if err != nil {
		return buffered, nil //nolint:nilerr // Truncated; the library reports it
	}
	block = block[12:]

	dictionary, ok := lzma2DictionarySize(block)
	if !ok {
		return buffered, nil
	}
	if dictionary > maxXZDictionary {
		return nil, fmt.Errorf("%w: the xz stream declares a %d MiB dictionary and the limit is %d MiB",
			ErrDeclaredSizeTooLarge, dictionary>>20, uint64(maxXZDictionary)>>20)
	}
	return buffered, nil
}

// lzma2DictionarySize walks an xz block header to the LZMA2 filter's property
// byte and returns the dictionary size it encodes. ok is false when the header
// does not plainly carry one, which is the library's business to report rather
// than this function's to guess at.
//
// The walk cannot assume a fixed offset. The block flags say whether a
// compressed size and an uncompressed size sit between them and the filter list,
// and every CLI level writes both, so a real xz header begins
// 04 c0 <compressed size> <uncompressed size> 21 01 <property> with the two
// sizes as multi-byte integers, while this package's own writer, which emits
// neither, begins 02 00 21 01 16.
func lzma2DictionarySize(block []byte) (uint64, bool) {
	if len(block) < 2 {
		return 0, false
	}
	flags := block[1]
	filters := int(flags&0x03) + 1
	pos := 2

	for _, present := range []bool{flags&0x40 != 0, flags&0x80 != 0} {
		if !present {
			continue
		}
		if _, n := xzVarint(block[pos:]); n > 0 {
			pos += n
		} else {
			return 0, false
		}
	}

	for range filters {
		id, n := xzVarint(block[pos:])
		if n == 0 {
			return 0, false
		}
		pos += n
		size, n := xzVarint(block[pos:])
		if n == 0 {
			return 0, false
		}
		pos += n
		// Compared against a cap that fits both types, so a size the header
		// inflates cannot wrap into a negative index. block is at most a block
		// header, so what survives the comparison fits in an int.
		if size > headerPeekSize || int(size) > len(block)-pos {
			return 0, false
		}
		width := int(size)
		properties := block[pos : pos+width]
		pos += width

		if id != lzma2FilterID || len(properties) != 1 {
			continue
		}
		// The format defines the property as 0 through 40, encoding 4 KiB
		// through very nearly 4 GiB: the library reads 40 as one byte short of
		// it, where the formula below gives the round number. Nothing here turns
		// on the difference, since both are past any ceiling worth setting.
		// Anything outside the range is the library's to reject.
		property := properties[0]
		if property > 40 {
			return 0, false
		}
		return (uint64(2) | uint64(property&1)) << (uint64(property)/2 + 11), true
	}
	return 0, false
}

// xzVarint decodes the format's little-endian base-128 integer and returns how
// many bytes it took, or zero when the encoding runs past the buffer or past the
// nine bytes the format allows.
func xzVarint(b []byte) (uint64, int) {
	var value uint64
	for i := range b {
		if i >= 9 {
			return 0, 0
		}
		value |= uint64(b[i]&0x7F) << (7 * uint(i))
		if b[i]&0x80 == 0 {
			return value, i + 1
		}
	}
	return 0, 0
}

// xzStreamMagic and zstdFrameMagic mark the streams the checks below read. A
// zstd skippable frame has a different magic and carries no window, so it is
// left alone; anything that is not one of these is not a header this package
// can read meaning out of, and the library reports the format error.
var (
	xzStreamMagic  = []byte{0xFD, '7', 'z', 'X', 'Z', 0x00}
	zstdFrameMagic = []byte{0x28, 0xB5, 0x2F, 0xFD}
)

// zstdWithinWindowLimit is xzWithinDictionaryLimit's counterpart for zstd: it
// reads the first frame's header to learn the window it asks for and refuses the
// stream when it asks for more than maxZstdWindow.
//
// The ceiling itself is enforced by the decoder option, which sees every frame.
// This exists so the case a caller actually meets, one frame whose window
// descriptor is too large, is reported with the file's number and this
// package's limit rather than as a bare read failure from the library.
//
// A single-segment frame carries no window descriptor; its window is its content
// size, which the option bounds. Anything the header does not plainly say is
// left to the library.
func zstdWithinWindowLimit(reader io.Reader) (io.Reader, error) {
	buffered := bufio.NewReaderSize(reader, headerPeekSize)
	header, err := buffered.Peek(6)
	if err != nil {
		return buffered, nil //nolint:nilerr // Too short to hold a window descriptor; the library reports it
	}
	if !bytes.Equal(header[:4], zstdFrameMagic) {
		return buffered, nil
	}
	descriptor := header[4]
	if descriptor&0x20 != 0 { // single segment: no window descriptor
		return buffered, nil
	}

	window := header[5]
	exponent := uint64(window >> 3)
	mantissa := uint64(window & 0x07)
	base := uint64(1) << (10 + exponent)
	declared := base + (base/8)*mantissa
	if declared > maxZstdWindow {
		return nil, fmt.Errorf("%w: the zstd frame declares a %d MiB window and the limit is %d MiB",
			ErrDeclaredSizeTooLarge, declared>>20, uint64(maxZstdWindow)>>20)
	}
	return buffered, nil
}
