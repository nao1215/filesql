package filesql

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

// Encoding is the text encoding a save writes its output in.
//
// A load reads UTF-8, and UTF-16 through the byte-order mark a save writes;
// Shift-JIS, EUC-JP and ISO-2022-JP are refused on load, so a file written in
// one of those is for other tools. Without a write side at all, a caller that
// decoded a legacy source before loading had no way to get one back, so an
// in-place save changed the file's encoding on disk without saying so.
// Compression is the same shape of decision and has had an option
// since the beginning; this is the one for text.
type Encoding int

const (
	// EncodingUTF8 writes UTF-8, which is what a save wrote before this option
	// existed and remains the default.
	EncodingUTF8 Encoding = iota
	// EncodingShiftJIS writes Shift-JIS (CP932).
	EncodingShiftJIS
	// EncodingEUCJP writes EUC-JP.
	EncodingEUCJP
	// EncodingISO2022JP writes ISO-2022-JP.
	EncodingISO2022JP
	// EncodingUTF16LE writes little-endian UTF-16 with a byte-order mark.
	EncodingUTF16LE
	// EncodingUTF16BE writes big-endian UTF-16 with a byte-order mark.
	EncodingUTF16BE
	// encodingUTF8BOM writes UTF-8 led by a byte-order mark. It has no exported
	// name because a caller asking for UTF-8 output wants it plain; the mark
	// exists here so an in-place save can put back one the source carried, which
	// is what a spreadsheet program uses to recognize the file it wrote.
	encodingUTF8BOM
)

// String returns the name a user types for the encoding.
func (e Encoding) String() string {
	switch e {
	case EncodingUTF8:
		return "utf-8"
	case EncodingShiftJIS:
		return "shift-jis"
	case EncodingEUCJP:
		return "euc-jp"
	case EncodingISO2022JP:
		return "iso-2022-jp"
	case EncodingUTF16LE:
		return "utf-16le"
	case EncodingUTF16BE:
		return "utf-16be"
	case encodingUTF8BOM:
		return "utf-8-bom"
	default:
		return "unknown"
	}
}

// encoder returns the transformer that writes e, and whether e needs one at all.
// UTF-8 needs none: the values are already UTF-8, so a transformer would only
// copy them.
//
// The UTF-16 encoders write a byte-order mark, because that is what lets the
// read side recognize the file without being told the encoding — a UTF-16 file
// with no mark is indistinguishable from single-byte data, which is the case
// the mark exists to settle.
//
// None of these is wrapped in encoding.ReplaceUnsupported. The x/text encoders
// fail on a rune the target has no way to write, and that failure is the point:
// substituting would put back the silent corruption the read side refuses.
func (e Encoding) encoder() (transform.Transformer, bool) {
	var enc encoding.Encoding
	switch e {
	case EncodingUTF8:
		return nil, false
	case EncodingShiftJIS:
		enc = japanese.ShiftJIS
	case EncodingEUCJP:
		enc = japanese.EUCJP
	case EncodingISO2022JP:
		// The escape is refused in front of the encoder rather than by it: see
		// refuseEscape.
		return transform.Chain(refuseEscape{}, japanese.ISO2022JP.NewEncoder()), true
	case EncodingUTF16LE:
		enc = unicode.UTF16(unicode.LittleEndian, unicode.UseBOM)
	case EncodingUTF16BE:
		enc = unicode.UTF16(unicode.BigEndian, unicode.UseBOM)
	case encodingUTF8BOM:
		enc = unicode.UTF8BOM
	default:
		return nil, false
	}
	return enc.NewEncoder(), true
}

// escapeByte is U+001B, which ISO-2022-JP reads as the first byte of a
// character-set designator rather than as data.
const escapeByte = 0x1b

// errEscapeUnwritable is what refuseEscape fails with. It is worded as the
// x/text repertoire errors are, since it arrives where they do.
var errEscapeUnwritable = errors.New("encoding: an escape begins a character-set designator and cannot be written as data")

// refuseEscape fails on an escape byte, and passes everything else through.
//
// ISO-2022-JP switches character sets with an escape sequence, so an escape held
// in a value or a column name goes into the file as a designator: "x\x1b$By" was
// written whole and read back as "x絈", the y taken for half of a double-byte
// character, and a column named that way took the row after it into the same
// word. Neither the dump nor the decoder said anything, because the bytes are a
// valid stream that says something else.
//
// It stands in front of the encoder because x/text writes the byte through
// rather than refusing it. If that changes upstream this becomes redundant and
// can go. The other encodings need none of it: all of them write an escape as
// the control character it is and read it back as itself.
type refuseEscape struct{ transform.NopResetter }

// Transform copies src up to the first escape, and fails there.
func (refuseEscape) Transform(dst, src []byte, _ bool) (nDst, nSrc int, err error) {
	end := len(src)
	if i := bytes.IndexByte(src, escapeByte); i >= 0 {
		end = i
	}
	n := copy(dst, src[:end])
	if n < end {
		return n, n, transform.ErrShortDst
	}
	if end < len(src) {
		return n, n, errEscapeUnwritable
	}
	return n, n, nil
}

// encodedWriter is the text writer for one save, and remembers whether the
// encoder was what failed.
//
// Telling the two apart matters and cannot be done after the fact: x/text
// reports a rune outside the target repertoire with an internal.RepertoireError,
// an unexported type, so a caller has no sentinel to match and would have to
// compare message text. Recording it at the point it happens is exact, and it
// keeps a disk error from being reported as an encoding one.
type encodedWriter struct {
	w      io.Writer
	closer func() error
	failed bool
}

// Write encodes p, noting a failure as the encoder's rather than the file's.
func (e *encodedWriter) Write(p []byte) (int, error) {
	n, err := e.w.Write(p)
	if err != nil {
		e.failed = true
	}
	return n, err
}

// Close flushes the encoder, which is where a rune held back as a partial
// sequence is finally refused.
func (e *encodedWriter) Close() error {
	err := e.closer()
	if err != nil {
		e.failed = true
	}
	return err
}

// encoderFailed reports whether the encoder refused something, which is what
// separates "this encoding cannot write this table" from a failure to write the
// bytes at all.
func (e *encodedWriter) encoderFailed() bool { return e != nil && e.failed }

// encodingWriter wraps w so text written to it is encoded as e. It returns w
// unchanged for UTF-8, where the values are already in the target encoding and a
// transformer would only copy them.
//
// It wraps inside the compressor rather than outside it: the bytes a compressor
// stores are the encoded ones, so a reader decompresses and then decodes, which
// is the order the read side already applies.
func (e Encoding) encodingWriter(w io.Writer) (io.Writer, *encodedWriter) {
	transformer, ok := e.encoder()
	if !ok {
		return w, nil
	}
	tw := transform.NewWriter(w, transformer)
	wrapped := &encodedWriter{w: tw, closer: tw.Close}
	return wrapped, wrapped
}

// unwritableRune reports whether e can write a rune, and names e for a refusal.
// It is nil for the encodings that can write every character, which is UTF-8
// and both UTF-16 forms, so a dump into one pays nothing for the check.
//
// The answers are remembered because a column tends to hold the same characters
// over and over: a table of Japanese text asks about a few hundred runes across
// however many rows it has. Only what is not ASCII reaches here, since the
// caller passes over those.
func (e Encoding) unwritableRune() func(rune) (string, bool) {
	transformer, ok := e.encoder()
	if !ok {
		return nil
	}
	name := e.String()
	known := make(map[rune]bool)
	buf := make([]byte, utf8.UTFMax)
	return func(r rune) (string, bool) {
		writable, seen := known[r]
		if !seen {
			n := utf8.EncodeRune(buf, r)
			_, _, err := transform.Bytes(transformer, buf[:n])
			writable = err == nil
			known[r] = writable
		}
		return name, writable
	}
}

// encodingError wraps a failure from the encoder so a caller can match it with
// errors.Is and read which encoding refused the value.
func encodingError(e Encoding, err error) error {
	return fmt.Errorf("%w: %s cannot write a value in this table: %w", ErrEncoding, e, err)
}
