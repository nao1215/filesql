// Package textin reads a text source the one way this module reads one: a
// leading byte-order mark decides the encoding, and what follows has to be
// UTF-8.
//
// It is a package of its own because two doors lead here. filesql.OpenContext
// wraps every text source in it, and parser.Parse does the same for the files
// it is given, so a file that is not UTF-8 is refused by both with one sentence
// about its encoding rather than by one of them with a sentence about a field
// count. The root package imports parser, so neither could hold this for the
// other.
package textin

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"

	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

// ErrInvalidUTF8 marks a byte that cannot be part of a character. It is the
// value filesql.ErrInvalidUTF8 names, so a caller matching that sentinel finds
// a refusal from either door.
var ErrInvalidUTF8 = errors.New("filesql: invalid UTF-8")

// ErrEncoding marks input written in something this package reads but cannot
// use as it stands: UTF-16 a decoder would answer with U+FFFD, and ISO-2022-JP,
// which is seven-bit and would otherwise pass every check here. It is the value
// filesql.ErrEncoding names.
var ErrEncoding = errors.New("filesql: text encoding failed")

// The byte-order marks a text source can begin with. They are exported because
// a save that overwrites a file writes the encoding that file had, and the
// encodings it can put back are exactly the ones the read side recognizes
// without being told: two lists of these would be one rule with two answers.
var (
	BOMUTF16LE = []byte{0xFF, 0xFE}       //nolint:gochecknoglobals // constant-like
	BOMUTF16BE = []byte{0xFE, 0xFF}       //nolint:gochecknoglobals // constant-like
	BOMUTF8    = []byte{0xEF, 0xBB, 0xBF} //nolint:gochecknoglobals // constant-like
)

// Decode wraps a text reader so a leading Unicode byte-order mark is
// honored: a UTF-8 BOM is stripped and UTF-16 (LE or BE) input is transcoded to
// UTF-8. Editors and shells such as Excel, Notepad, and PowerShell prepend these
// marks, and without this a UTF-8 BOM leaks into the first column name while a
// UTF-16 file is misread as single-byte data.
//
// Input without a recognized BOM passes through unchanged, so ordinary UTF-8 is
// untouched. What follows the BOM handling must still be UTF-8: a non-Unicode
// legacy encoding (for example Shift-JIS) carries no mark to detect and would
// otherwise be stored as bytes that are not characters, so it is rejected rather
// than guessed at. Only call this for a text format: a binary container and a record format
// with its own framing are read as bytes.
// UTF-16 input is checked before it is decoded, because the decoder answers
// damage with U+FFFD rather than with an error; see utf16ValidatingReader.
func Decode(reader io.Reader) io.Reader {
	buffered := bufio.NewReader(reader)
	// A short input peeks short rather than failing: what came back is still
	// enough to say which mark, if any, it begins with.
	head, _ := buffered.Peek(len(BOMUTF8)) //nolint:errcheck // A short read is answered by the prefix tests below
	switch {
	case bytes.HasPrefix(head, BOMUTF16LE):
		return decodeMark(newUTF16ValidatingReader(buffered, true))
	case bytes.HasPrefix(head, BOMUTF16BE):
		return decodeMark(newUTF16ValidatingReader(buffered, false))
	case bytes.HasPrefix(head, BOMUTF8):
		return decodeMark(buffered)
	default:
		// No mark, so there is nothing for the transcoder to strip or convert
		// and its two buffers are pure cost. This is the common case: every
		// file a tool on a Unix system writes takes it.
		return newUTF8ValidatingReader(buffered)
	}
}

// decodeMark strips or converts the mark source begins with and validates what
// comes out.
func decodeMark(source io.Reader) io.Reader {
	return newUTF8ValidatingReader(transform.NewReader(source, unicode.BOMOverride(transform.Nop)))
}

// utf16ValidatingReader passes its input through unchanged and fails the read
// that carries UTF-16 the decoder cannot use.
//
// The decoder answers a code unit it cannot use — half of a surrogate pair, or a
// file that ends in the middle of a unit — with U+FFFD, and what reaches SQLite
// is then a character the file never held and nothing can tell from one it did.
// That is the silent corruption utf8ValidatingReader exists to refuse, arrived
// at through the decoder instead of through a guess, so it is refused here in
// the same shape: judge the bytes, do not edit them, and say where the input
// stopped being decodable.
//
// The check runs before the decoder rather than after it because a U+FFFD the
// file really holds is data, and after the decoder the two are the same bytes.
type utf16ValidatingReader struct {
	reader io.Reader
	// littleEndian is the byte order the leading mark named. It is fixed for the
	// whole stream: the mark appears once, at the start.
	littleEndian bool
	// half holds the first byte of a code unit whose second byte is in the next
	// read.
	half []byte
	// wantLow records that the last complete unit was a high surrogate, whose
	// low half may still arrive with the next read.
	wantLow bool
	// highAt is where that high surrogate started, so an unpaired one can be
	// reported at its own offset rather than at the one after it.
	highAt int64
	// offset counts the bytes validated so far, so the error can say where the
	// input stopped being decodable.
	offset int64
	// failed is the error that ended this reader, returned again for every later
	// read for the reason utf8ValidatingReader gives.
	failed error
}

// newUTF16ValidatingReader returns reader with UTF-16 validation applied to
// everything read from it. littleEndian is the byte order of the mark that
// selected it.
func newUTF16ValidatingReader(reader io.Reader, littleEndian bool) io.Reader {
	return &utf16ValidatingReader{reader: reader, littleEndian: littleEndian}
}

// Read reads from the underlying reader and reports an error when what it read
// is not UTF-16 a decoder can use. The bytes are passed through untouched.
func (r *utf16ValidatingReader) Read(p []byte) (int, error) {
	if r.failed != nil {
		return 0, r.failed
	}
	n, err := r.reader.Read(p)
	if n > 0 {
		if invalid := r.validate(p[:n]); invalid != nil {
			r.failed = invalid
			return n, invalid
		}
	}
	if errors.Is(err, io.EOF) {
		if invalid := r.atEnd(); invalid != nil {
			r.failed = invalid
			return n, invalid
		}
	}
	return n, err
}

// validate scans one chunk, carrying over a code unit and a surrogate pair that
// the previous chunk ended in the middle of.
func (r *utf16ValidatingReader) validate(chunk []byte) error {
	buf := chunk
	if len(r.half) > 0 {
		buf = append(r.half, chunk...)
	}

	for len(buf) >= 2 {
		unit := uint16(buf[1])<<8 | uint16(buf[0])
		if !r.littleEndian {
			unit = uint16(buf[0])<<8 | uint16(buf[1])
		}
		switch {
		case r.wantLow:
			if !isLowSurrogate(unit) {
				return fmt.Errorf("%w: UTF-16 input has a high surrogate with no low half at byte offset %d",
					ErrEncoding, r.highAt)
			}
			r.wantLow = false
		case isHighSurrogate(unit):
			r.wantLow = true
			r.highAt = r.offset
		case isLowSurrogate(unit):
			return fmt.Errorf("%w: UTF-16 input has a low surrogate with no high half at byte offset %d",
				ErrEncoding, r.offset)
		}
		buf = buf[2:]
		r.offset += 2
	}
	r.half = append(r.half[:0], buf...)
	return nil
}

// atEnd is the verdict on what the input ended with. A unit left half read and a
// surrogate pair left half written are both damage: there is no longer a next
// chunk to complete them.
func (r *utf16ValidatingReader) atEnd() error {
	if r.wantLow {
		return fmt.Errorf("%w: UTF-16 input ends with a high surrogate with no low half at byte offset %d",
			ErrEncoding, r.highAt)
	}
	if len(r.half) > 0 {
		return fmt.Errorf("%w: UTF-16 input ends in the middle of a code unit at byte offset %d",
			ErrEncoding, r.offset)
	}
	return nil
}

// isHighSurrogate reports whether unit is the first half of a surrogate pair.
func isHighSurrogate(unit uint16) bool {
	return unit >= 0xD800 && unit <= 0xDBFF
}

// isLowSurrogate reports whether unit is the second half of a surrogate pair.
func isLowSurrogate(unit uint16) bool {
	return unit >= 0xDC00 && unit <= 0xDFFF
}

// utf8ValidatingReader passes its input through unchanged and fails the read
// that carries a byte sequence which is not UTF-8.
//
// SQLite stores TEXT as UTF-8. Bytes in another encoding are not rejected by the
// engine — they are stored as given and come back as mojibake, so LENGTH counts
// the wrong number of characters, LIKE and UPPER operate on fragments of
// characters, and a caller that reads the column gets bytes no consumer can
// decode. None of that reports an error, which is why validation happens at the
// one place every text format is read through rather than at each parser.
//
// It validates rather than transcodes because nothing in the byte stream says
// which encoding it is. Detection is a guess, and a wrong guess is the same
// silent corruption in a different shape; the caller knows what it wrote and can
// transcode before loading.
type utf8ValidatingReader struct {
	reader io.Reader
	// pending holds the trailing bytes of a rune whose encoding was split across
	// two reads. They are validated with the next chunk rather than on their own,
	// where they would look like a truncated sequence.
	pending []byte
	// offset counts the bytes validated so far, so the error can say where the
	// input stopped being UTF-8.
	offset int64
	// escTail holds the last bytes of the previous chunk, so an escape sequence
	// split across two reads is still recognized.
	escTail []byte
	// failed is the error that ended this reader. It is returned again for every
	// later read, because a reader that reported a failure and then carried on
	// would hand the parser the bytes after the bad ones and let it succeed on a
	// prefix of the file.
	failed error
}

// iso2022JPDesignators are the escape sequences ISO-2022-JP uses to switch
// character sets. A bare ESC is not enough to go on: it is a legal character and
// may appear in data, while these three bytes together are not something
// tabular text produces by accident.
var iso2022JPDesignators = [][]byte{ //nolint:gochecknoglobals // constant-like lookup table
	[]byte("\x1b$@"), // JIS X 0208-1978
	[]byte("\x1b$B"), // JIS X 0208-1983
	[]byte("\x1b(J"), // JIS X 0201 Roman
	[]byte("\x1b(B"), // back to ASCII
}

// maxDesignator is how many trailing bytes have to be carried between reads so a
// sequence split across two of them is still seen.
const maxDesignator = 3

// newUTF8ValidatingReader returns reader with UTF-8 validation applied to
// everything read from it.
func newUTF8ValidatingReader(reader io.Reader) io.Reader {
	return &utf8ValidatingReader{reader: reader}
}

// Read reads from the underlying reader and reports an error when what it read
// is not UTF-8. The bytes themselves are passed through untouched, so validation
// costs a scan and never changes what a parser sees.
func (r *utf8ValidatingReader) Read(p []byte) (int, error) {
	if r.failed != nil {
		return 0, r.failed
	}
	n, err := r.reader.Read(p)
	if n > 0 {
		if invalid := r.validate(p[:n]); invalid != nil {
			r.failed = invalid
			if errors.Is(invalid, ErrEncoding) {
				// An escape-encoded chunk is withheld rather than handed over with
				// the error. Its bytes parse as text, so a parser given them reaches
				// a conclusion about the data — a column count mismatch — before it
				// looks at the error. Bytes that are not UTF-8 cannot be mistaken
				// for structure that way, so those still travel with their error.
				return 0, invalid
			}
			return n, invalid
		}
	}
	// A rune encoding left incomplete when the input ends is invalid: there is no
	// longer a next chunk to complete it.
	if errors.Is(err, io.EOF) && len(r.pending) > 0 {
		return n, fmt.Errorf("%w: incomplete character at byte offset %d", ErrInvalidUTF8, r.offset)
	}
	return n, err
}

// validate scans one chunk, carrying over a rune encoding that the previous
// chunk ended in the middle of.
func (r *utf8ValidatingReader) validate(chunk []byte) error {
	buf := chunk
	if len(r.pending) > 0 {
		buf = append(r.pending, chunk...)
	}

	// ISO-2022-JP is seven-bit, so it passes the UTF-8 check below and fails much
	// later as a field count, blaming the caller's data for the encoding.
	if r.hasEscapeDesignator(buf) {
		return fmt.Errorf("%w: input looks like ISO-2022-JP; filesql reads UTF-8, so transcode it first", ErrEncoding)
	}

	// A trailing incomplete rune is not an error yet, so it is held back and the
	// rest is validated in one pass. utf8.Valid is the fast path; the byte-wise
	// walk below runs only to say where an invalid sequence starts.
	head, tail := splitTrailingPartialRune(buf)
	if utf8.Valid(head) {
		r.offset += int64(len(head))
		r.pending = append(r.pending[:0], tail...)
		return nil
	}

	for len(head) > 0 {
		// DecodeRune reports (RuneError, 1) for a byte that cannot begin or
		// continue a valid encoding. A genuine U+FFFD in the input decodes to the
		// same rune with size 3, so the size is what separates the two.
		char, size := utf8.DecodeRune(head)
		if char == utf8.RuneError && size == 1 {
			return fmt.Errorf("%w: byte 0x%02X at offset %d is not part of a valid character",
				ErrInvalidUTF8, head[0], r.offset)
		}
		head = head[size:]
		r.offset += int64(size)
	}
	return nil
}

// splitTrailingPartialRune divides buf into the part that can be validated now
// and the trailing bytes that may still become a valid rune once more input
// arrives. Only the last utf8.UTFMax-1 bytes can be such a remainder.
func splitTrailingPartialRune(buf []byte) (head, tail []byte) {
	for back := 1; back < utf8.UTFMax && back <= len(buf); back++ {
		start := len(buf) - back
		if !utf8.RuneStart(buf[start]) {
			continue
		}
		if utf8.FullRune(buf[start:]) {
			break
		}
		return buf[:start], buf[start:]
	}
	return buf, nil
}

// hasEscapeDesignator reports whether chunk carries an ISO-2022-JP designator,
// carrying the trailing bytes so a sequence split across two reads is seen.
func (r *utf8ValidatingReader) hasEscapeDesignator(chunk []byte) bool {
	scan := chunk
	if len(r.escTail) > 0 {
		scan = append(r.escTail, chunk...)
	}
	found := false
	for _, designator := range iso2022JPDesignators {
		if bytes.Contains(scan, designator) {
			found = true
			break
		}
	}
	tail := scan
	if len(tail) > maxDesignator {
		tail = tail[len(tail)-maxDesignator:]
	}
	r.escTail = append(r.escTail[:0], tail...)
	return found
}
