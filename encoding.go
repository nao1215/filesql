package filesql

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

// isTextBaseType reports whether a base file type is a text format whose byte
// stream may carry a Unicode byte-order mark. Binary containers (Parquet, XLSX)
// and record formats with their own framing (ACH, Fedwire) must not be decoded.
func isTextBaseType(ft FileType) bool {
	switch ft {
	case FileTypeCSV, FileTypeTSV, FileTypeLTSV, FileTypeJSON, FileTypeJSONL:
		return true
	default:
		return false
	}
}

// decodeTextReader wraps a text reader so a leading Unicode byte-order mark is
// honored: a UTF-8 BOM is stripped and UTF-16 (LE or BE) input is transcoded to
// UTF-8. Editors and shells such as Excel, Notepad, and PowerShell prepend these
// marks, and without this a UTF-8 BOM leaks into the first column name while a
// UTF-16 file is misread as single-byte data.
//
// Input without a recognized BOM passes through unchanged, so ordinary UTF-8 is
// untouched. What follows the BOM handling must still be UTF-8: a non-Unicode
// legacy encoding (for example Shift-JIS) carries no mark to detect and would
// otherwise be stored as bytes that are not characters, so it is rejected rather
// than guessed at. Only call this for text formats; see isTextBaseType.
// UTF-16 input is checked before it is decoded, because the decoder answers
// damage with U+FFFD rather than with an error; see utf16ValidatingReader.
func decodeTextReader(reader io.Reader) io.Reader {
	buffered := bufio.NewReader(reader)
	var source io.Reader = buffered
	if head, err := buffered.Peek(len(bomUTF16LE)); err == nil || len(head) == len(bomUTF16LE) {
		switch {
		case bytes.HasPrefix(head, bomUTF16LE):
			source = newUTF16ValidatingReader(buffered, true)
		case bytes.HasPrefix(head, bomUTF16BE):
			source = newUTF16ValidatingReader(buffered, false)
		}
	}
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

// bomEncodings maps a leading byte-order mark to the encoding that wrote it.
// These are exactly the encodings the read side recognizes without being told,
// which is what makes them the ones an in-place save can put back.
var bomEncodings = []struct {
	mark     []byte
	encoding Encoding
}{
	{mark: bomUTF16LE, encoding: EncodingUTF16LE},
	{mark: bomUTF16BE, encoding: EncodingUTF16BE},
	{mark: bomUTF8, encoding: encodingUTF8BOM},
}

// The byte-order marks this package recognizes on the read side.
var (
	bomUTF16LE = []byte{0xFF, 0xFE}       //nolint:gochecknoglobals // constant-like
	bomUTF16BE = []byte{0xFE, 0xFF}       //nolint:gochecknoglobals // constant-like
	bomUTF8    = []byte{0xEF, 0xBB, 0xBF} //nolint:gochecknoglobals // constant-like
)

// detectSourceEncoding reports the text encoding path is written in, so a save
// that overwrites it can write the same one. A save that always wrote plain
// UTF-8 replaced a UTF-16 file with bytes its own reader would still accept but
// every other reader of that file would not, and dropped the mark a UTF-8 file
// carried, changing a header row nobody had edited.
//
// Only the marks decide. A file with no mark is UTF-8, because that is the only
// thing the read side accepts without one, and a binary or record format is left
// alone: nothing in it is text this package encodes.
func detectSourceEncoding(path string) Encoding {
	factory := NewCompressionFactory()
	if !isTextBaseType(factory.getBaseFileType(path)) {
		return EncodingUTF8
	}

	reader, cleanup, err := factory.CreateReaderForFile(path)
	if err != nil {
		return EncodingUTF8
	}
	defer func() {
		_ = cleanup() //nolint:errcheck // Reading for detection only; a close failure cannot affect the answer
	}()

	var head [3]byte
	n, err := io.ReadFull(reader, head[:])
	if err != nil && n == 0 {
		return EncodingUTF8
	}
	for _, candidate := range bomEncodings {
		if bytes.HasPrefix(head[:n], candidate.mark) {
			return candidate.encoding
		}
	}
	return EncodingUTF8
}
