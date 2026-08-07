package filesql

import (
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
func decodeTextReader(reader io.Reader) io.Reader {
	return newUTF8ValidatingReader(transform.NewReader(reader, unicode.BOMOverride(transform.Nop)))
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
}

// newUTF8ValidatingReader returns reader with UTF-8 validation applied to
// everything read from it.
func newUTF8ValidatingReader(reader io.Reader) io.Reader {
	return &utf8ValidatingReader{reader: reader}
}

// Read reads from the underlying reader and reports an error when what it read
// is not UTF-8. The bytes themselves are passed through untouched, so validation
// costs a scan and never changes what a parser sees.
func (r *utf8ValidatingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		if invalid := r.validate(p[:n]); invalid != nil {
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
