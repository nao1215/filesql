package textin

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"testing"
	"testing/quick"
	"unicode/utf8"
)

// TestUTF8ValidatingReaderPassesThroughUnchanged is the property the validator
// must not break: it is a filter that judges, not one that edits. For any input
// it accepts, the bytes a parser reads are byte-for-byte the bytes it was given,
// whatever size the reads happen to be.
func TestUTF8ValidatingReaderPassesThroughUnchanged(t *testing.T) {
	t.Parallel()

	property := func(text string, chunk uint8) bool {
		if !utf8.ValidString(text) {
			return true // only accepted input has a pass-through contract
		}
		size := int(chunk)%7 + 1
		got, err := io.ReadAll(newUTF8ValidatingReader(&chunkedReader{
			data: []byte(text),
			size: size,
		}))
		return err == nil && string(got) == text
	}

	if err := quick.Check(property, &quick.Config{
		MaxCount: 500,
		Rand:     rand.New(rand.NewSource(1)), //nolint:gosec // deterministic input generation, not security
	}); err != nil {
		t.Error(err)
	}
}

// TestUTF8ValidatingReaderVerdictIndependentOfChunking pins the property a
// streaming validator is most likely to get wrong: a rune whose encoding is
// split across two reads must not be judged on its first half. The verdict has
// to match utf8.Valid over the whole input at every read size.
func TestUTF8ValidatingReaderVerdictIndependentOfChunking(t *testing.T) {
	t.Parallel()

	inputs := [][]byte{
		[]byte("日本語のテキストと絵文字🍣"),
		[]byte("ascii only"),
		{0x80},                   // lone continuation byte
		{0xE3, 0x81},             // truncated three-byte sequence
		{0xE3, 0x81, 0x82},       // complete three-byte sequence
		{0xF0, 0x9F, 0x8D},       // truncated four-byte sequence
		{0xF0, 0x9F, 0x8D, 0xA3}, // complete four-byte sequence
		{0xEF, 0xBF, 0xBD},       // an encoded U+FFFD is valid input
		[]byte("a\xC3\xA9b"),     // valid two-byte sequence between ASCII
		[]byte("a\xC3b"),         // two-byte sequence cut short mid-string
		{0xED, 0xA0, 0x80},       // surrogate half, invalid in UTF-8
		{0xC0, 0x80},             // overlong encoding of NUL
	}

	for _, input := range inputs {
		for size := 1; size <= len(input)+2; size++ {
			got, err := io.ReadAll(newUTF8ValidatingReader(&chunkedReader{
				data: input,
				size: size,
			}))
			wantValid := utf8.Valid(input)
			if wantValid != (err == nil) {
				t.Errorf("input %x read %d bytes at a time: err = %v, want valid = %v",
					input, size, err, wantValid)
				continue
			}
			if err == nil && !bytes.Equal(got, input) {
				t.Errorf("input %x read %d bytes at a time: got %x", input, size, got)
			}
			if err != nil && !errors.Is(err, ErrInvalidUTF8) {
				t.Errorf("input %x read %d bytes at a time: err = %v, want ErrInvalidUTF8", input, size, err)
			}
		}
	}
}

// chunkedReader hands out at most size bytes per Read, so a test can put a rune
// boundary wherever it needs one.
type chunkedReader struct {
	data []byte
	size int
	pos  int
}

func (r *chunkedReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := min(min(r.size, len(p)), len(r.data)-r.pos)
	copy(p, r.data[r.pos:r.pos+n])
	r.pos += n
	return n, nil
}

// TestUTF16ValidatingReaderVerdictIndependentOfChunking pins the property a
// streaming validator is most likely to get wrong: a code unit or a surrogate
// pair split across two reads must not be judged on its first half. The verdict
// has to be the same at every read size, and an accepted input has to come
// through byte for byte.
func TestUTF16ValidatingReaderVerdictIndependentOfChunking(t *testing.T) {
	t.Parallel()

	inputs := []struct {
		name  string
		units []uint16
		valid bool
	}{
		{name: "plain text", units: utf16Units("ab"), valid: true},
		{name: "a surrogate pair", units: []uint16{0xD83C, 0xDF63}, valid: true},
		{name: "a pair between text", units: []uint16{'a', 0xD83C, 0xDF63, 'b'}, valid: true},
		{name: "a replacement character", units: []uint16{0xFFFD}, valid: true},
		{name: "nothing but the mark", units: nil, valid: true},
		{name: "an unpaired high surrogate", units: []uint16{0xD800, 'a'}, valid: false},
		{name: "an unpaired low surrogate", units: []uint16{0xDC00, 'a'}, valid: false},
		{name: "two high surrogates", units: []uint16{0xD800, 0xD800}, valid: false},
		{name: "a high surrogate at the end", units: []uint16{'a', 0xD800}, valid: false},
	}

	for _, littleEndian := range []bool{true, false} {
		for _, input := range inputs {
			whole := utf16FromUnits(littleEndian, input.units)
			cases := []struct {
				name  string
				data  []byte
				valid bool
			}{
				{name: input.name, data: whole, valid: input.valid},
				// The same input with its last byte cut off ends in the middle of
				// a unit, which is damage whatever the units before it were.
				{name: input.name + ", cut mid unit", data: whole[:len(whole)-1], valid: false},
			}
			for _, tc := range cases {
				for size := 1; size <= len(tc.data)+2; size++ {
					got, err := io.ReadAll(newUTF16ValidatingReader(&chunkedReader{
						data: tc.data,
						size: size,
					}, littleEndian))
					if tc.valid != (err == nil) {
						t.Errorf("%s (little endian %v) read %d bytes at a time: err = %v, want valid = %v",
							tc.name, littleEndian, size, err, tc.valid)
						continue
					}
					if err == nil && !bytes.Equal(got, tc.data) {
						t.Errorf("%s (little endian %v) read %d bytes at a time: got %x", tc.name, littleEndian, size, got)
					}
					if err != nil && !errors.Is(err, ErrEncoding) {
						t.Errorf("%s (little endian %v) read %d bytes at a time: err = %v, want ErrEncoding",
							tc.name, littleEndian, size, err)
					}
				}
			}
		}
	}
}

func utf16FromUnits(littleEndian bool, units []uint16) []byte {
	out := make([]byte, 0, 2+2*len(units))
	if littleEndian {
		out = append(out, 0xFF, 0xFE)
	} else {
		out = append(out, 0xFE, 0xFF)
	}
	for _, u := range units {
		low, high := byte(u&0xFF), byte(u>>8)
		if littleEndian {
			out = append(out, low, high)
		} else {
			out = append(out, high, low)
		}
	}
	return out
}

func utf16Units(s string) []uint16 {
	units := make([]uint16, 0, len(s))
	for _, r := range s {
		units = append(units, uint16(r)) //nolint:gosec // test input stays below U+10000
	}
	return units
}
