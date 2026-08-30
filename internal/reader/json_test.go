package reader

import (
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTruncateLine(t *testing.T) {
	t.Parallel()

	t.Run("short string unchanged", func(t *testing.T) {
		t.Parallel()

		result := truncateLine("hello", 10)

		assert.Equal(t, "hello", result)
	})

	t.Run("long string truncated with ellipsis", func(t *testing.T) {
		t.Parallel()

		result := truncateLine("this is a very long string", 10)

		assert.Equal(t, "this is a ...", result)
	})

	t.Run("exact length unchanged", func(t *testing.T) {
		t.Parallel()

		result := truncateLine("12345", 5)

		assert.Equal(t, "12345", result)
	})
}

// TestReadJSONClassifiesMalformedDocumentsAsInvalidData pins what a JSON read
// says about a document that is not JSON, apart from what it says about a
// reader that failed underneath it. Both used to arrive as KindParse or
// KindInvalidData depending on whether the document opened with a bracket, so a
// caller matching one sentinel matched half the malformed documents.
func TestReadJSONClassifiesMalformedDocumentsAsInvalidData(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"unterminated object":                      `{"a":`,
		"unterminated array":                       `[`,
		"array holding a broken object":            `[{"a":`,
		"array missing its close bracket":          `[1, 2`,
		"array element that is not JSON":           `[1, tru]`,
		"trailing bytes after a full array":        `[1] garbage`,
		"bytes that are not JSON at all":           `not json`,
		"a second close bracket after an array":    `[1]]`,
		"a close brace after an array":             `[1]}`,
		"a close bracket after an empty array":     `[]]`,
		"a close brace after an empty array":       `[]}`,
		"a close bracket on a line of its own":     "[1]\n]",
		"a whole second array after a close brace": `[{"a":1}]}[{"a":2}]`,
	}

	for name, document := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := Read(strings.NewReader(document), FormatJSON, Options{}, func(*Chunk) error { return nil })

			require.Error(t, err)
			var readErr *Error
			require.ErrorAs(t, err, &readErr)
			assert.Equal(t, KindInvalidData, readErr.Kind, "%s should be invalid data, not %d", document, readErr.Kind)
		})
	}
}

// TestReadJSONReportsAFailedReaderAsAParseError keeps the classification above
// from swallowing the other case: bytes that never arrived are not bytes that
// are not JSON, and a caller that retries on I/O must be able to tell them
// apart.
func TestReadJSONReportsAFailedReaderAsAParseError(t *testing.T) {
	t.Parallel()

	broken := &failingReader{data: `[{"a":1},`, err: errReaderFailed}

	_, err := Read(broken, FormatJSON, Options{}, func(*Chunk) error { return nil })

	require.Error(t, err)
	var readErr *Error
	require.ErrorAs(t, err, &readErr)
	assert.Equal(t, KindParse, readErr.Kind)
	assert.ErrorIs(t, err, errReaderFailed)
}

// errReaderFailed is what the failing reader above reports, so a test can
// require that the cause reached the caller rather than being replaced by a
// decoder message.
var errReaderFailed = errors.New("the disk went away")

// TestJSONLLineIsBounded pins that a JSONL line is held to the same bound a
// delimited record is.
//
// A line has to be complete before it can be parsed, so its length is what the
// read costs, and one line with no terminator made that the whole stream. CSV
// and TSV were bounded and this reader, which is line-oriented in the same way,
// was not.
func TestJSONLLineIsBounded(t *testing.T) {
	t.Parallel()

	// A limit small enough to reach in a test, standing in for the real one.
	const limit = 1 << 10

	readAll := func(t *testing.T, body string) error {
		t.Helper()
		opts := Options{ChunkSize: 8, maxRecord: limit}
		_, err := Read(strings.NewReader(body), FormatJSONL, opts, func(*Chunk) error { return nil })
		return err
	}

	t.Run("a line under the limit loads", func(t *testing.T) {
		t.Parallel()

		assert.NoError(t, readAll(t, `{"a":"`+strings.Repeat("x", limit/2)+`"}`+"\n"))
	})

	t.Run("a line past the limit is refused", func(t *testing.T) {
		t.Parallel()

		err := readAll(t, `{"a":"`+strings.Repeat("x", limit*4)+`"}`+"\n")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrRecordTooLong)
		assert.Contains(t, err.Error(), "line 1")
		assert.Contains(t, err.Error(), "1 KiB")
	})

	t.Run("many ordinary lines are unaffected", func(t *testing.T) {
		t.Parallel()

		assert.NoError(t, readAll(t, strings.Repeat(`{"a":1}`+"\n", limit)))
	})

	t.Run("a line past the limit with no terminator at all is refused", func(t *testing.T) {
		t.Parallel()

		err := readAll(t, `{"a":"`+strings.Repeat("x", limit*4))
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrRecordTooLong)
	})
}

// TestReadJSONAcceptsWhatEncodingJSONAccepts is the oracle this reader is held
// to: filesql reads JSON with encoding/json, so a document that library calls
// invalid has no business loading here, and one it calls valid has no business
// being refused.
//
// The one deliberate divergence is an input holding no value at all, which is
// an empty table rather than an error, because only the caller knows whether a
// file with nothing in it is a failure.
func TestReadJSONAcceptsWhatEncodingJSONAccepts(t *testing.T) {
	t.Parallel()

	documents := []string{
		`[1]`, `[1] `, "[1]\n", `[]`, `[[1]]`, `[{"a":1}]`, `[{"a":1},{"a":2}]`,
		`{"a":1}`, `1`, `null`, `"x"`,
		`[1]]`, `[1]}`, `[]]`, `[]}`, "[1]\n]", `[{"a":1}]}[{"a":2}]`,
		`[1] 2`, `[1],`, `[1]x`, `[1`, `{"a":`, `not json`,
	}

	for _, document := range documents {
		t.Run(document, func(t *testing.T) {
			t.Parallel()

			_, err := Read(strings.NewReader(document), FormatJSON, Options{}, func(*Chunk) error { return nil })

			assert.Equal(t, json.Valid([]byte(document)), err == nil,
				"encoding/json and this reader disagree about %q", document)
		})
	}
}

// TestJSONArrayElementIsBounded pins that one element of a JSON array is held to
// the same bound a delimited record and a JSONL line are held to.
//
// An element is held whole while it is decoded, so its length is what the read
// costs, and one element with no terminator made that the whole stream. The
// array is the JSON shape that arrives in chunks, so the rule that covers the
// other chunked formats covers it.
func TestJSONArrayElementIsBounded(t *testing.T) {
	t.Parallel()

	// A limit small enough to reach in a test, standing in for the real one.
	const limit = 1 << 10

	readAll := func(t *testing.T, body string) (int, error) {
		t.Helper()
		counter := &countingReader{src: strings.NewReader(body)}
		opts := Options{ChunkSize: 8, maxRecord: limit}
		_, err := Read(counter, FormatJSON, opts, func(*Chunk) error { return nil })
		return counter.read, err
	}

	t.Run("an element under the limit loads", func(t *testing.T) {
		t.Parallel()

		_, err := readAll(t, `[{"a":"`+strings.Repeat("x", limit/2)+`"}]`)
		assert.NoError(t, err)
	})

	t.Run("an element past the limit is refused", func(t *testing.T) {
		t.Parallel()

		_, err := readAll(t, `[{"a":"`+strings.Repeat("x", limit*4)+`"}]`)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrRecordTooLong)
		assert.Contains(t, err.Error(), "1 KiB")
	})

	t.Run("an element with no terminator at all is refused without reading the rest", func(t *testing.T) {
		t.Parallel()

		read, err := readAll(t, `[{"a":"`+strings.Repeat("x", limit*40))
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrRecordTooLong)
		// The decoder reads ahead into a buffer that doubles, so the refusal
		// lands within a few reads of the bound rather than on it. What matters
		// is that it lands: the body is forty times the bound.
		assert.Less(t, read, limit*8, "the read consumed %d bytes for a %d byte bound", read, limit)
	})

	t.Run("a refused element stops the reading rather than slowing it", func(t *testing.T) {
		t.Parallel()

		// The bound has to cost the same however the decoder happens to ask for
		// bytes. json.Decoder does not stop at the first error it is handed --
		// it calls Read again -- so a reader that keeps serving after the
		// refusal lets it walk the rest of the stream, which is exactly the
		// read the bound exists to prevent. Through Go 1.26 the decoder's
		// buffer growth hid this; on 1.27 it consumed a forty-times-oversized
		// body whole. Asserting on the source reads keeps the property pinned
		// to behavior rather than to a toolchain.
		body := `[{"a":"` + strings.Repeat("x", limit*40)
		read, err := readAll(t, body)
		require.ErrorIs(t, err, ErrRecordTooLong)
		assert.Less(t, read, len(body)/2,
			"a %d byte body was read %d bytes deep for a %d byte bound", len(body), read, limit)
	})

	t.Run("many small elements are unaffected", func(t *testing.T) {
		t.Parallel()

		body := `[` + strings.TrimSuffix(strings.Repeat(`{"a":1},`, limit), ",") + `]`
		_, err := readAll(t, body)
		assert.NoError(t, err)
	})

	t.Run("an element near the limit does not spend what the elements after it are allowed", func(t *testing.T) {
		t.Parallel()

		// The decoder reads ahead, so the bytes handed over while one element
		// is being decoded are partly the elements after it. Counting them
		// against the element being decoded would refuse a file of short
		// records for the length of its neighbours: here one element just
		// under the bound is followed by enough small ones to pass it again.
		// The bound is on one element, so this file loads.
		body := `[{"a":"` + strings.Repeat("x", limit-16) + `"},` +
			strings.TrimSuffix(strings.Repeat(`{"a":1},`, limit/2), ",") + `]`
		_, err := readAll(t, body)
		assert.NoError(t, err)
	})

	t.Run("a document that is not an array is one record and is bounded too", func(t *testing.T) {
		t.Parallel()

		// Such a document becomes one row holding the whole of it, so the
		// record bound is the bound on it. Without one, a stream sending a
		// document that never ends was read however long it was.
		_, err := readAll(t, `{"a":"`+strings.Repeat("x", limit*4)+`"}`)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrRecordTooLong)
	})

	t.Run("a document under the bound is read whole", func(t *testing.T) {
		t.Parallel()

		_, err := readAll(t, `{"a":"`+strings.Repeat("x", limit/2)+`"}`)
		assert.NoError(t, err)
	})
}

// countingReader reports how much of its source a read consumed, which is what
// a bound can be asserted on: a memory figure moves with the Go version, while
// how many bytes were asked for does not.
type countingReader struct {
	src  io.Reader
	read int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.src.Read(p)
	c.read += n
	return n, err
}
