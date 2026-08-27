package reader

import (
	"errors"
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
		"unterminated object":               `{"a":`,
		"unterminated array":                `[`,
		"array holding a broken object":     `[{"a":`,
		"array missing its close bracket":   `[1, 2`,
		"array element that is not JSON":    `[1, tru]`,
		"trailing bytes after a full array": `[1] garbage`,
		"bytes that are not JSON at all":    `not json`,
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
