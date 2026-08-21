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
