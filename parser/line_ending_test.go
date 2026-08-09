package parser

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeLineEndings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "a CR-terminated file becomes LF-terminated",
			input: "a\rb\rc\r",
			want:  "a\nb\nc\n",
		},
		{
			name:  "an LF-terminated file is untouched",
			input: "a\nb\nc\n",
			want:  "a\nb\nc\n",
		},
		{
			name:  "a CRLF-terminated file is untouched",
			input: "a\r\nb\r\n",
			want:  "a\r\nb\r\n",
		},
		{
			name:  "a CR is data in a file that has line feeds",
			input: "a\rb\nc\n",
			want:  "a\rb\nc\n",
		},
		{
			name:  "a file with no line ending at all is untouched",
			input: "a,b,c",
			want:  "a,b,c",
		},
		{
			name:  "an empty file is untouched",
			input: "",
			want:  "",
		},
		{
			name:  "a quoted carriage return survives a CR-terminated file",
			input: "a,b\rx,\"y\rz\"\r",
			want:  "a,b\nx,\"y\rz\"\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := io.ReadAll(NormalizeLineEndings(strings.NewReader(tt.input)))

			require.NoError(t, err)
			assert.Equal(t, tt.want, string(got))
		})
	}
}

// The sniff peeks, and Peek consumes the source's error. Losing it would turn a
// file the UTF-8 validator refused into a successful load.
func TestNormalizeLineEndings_KeepsSourceError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("source rejected its input")
	reader := NormalizeLineEndings(&failingReader{data: "name,age\n", err: sentinel})

	_, err := io.ReadAll(reader)

	require.ErrorIs(t, err, sentinel)
}

// failingReader returns its data together with err, the way a validating reader
// refuses the chunk it just read.
type failingReader struct {
	data string
	err  error
	done bool
}

func (f *failingReader) Read(p []byte) (int, error) {
	if f.done {
		return 0, f.err
	}
	f.done = true
	n := copy(p, f.data)
	return n, f.err
}

// TestNormalizeLineEndings_OversizedFirstRecord covers the two ways a record
// longer than the sniff window could be misread. Neither may corrupt data: the
// window is a heuristic, so it decides only what it can see, and what it cannot
// see is left as it was.
func TestNormalizeLineEndings_OversizedFirstRecord(t *testing.T) {
	t.Parallel()

	t.Run("a quoted carriage return in a huge LF record is not a terminator", func(t *testing.T) {
		t.Parallel()

		// The first line feed sits past the window, and the record holds a quoted
		// carriage return before it. Translating that one would rewrite the value.
		input := "name,note\nbig,\"a\rb" + strings.Repeat("x", lineEndingSniffLimit) + "\"\nlast,1\n"

		got, err := io.ReadAll(NormalizeLineEndings(strings.NewReader(input)))

		require.NoError(t, err)
		assert.Equal(t, input, string(got))
	})

	t.Run("a record filling the window with no terminator is left alone", func(t *testing.T) {
		t.Parallel()

		// A quoted carriage return is all the window holds, so there is nothing
		// that says this file ends its lines with one.
		input := "\"a\rb" + strings.Repeat("x", lineEndingSniffLimit) + "\"\n"

		got, err := io.ReadAll(NormalizeLineEndings(strings.NewReader(input)))

		require.NoError(t, err)
		assert.Equal(t, input, string(got))
	})
}
