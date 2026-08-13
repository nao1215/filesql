package filesql

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The names the encodings answer with. Each is written once here so the two
// tables below agree on what a given encoding is called.
const (
	nameUTF8      = "utf-8"
	nameShiftJIS  = "shift-jis"
	nameEUCJP     = "euc-jp"
	nameISO2022JP = "iso-2022-jp"
	nameUTF16LE   = "utf-16le"
	nameUTF16BE   = "utf-16be"
)

// TestEncoding_String pins the name of every encoding. The name is what a save
// error quotes back to the caller, so an encoding that answers with someone
// else's name misdirects whoever reads the failure.
func TestEncoding_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		encoding Encoding
		want     string
	}{
		{EncodingUTF8, nameUTF8},
		{EncodingShiftJIS, nameShiftJIS},
		{EncodingEUCJP, nameEUCJP},
		{EncodingISO2022JP, nameISO2022JP},
		{EncodingUTF16LE, nameUTF16LE},
		{EncodingUTF16BE, nameUTF16BE},
		{Encoding(99), unknownName},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.encoding.String())
		})
	}
}

// TestEncoding_Encoder checks which encodings need a transformer. UTF-8 needs
// none because the values are already UTF-8, and an unknown value is treated the
// same way rather than being guessed at.
func TestEncoding_Encoder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		encoding    Encoding
		wantEncoder bool
	}{
		{nameUTF8 + " needs no transformer", EncodingUTF8, false},
		{nameShiftJIS, EncodingShiftJIS, true},
		{nameEUCJP, EncodingEUCJP, true},
		{nameISO2022JP, EncodingISO2022JP, true},
		{nameUTF16LE, EncodingUTF16LE, true},
		{nameUTF16BE, EncodingUTF16BE, true},
		{"an unknown encoding needs no transformer", Encoding(99), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			transformer, ok := tt.encoding.encoder()
			assert.Equal(t, tt.wantEncoder, ok)
			if tt.wantEncoder {
				assert.NotNil(t, transformer)
				return
			}
			assert.Nil(t, transformer)
		})
	}
}

// TestEncoding_EncodingWriter covers both shapes of the writer wrapper: the
// encodings that need one get a writer whose failures are attributed to the
// encoder, and the ones that do not get their own writer back untouched.
func TestEncoding_EncodingWriter(t *testing.T) {
	t.Parallel()

	t.Run(nameUTF8+" hands back the same writer", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		w, encoded := EncodingUTF8.encodingWriter(&buf)
		assert.Nil(t, encoded, "there is nothing to attribute a failure to without an encoder")
		assert.False(t, encoded.encoderFailed(), "a nil encoded writer reports no failure")

		_, err := w.Write([]byte("hello"))
		require.NoError(t, err)
		assert.Equal(t, "hello", buf.String(), "UTF-8 values pass through unchanged")
	})

	t.Run(nameShiftJIS+" encodes what it writes", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		w, encoded := EncodingShiftJIS.encodingWriter(&buf)
		require.NotNil(t, encoded)

		_, err := w.Write([]byte("あ"))
		require.NoError(t, err)
		require.NoError(t, encoded.Close())
		assert.Equal(t, []byte{0x82, 0xa0}, buf.Bytes(), "あ is 0x82a0 in Shift-JIS")
		assert.False(t, encoded.encoderFailed())
	})
}

// TestEncodedWriter_RecordsItsOwnFailures checks the bookkeeping that separates
// "this encoding cannot write this table" from a failure to write the bytes at
// all. x/text reports an unwritable rune with an unexported error type, so the
// only exact record of it is the one taken here.
func TestEncodedWriter_RecordsItsOwnFailures(t *testing.T) {
	t.Parallel()

	t.Run("a failed write is recorded", func(t *testing.T) {
		t.Parallel()

		wantErr := errors.New("refused")
		w := &encodedWriter{
			w:      writerFunc(func([]byte) (int, error) { return 0, wantErr }),
			closer: func() error { return nil },
		}

		_, err := w.Write([]byte("あ"))
		require.ErrorIs(t, err, wantErr)
		assert.True(t, w.encoderFailed(), "the refusal must be attributed to the encoder")
	})

	t.Run("a failed close is recorded", func(t *testing.T) {
		t.Parallel()

		wantErr := errors.New("held back a partial sequence")
		w := &encodedWriter{
			w:      &bytes.Buffer{},
			closer: func() error { return wantErr },
		}

		require.ErrorIs(t, w.Close(), wantErr)
		assert.True(t, w.encoderFailed(), "a rune refused at flush time is still the encoder's refusal")
	})

	t.Run("a clean writer reports no failure", func(t *testing.T) {
		t.Parallel()

		w := &encodedWriter{w: &bytes.Buffer{}, closer: func() error { return nil }}
		_, err := w.Write([]byte("ok"))
		require.NoError(t, err)
		require.NoError(t, w.Close())
		assert.False(t, w.encoderFailed())
	})
}

// writerFunc turns a function into an io.Writer.
type writerFunc func(p []byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }
