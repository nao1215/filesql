package filesql

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// unknownCompression is a value outside the set this package defines. A caller
// can produce one by converting an int, so both directions have to answer rather
// than fall through with a nil reader or writer.
const unknownCompression CompressionType = 99

// TestCompressionHandler_UnknownType covers the refusal of a compression type
// this package does not know.
func TestCompressionHandler_UnknownType(t *testing.T) {
	t.Parallel()

	handler := NewCompressionHandler(unknownCompression)

	t.Run("reading", func(t *testing.T) {
		t.Parallel()

		reader, cleanup, err := handler.CreateReader(bytes.NewReader(nil))
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrCompression)
		assert.Nil(t, reader)
		assert.Nil(t, cleanup)
	})

	t.Run("writing", func(t *testing.T) {
		t.Parallel()

		writer, cleanup, err := handler.CreateWriter(&bytes.Buffer{})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrCompression)
		assert.Nil(t, writer)
		assert.Nil(t, cleanup)
	})
}
