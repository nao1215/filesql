package reader

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
