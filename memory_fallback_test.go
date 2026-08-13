package filesql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMemoryPool_ForeignValueInThePool checks the type assertions in the pool
// getters. A sync.Pool holds any, so nothing stops a value of another type from
// reaching a getter; the getters answer with a fresh slice rather than panicking,
// and these cases pin that down.
func TestMemoryPool_ForeignValueInThePool(t *testing.T) {
	t.Parallel()

	t.Run("byte buffer", func(t *testing.T) {
		t.Parallel()
		pool := newMemoryPool(1024)
		pool.bytePool.Put(new(pooledStringSlice))

		buf := pool.getByteBuffer()
		assert.NotNil(t, buf, "a foreign value must not stop the pool from answering")
		assert.Empty(t, buf, "the buffer is handed back with no leftover length")
	})

	t.Run("record slice", func(t *testing.T) {
		t.Parallel()
		pool := newMemoryPool(1024)
		pool.recordPool.Put(new(pooledByteSlice))

		slice := pool.getRecordSlice()
		assert.NotNil(t, slice, "a foreign value must not stop the pool from answering")
		assert.Empty(t, slice, "the slice is handed back with no leftover length")
	})

	t.Run("string slice", func(t *testing.T) {
		t.Parallel()
		pool := newMemoryPool(1024)
		pool.stringPool.Put(new(pooledRecordSlice))

		slice := pool.getStringSlice()
		assert.NotNil(t, slice, "a foreign value must not stop the pool from answering")
		assert.Empty(t, slice, "the slice is handed back with no leftover length")
	})
}

// TestMemoryLimit_ShouldReduceChunkSizeUnderPressure covers the two reducing
// answers. The status comes from the live heap, so the limit is set from the
// heap this process already holds instead of trying to allocate up to a fixed
// one: a limit at the current usage reads as exceeded, and one just above it
// reads as a warning.
func TestMemoryLimit_ShouldReduceChunkSizeUnderPressure(t *testing.T) {
	t.Parallel()

	t.Run("exceeded cuts the chunk to a quarter", func(t *testing.T) {
		t.Parallel()
		limit := newMemoryLimit(defaultMemoryLimit)
		// A limit at the heap this process already holds is exceeded however the
		// heap moves afterwards, because it can only grow past it.
		limit.maxMemoryMB = limit.getMemoryInfo().currentMB
		require.Equal(t, memoryStatusExceeded, limit.checkMemoryUsage())

		shouldReduce, size := limit.shouldReduceChunkSize(1000)
		assert.True(t, shouldReduce)
		assert.Equal(t, 250, size, "an exceeded limit cuts the chunk to a quarter")
	})

	t.Run("warning halves the chunk", func(t *testing.T) {
		t.Parallel()
		limit := newMemoryLimit(defaultMemoryLimit)
		// A gigabyte of headroom keeps the limit out of reach, and a threshold of
		// zero makes any usage at all a warning, so neither answer depends on what
		// the heap does while the test runs.
		limit.maxMemoryMB = limit.getMemoryInfo().currentMB + 1024
		limit.warningThreshold = 0
		require.Equal(t, memoryStatusWarning, limit.checkMemoryUsage())

		shouldReduce, size := limit.shouldReduceChunkSize(1000)
		assert.True(t, shouldReduce)
		assert.Equal(t, 500, size, "a warning cuts the chunk in half")
	})
}
