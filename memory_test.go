package filesql

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xuri/excelize/v2"
)

func TestNewMemoryPool(t *testing.T) {
	t.Parallel()

	t.Run("default max size", func(t *testing.T) {
		t.Parallel()
		pool := newMemoryPool(0)
		assert.Equal(t, 1024*1024, pool.maxSize, "should use default max size")
	})

	t.Run("custom max size", func(t *testing.T) {
		t.Parallel()
		customSize := 512 * 1024
		pool := newMemoryPool(customSize)
		assert.Equal(t, customSize, pool.maxSize, "should use custom max size")
	})
}

func TestMemoryPool_ByteBuffer(t *testing.T) {
	t.Parallel()
	pool := newMemoryPool(1024 * 1024)

	t.Run("get and put byte buffer", func(t *testing.T) {
		t.Parallel()
		buf := pool.getByteBuffer()
		assert.NotNil(t, buf, "buffer should not be nil")
		assert.Equal(t, 0, len(buf), "buffer length should be 0")
		assert.GreaterOrEqual(t, cap(buf), 0, "buffer should have capacity")

		// Write some data
		buf = append(buf, []byte("test data")...)
		assert.Equal(t, 9, len(buf), "buffer should contain data")

		// Put back to pool
		pool.putByteBuffer(buf)

		// Get another buffer (should be reused)
		buf2 := pool.getByteBuffer()
		assert.NotNil(t, buf2, "second buffer should not be nil")
		assert.Equal(t, 0, len(buf2), "second buffer length should be reset to 0")
	})

	t.Run("reject oversized buffer", func(t *testing.T) {
		t.Parallel()
		smallPool := newMemoryPool(100) // 100 byte limit

		// Create a large buffer
		largeBuf := make([]byte, 200)

		// Pool should reject oversized buffer (no panic should occur)
		require.NotPanics(t, func() {
			smallPool.putByteBuffer(largeBuf)
		})
	})
}

func TestMemoryPool_RecordSlice(t *testing.T) {
	t.Parallel()
	pool := newMemoryPool(1024 * 1024)

	t.Run("get and put record slice", func(t *testing.T) {
		t.Parallel()
		slice := pool.getRecordSlice()
		assert.NotNil(t, slice, "slice should not be nil")
		assert.Equal(t, 0, len(slice), "slice length should be 0")

		// Add some records
		slice = append(slice, record{"col1", "col2"})
		slice = append(slice, record{"val1", "val2"})
		assert.Equal(t, 2, len(slice), "slice should contain records")

		// Put back to pool
		pool.putRecordSlice(slice)

		// Get another slice (should be reused)
		slice2 := pool.getRecordSlice()
		assert.NotNil(t, slice2, "second slice should not be nil")
		assert.Equal(t, 0, len(slice2), "second slice length should be reset to 0")
	})
}

func TestMemoryPool_StringSlice(t *testing.T) {
	t.Parallel()
	pool := newMemoryPool(1024 * 1024)

	t.Run("get and put string slice", func(t *testing.T) {
		t.Parallel()
		slice := pool.getStringSlice()
		assert.NotNil(t, slice, "slice should not be nil")
		assert.Equal(t, 0, len(slice), "slice length should be 0")

		// Add some strings
		slice = append(slice, "string1", "string2")
		assert.Equal(t, 2, len(slice), "slice should contain strings")

		// Put back to pool
		pool.putStringSlice(slice)

		// Get another slice (should be reused)
		slice2 := pool.getStringSlice()
		assert.NotNil(t, slice2, "second slice should not be nil")
		assert.Equal(t, 0, len(slice2), "second slice length should be reset to 0")
	})
}

func TestMemoryPool_ForceGC(t *testing.T) {
	t.Parallel()
	pool := newMemoryPool(1024 * 1024)

	t.Run("force GC clears pools under memory pressure", func(t *testing.T) {
		// This test is more about ensuring forceGC doesn't panic
		// Actual memory pressure simulation is difficult in unit tests

		// Get some buffers to populate pools
		buf := pool.getByteBuffer()
		records := pool.getRecordSlice()
		strings := pool.getStringSlice()

		// Put them back
		pool.putByteBuffer(buf)
		pool.putRecordSlice(records)
		pool.putStringSlice(strings)

		// Force GC should not panic
		require.NotPanics(t, func() {
			pool.forceGC()
		})
	})
}

func TestMemoryPool_ConcurrentAccess(t *testing.T) {
	t.Parallel()
	pool := newMemoryPool(1024 * 1024)

	t.Run("concurrent access to byte buffers", func(t *testing.T) {
		t.Parallel()
		const goroutines = 10
		const iterations = 100

		// Use channels to coordinate goroutines
		done := make(chan bool, goroutines)

		for range goroutines {
			go func() {
				defer func() { done <- true }()

				for range iterations {
					buf := pool.getByteBuffer()
					buf = append(buf, []byte("test")...)
					pool.putByteBuffer(buf)
				}
			}()
		}

		// Wait for all goroutines to complete
		for range goroutines {
			<-done
		}
	})
}

func TestNewMemoryLimit(t *testing.T) {
	t.Parallel()

	t.Run("default max memory", func(t *testing.T) {
		t.Parallel()
		limit := newMemoryLimit(0)
		assert.Equal(t, int64(512), limit.maxMemoryMB, "should use default max memory")
		assert.True(t, limit.isEnabled(), "should be enabled by default")
		assert.Equal(t, 0.8, limit.warningThreshold, "should use default warning threshold")
	})

	t.Run("custom max memory", func(t *testing.T) {
		t.Parallel()
		customMemory := int64(1024)
		limit := newMemoryLimit(customMemory)
		assert.Equal(t, customMemory, limit.maxMemoryMB, "should use custom max memory")
	})

	t.Run("upper bounds validation", func(t *testing.T) {
		t.Parallel()
		// Test that extremely large memory limits are capped
		unreasonableMemory := int64(1000 * 1024) // 1000GB - unreasonable
		limit := newMemoryLimit(unreasonableMemory)
		assert.Equal(t, int64(64*1024), limit.maxMemoryMB, "should cap at reasonable maximum of 64GB")
	})
}

func TestMemoryLimit_EnableDisable(t *testing.T) {
	t.Parallel()
	limit := newMemoryLimit(512)

	t.Run("enable and disable", func(t *testing.T) {
		t.Parallel()
		// Should be enabled by default
		assert.True(t, limit.isEnabled(), "should be enabled by default")

		// Disable
		limit.disable()
		assert.False(t, limit.isEnabled(), "should be disabled after Disable()")

		// Enable again
		limit.enable()
		assert.True(t, limit.isEnabled(), "should be enabled after Enable()")
	})
}

func TestMemoryLimit_SetWarningThreshold(t *testing.T) {
	t.Parallel()

	t.Run("valid thresholds", func(t *testing.T) {
		t.Parallel()
		limit := newMemoryLimit(512)
		// Test valid threshold values
		validThresholds := []float64{0.1, 0.5, 0.7, 0.9, 1.0}

		for _, threshold := range validThresholds {
			limit.setWarningThreshold(threshold)
			assert.Equal(t, threshold, limit.warningThreshold,
				"should set valid threshold %.1f", threshold)
		}
	})

	t.Run("invalid thresholds", func(t *testing.T) {
		t.Parallel()
		limit := newMemoryLimit(512)
		originalThreshold := limit.warningThreshold
		invalidThresholds := []float64{-0.1, 0.0, 1.1, 2.0}

		for _, threshold := range invalidThresholds {
			limit.setWarningThreshold(threshold)
			assert.Equal(t, originalThreshold, limit.warningThreshold,
				"should not change threshold for invalid value %.1f", threshold)
		}
	})
}

func TestMemoryLimit_CheckMemoryUsage(t *testing.T) {
	t.Parallel()

	t.Run("disabled limit returns OK", func(t *testing.T) {
		t.Parallel()
		limit := newMemoryLimit(1) // Very small limit
		limit.disable()

		status := limit.checkMemoryUsage()
		assert.Equal(t, memoryStatusOK, status, "disabled limit should always return OK")
	})

	t.Run("enabled limit checks actual usage", func(t *testing.T) {
		t.Parallel()
		// Use a very large limit to ensure we're in OK status for this test
		limit := newMemoryLimit(10000) // 10GB limit
		limit.enable()

		status := limit.checkMemoryUsage()
		// With such a large limit, we should be OK
		assert.Equal(t, memoryStatusOK, status, "with large limit should return OK")
	})
}

func TestMemoryLimit_GetMemoryInfo(t *testing.T) {
	t.Parallel()
	limit := newMemoryLimit(1024)

	t.Run("memory info structure", func(t *testing.T) {
		t.Parallel()
		info := limit.getMemoryInfo()

		assert.GreaterOrEqual(t, info.currentMB, int64(0), "current memory should be non-negative")
		assert.Equal(t, int64(1024), info.limitMB, "limit should match configured value")
		assert.GreaterOrEqual(t, info.usage, 0.0, "usage should be non-negative")
		assert.LessOrEqual(t, info.usage, 100.0, "usage should not exceed reasonable bounds")
		assert.Contains(t, []memoryStatus{memoryStatusOK, memoryStatusWarning, memoryStatusExceeded},
			info.status, "status should be a valid memoryStatus")
	})
}

func TestMemoryLimit_ShouldReduceChunkSize(t *testing.T) {
	t.Parallel()

	t.Run("OK status - no reduction", func(t *testing.T) {
		t.Parallel()
		// Use large limit to ensure OK status
		limit := newMemoryLimit(10000)
		originalChunkSize := 1000

		shouldReduce, newSize := limit.shouldReduceChunkSize(originalChunkSize)
		assert.False(t, shouldReduce, "should not reduce chunk size when OK")
		assert.Equal(t, originalChunkSize, newSize, "chunk size should remain unchanged")
	})

	t.Run("chunk size reduction logic", func(t *testing.T) {
		t.Parallel()
		limit := newMemoryLimit(512)
		originalChunkSize := 1000

		// We can't easily simulate WARNING or EXCEEDED status in unit tests
		// since it depends on actual runtime memory usage, but we can test
		// the logic by calling the method
		shouldReduce, newSize := limit.shouldReduceChunkSize(originalChunkSize)

		if shouldReduce {
			// If reduction is recommended, new size should be smaller
			assert.Less(t, newSize, originalChunkSize, "reduced chunk size should be smaller")
			assert.Greater(t, newSize, 0, "reduced chunk size should be positive")
		} else {
			assert.Equal(t, originalChunkSize, newSize, "chunk size should remain unchanged when no reduction needed")
		}
	})
}

func TestMemoryLimit_CreateMemoryError(t *testing.T) {
	t.Parallel()
	limit := newMemoryLimit(512)

	t.Run("error message format", func(t *testing.T) {
		t.Parallel()
		operation := "test operation"
		err := limit.createMemoryError(operation)

		assert.Error(t, err, "should return an error")
		assert.Contains(t, err.Error(), operation, "error should contain operation name")
		assert.Contains(t, err.Error(), "memory limit exceeded", "error should mention memory limit")
		assert.Contains(t, err.Error(), "MB", "error should include memory units")
		assert.Contains(t, err.Error(), "%", "error should include percentage")
	})
}

func TestMemoryStatus_String(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		status   memoryStatus
		expected string
	}{
		{memoryStatusOK, "OK"},
		{memoryStatusWarning, "WARNING"},
		{memoryStatusExceeded, "EXCEEDED"},
		{memoryStatus(999), "UNKNOWN"}, // Invalid status
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expected, tc.status.String(),
				"status %d should return %s", int(tc.status), tc.expected)
		})
	}
}

func TestMemoryLimit_ConcurrentAccess(t *testing.T) {
	t.Parallel()
	limit := newMemoryLimit(512)

	t.Run("concurrent usage checks", func(t *testing.T) {
		t.Parallel()
		const goroutines = 10
		const iterations = 100

		done := make(chan bool, goroutines)

		for range goroutines {
			go func() {
				defer func() { done <- true }()

				for j := range iterations {
					// These operations should be thread-safe
					_ = limit.checkMemoryUsage()
					_ = limit.getMemoryInfo()
					_, _ = limit.shouldReduceChunkSize(1000)

					// Enable/disable operations
					if j%10 == 0 {
						limit.disable()
						limit.enable()
					}
				}
			}()
		}

		// Wait for all goroutines to complete
		for range goroutines {
			<-done
		}

		// Should still be enabled after concurrent access
		assert.True(t, limit.isEnabled(), "should remain enabled after concurrent access")
	})
}

func BenchmarkMemoryPool_ByteBuffer(b *testing.B) {
	pool := newMemoryPool(1024 * 1024)

	b.Run("with pool", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			buf := pool.getByteBuffer()
			buf = append(buf, []byte("benchmark data")...)
			pool.putByteBuffer(buf)
		}
	})

	b.Run("without pool", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			buf := make([]byte, 0, 1024)
			buf = append(buf, []byte("benchmark data")...)
			// No pooling - let GC handle it
			_ = buf
		}
	})
}

func BenchmarkMemoryPool_RecordSlice(b *testing.B) {
	pool := newMemoryPool(1024 * 1024)

	b.Run("with pool", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			slice := pool.getRecordSlice()
			slice = append(slice, record{"col1", "col2"})
			pool.putRecordSlice(slice)
		}
	})

	b.Run("without pool", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			slice := make([]record, 0, 100)
			slice = append(slice, record{"col1", "col2"})
			// No pooling - let GC handle it
			_ = slice
		}
	})
}

func BenchmarkMemoryLimit_CheckMemoryUsage(b *testing.B) {
	limit := newMemoryLimit(1024)

	b.ResetTimer()
	for range b.N {
		_ = limit.checkMemoryUsage()
	}
}

func BenchmarkMemoryLimit_GetMemoryInfo(b *testing.B) {
	limit := newMemoryLimit(1024)

	b.ResetTimer()
	for range b.N {
		_ = limit.getMemoryInfo()
	}
}

// createTestXLSX creates a test XLSX file in memory
func createTestXLSX(t *testing.T, rows int, cols int) *bytes.Buffer {
	t.Helper()

	file := excelize.NewFile()
	defer file.Close()

	// Create headers
	for col := range cols {
		cell, err := excelize.CoordinatesToCellName(col+1, 1)
		require.NoError(t, err)
		err = file.SetCellValue("Sheet1", cell, "Column"+string(rune('A'+col)))
		require.NoError(t, err)
	}

	// Create data rows
	for row := 2; row <= rows+1; row++ {
		for col := range cols {
			cell, err := excelize.CoordinatesToCellName(col+1, row)
			require.NoError(t, err)
			value := "Row" + string(rune('0'+row-2)) + "Col" + string(rune('A'+col))
			err = file.SetCellValue("Sheet1", cell, value)
			require.NoError(t, err)
		}
	}

	// Write to buffer
	var buf bytes.Buffer
	err := file.Write(&buf)
	require.NoError(t, err)

	return &buf
}

func TestStreamingParser_ParseXLSXStream_MemoryOptimized(t *testing.T) {
	t.Parallel()

	t.Run("small XLSX with memory pool", func(t *testing.T) {
		t.Parallel()
		// Create a small test XLSX
		buf := createTestXLSX(t, 10, 3)

		parser := newStreamingParser(FileTypeXLSX, CompressionNone, "test_table", 5)
		table, err := parser.parseFromReader(buf)

		require.NoError(t, err)
		assert.NotNil(t, table)
		assert.Equal(t, "test_table", table.name.String())
		assert.Equal(t, 3, len(table.header))   // 3 columns
		assert.Equal(t, 10, len(table.records)) // 10 data rows
	})

	t.Run("memory limit enforcement", func(t *testing.T) {
		t.Parallel()
		// Create parser with very restrictive memory limit
		parser := newStreamingParser(FileTypeXLSX, CompressionNone, "test_table", 1000)
		parser.memoryLimit = newMemoryLimit(1) // 1MB limit - very restrictive

		// Create a small XLSX file
		buf := createTestXLSX(t, 5, 2)

		// This should work with small data
		table, err := parser.parseFromReader(buf)

		// Result depends on actual memory usage - either succeeds or fails with memory error
		if err != nil {
			assert.Contains(t, err.Error(), "memory limit exceeded",
				"should fail with memory limit error")
		} else {
			assert.NotNil(t, table, "should succeed with small data")
		}
	})

	t.Run("memory pool reuse", func(t *testing.T) {
		t.Parallel()
		parser := newStreamingParser(FileTypeXLSX, CompressionNone, "test_table", 5)

		// Process multiple XLSX files to test pool reuse
		for range 3 {
			buf := createTestXLSX(t, 5, 2)
			table, err := parser.parseFromReader(buf)

			require.NoError(t, err)
			assert.NotNil(t, table)
			assert.Equal(t, 5, len(table.records))
		}
	})
}

func TestStreamingParser_ProcessXLSXInChunks_MemoryOptimized(t *testing.T) {
	t.Parallel()

	t.Run("chunk processing with memory optimization", func(t *testing.T) {
		t.Parallel()

		// Skip time-consuming tests in local development
		if os.Getenv("GITHUB_ACTIONS") == "" {
			t.Skip("Skipping large data chunked reading test in local development")
		}

		// Create XLSX with more rows than chunk size
		buf := createTestXLSX(t, 25, 3) // 25 rows, 3 columns

		parser := newStreamingParser(FileTypeXLSX, CompressionNone, "test_table", 5) // 5 rows per chunk

		var processedChunks int
		var totalRecords int

		_, err := parser.ProcessInChunks(buf, func(chunk *tableChunk) error {
			processedChunks++
			totalRecords += len(chunk.records)

			// Verify chunk structure
			assert.Equal(t, "test_table", chunk.tableName)
			assert.Equal(t, 3, len(chunk.headers)) // 3 columns
			assert.LessOrEqual(t, len(chunk.records), 5, "chunk size should not exceed limit")
			assert.Greater(t, len(chunk.records), 0, "chunk should not be empty")

			return nil
		})

		require.NoError(t, err)
		assert.Equal(t, 5, processedChunks, "should process 5 chunks (25/5)")
		assert.Equal(t, 25, totalRecords, "should process all 25 records")
	})

	t.Run("memory pressure handling", func(t *testing.T) {
		t.Parallel()

		// Skip time-consuming tests in local development
		if os.Getenv("GITHUB_ACTIONS") == "" {
			t.Skip("Skipping memory pressure test in local development")
		}

		buf := createTestXLSX(t, 20, 3)

		parser := newStreamingParser(FileTypeXLSX, CompressionNone, "test_table", 10)
		parser.memoryLimit = newMemoryLimit(5) // Very small limit

		var processedChunks int

		_, err := parser.ProcessInChunks(buf, func(chunk *tableChunk) error {
			processedChunks++
			// Chunk size might be reduced due to memory pressure
			assert.LessOrEqual(t, len(chunk.records), 10, "chunk size should be at most original limit")
			return nil
		})

		// Should either succeed with reduced chunk sizes or fail with memory error
		if err != nil {
			assert.Contains(t, err.Error(), "memory limit exceeded",
				"should fail with memory limit error")
		} else {
			assert.Greater(t, processedChunks, 0, "should process at least one chunk")
		}
	})

	t.Run("empty XLSX handling", func(t *testing.T) {
		t.Parallel()
		// Create XLSX with headers only
		buf := createTestXLSX(t, 0, 3) // 0 data rows

		parser := newStreamingParser(FileTypeXLSX, CompressionNone, "test_table", 5)

		var processedChunks int
		var lastChunk *tableChunk

		_, err := parser.ProcessInChunks(buf, func(chunk *tableChunk) error {
			processedChunks++
			lastChunk = chunk
			return nil
		})

		require.NoError(t, err)
		// Header-only files should process exactly one chunk with headers but no records
		assert.Equal(t, 1, processedChunks, "should process one chunk for header-only file")
		if lastChunk != nil {
			assert.Equal(t, 3, len(lastChunk.getHeaders()), "should have correct number of headers")
			assert.Equal(t, 0, len(lastChunk.getRecords()), "should have no records")
		}
	})

	t.Run("error in chunk processor", func(t *testing.T) {
		t.Parallel()
		buf := createTestXLSX(t, 10, 2)

		parser := newStreamingParser(FileTypeXLSX, CompressionNone, "test_table", 5)

		_, err := parser.ProcessInChunks(buf, func(_ *tableChunk) error {
			return assert.AnError // Return an error to test error handling
		})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "chunk processor error", "should wrap processor error")
	})
}

func TestStreamingParser_MemoryPoolIntegration(t *testing.T) {
	t.Parallel()

	t.Run("memory pool usage during XLSX processing", func(t *testing.T) {
		t.Parallel()
		parser := newStreamingParser(FileTypeXLSX, CompressionNone, "test_table", 10)

		// Verify memory pool is properly initialized
		assert.NotNil(t, parser.memoryPool, "memory pool should be initialized")
		assert.NotNil(t, parser.memoryLimit, "memory limit should be initialized")

		// Test that we can get resources from the pool
		buf := parser.memoryPool.getByteBuffer()
		assert.NotNil(t, buf, "should get byte buffer from pool")
		parser.memoryPool.putByteBuffer(buf)

		records := parser.memoryPool.getRecordSlice()
		assert.NotNil(t, records, "should get record slice from pool")
		parser.memoryPool.putRecordSlice(records)

		strings := parser.memoryPool.getStringSlice()
		assert.NotNil(t, strings, "should get string slice from pool")
		parser.memoryPool.putStringSlice(strings)
	})

	t.Run("memory limit configuration", func(t *testing.T) {
		t.Parallel()
		parser := newStreamingParser(FileTypeXLSX, CompressionNone, "test_table", 10)

		// Test memory limit operations
		assert.True(t, parser.memoryLimit.isEnabled(), "memory limit should be enabled by default")

		info := parser.memoryLimit.getMemoryInfo()
		assert.Greater(t, info.limitMB, int64(0), "memory limit should be positive")

		// Test disabling
		parser.memoryLimit.disable()
		assert.False(t, parser.memoryLimit.isEnabled(), "memory limit should be disabled")

		// Re-enable
		parser.memoryLimit.enable()
		assert.True(t, parser.memoryLimit.isEnabled(), "memory limit should be re-enabled")
	})
}

func TestStreamingParser_XLSXErrorHandling(t *testing.T) {
	t.Parallel()

	t.Run("invalid XLSX data", func(t *testing.T) {
		t.Parallel()
		parser := newStreamingParser(FileTypeXLSX, CompressionNone, "test_table", 10)

		// Test with invalid XLSX data
		invalidData := strings.NewReader("not an xlsx file")

		_, err := parser.parseFromReader(invalidData)
		assert.Error(t, err, "should fail with invalid XLSX data")
		assert.Contains(t, err.Error(), "failed to open XLSX file", "should have appropriate error message")
	})

	t.Run("empty reader", func(t *testing.T) {
		t.Parallel()
		parser := newStreamingParser(FileTypeXLSX, CompressionNone, "test_table", 10)

		// Test with empty reader
		emptyReader := strings.NewReader("")

		_, err := parser.parseFromReader(emptyReader)
		assert.Error(t, err, "should fail with empty reader")
	})
}

func BenchmarkStreamingParser_XLSXProcessing(b *testing.B) {
	// Skip time-consuming benchmarks in local development
	if os.Getenv("GITHUB_ACTIONS") == "" {
		b.Skip("Skipping large XLSX benchmark in local development")
	}

	// Create a moderately large XLSX for benchmarking
	createTestData := func() []byte {
		file := excelize.NewFile()
		defer file.Close()

		// Create headers
		for col := range 5 {
			cell, err := excelize.CoordinatesToCellName(col+1, 1)
			if err != nil {
				b.Fatalf("failed to create cell name: %v", err)
			}
			if err := file.SetCellValue("Sheet1", cell, "Column"+string(rune('A'+col))); err != nil {
				b.Fatalf("failed to set cell value: %v", err)
			}
		}

		// Create 1000 data rows
		for row := 2; row <= 1001; row++ {
			for col := range 5 {
				cell, err := excelize.CoordinatesToCellName(col+1, row)
				if err != nil {
					b.Fatalf("failed to create cell name: %v", err)
				}
				value := "Row" + string(rune('0'+(row-2)%10)) + "Col" + string(rune('A'+col))
				if err := file.SetCellValue("Sheet1", cell, value); err != nil {
					b.Fatalf("failed to set cell value: %v", err)
				}
			}
		}

		var buf bytes.Buffer
		if err := file.Write(&buf); err != nil {
			b.Fatalf("failed to write file: %v", err)
		}
		return buf.Bytes()
	}

	testData := createTestData()

	b.Run("parseXLSXStream", func(b *testing.B) {
		parser := newStreamingParser(FileTypeXLSX, CompressionNone, "bench_table", 100)

		b.ResetTimer()
		for range b.N {
			// Create fresh reader for each iteration
			reader := bytes.NewReader(testData)

			_, err := parser.parseFromReader(reader)
			if err != nil {
				b.Fatalf("benchmark failed: %v", err)
			}
		}
	})

	b.Run("processXLSXInChunks", func(b *testing.B) {
		parser := newStreamingParser(FileTypeXLSX, CompressionNone, "bench_table", 100)

		b.ResetTimer()
		for range b.N {
			// Create fresh reader for each iteration
			reader := bytes.NewReader(testData)

			_, err := parser.ProcessInChunks(reader, func(chunk *tableChunk) error {
				// Minimal processing to avoid skewing benchmark
				_ = chunk.headers
				_ = chunk.records
				return nil
			})
			if err != nil {
				b.Fatalf("benchmark failed: %v", err)
			}
		}
	})
}

func TestMemoryPool_GetByteBuffer_Fallback(t *testing.T) {
	t.Parallel()

	pool := newMemoryPool(1024)

	// Get a buffer - should work normally
	buf := pool.getByteBuffer()
	assert.NotNil(t, buf)
	assert.Equal(t, 0, len(buf))

	// Test Put and Get cycle
	buf = append(buf, []byte("test data")...)
	pool.putByteBuffer(buf)

	buf2 := pool.getByteBuffer()
	assert.NotNil(t, buf2)
	assert.Equal(t, 0, len(buf2)) // Should be reset
}

func TestMemoryPool_GetRecordSlice_Fallback(t *testing.T) {
	t.Parallel()

	pool := newMemoryPool(1024)

	// Get a slice - should work normally
	slice := pool.getRecordSlice()
	assert.NotNil(t, slice)
	assert.Equal(t, 0, len(slice))

	// Test Put and Get cycle
	slice = append(slice, record{"test"})
	pool.putRecordSlice(slice)

	slice2 := pool.getRecordSlice()
	assert.NotNil(t, slice2)
	assert.Equal(t, 0, len(slice2)) // Should be reset
}

func TestMemoryPool_GetStringSlice_Fallback(t *testing.T) {
	t.Parallel()

	pool := newMemoryPool(1024)

	// Get a slice - should work normally
	slice := pool.getStringSlice()
	assert.NotNil(t, slice)
	assert.Equal(t, 0, len(slice))

	// Test Put and Get cycle
	slice = append(slice, "test")
	pool.putStringSlice(slice)

	slice2 := pool.getStringSlice()
	assert.NotNil(t, slice2)
	assert.Equal(t, 0, len(slice2)) // Should be reset
}
