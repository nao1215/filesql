package filesql

import (
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
)

// Memory management constants
const (
	// Default capacities for pooled objects
	defaultByteSliceCapacity   = 1024 // 1KB
	defaultRecordSliceCapacity = 100
	defaultStringSliceCapacity = 10

	// Memory size calculations
	defaultMemoryPoolSize    = 1024 * 1024 // 1MB
	defaultMemoryLimit       = 512         // 512MB
	maxReasonableMemoryLimit = 64 * 1024   // 64GB - reasonable upper bound for most systems
	averageRecordSizeFactor  = 64          // Used to calculate record slice size limits
	averageStringSizeFactor  = 32          // Used to calculate string slice size limits
	forceGCThresholdMB       = 100         // Force GC when heap exceeds 100MB

	// Memory warning threshold
	defaultWarningThreshold = 0.8 // 80%

	// Memory conversion constants
	bytesPerMB = 1024 * 1024

	// Atomic operation values
	atomicEnabled  = 1
	atomicDisabled = 0
)

// pooledByteSlice wraps []byte for pooling
type pooledByteSlice struct {
	data []byte
}

// pooledRecordSlice wraps []Record for pooling
type pooledRecordSlice struct {
	data []record
}

// pooledStringSlice wraps []string for pooling
type pooledStringSlice struct {
	data []string
}

// memoryPool manages a pool of reusable byte slices, record slices, and string slices
// to reduce memory allocations during file processing operations.
//
// It is internal. The only pool this package builds is the one newStreamingParser
// makes at a hardcoded 1MB, and nothing on DBBuilder passes another in, so the
// exported form promised a knob a caller had no way to turn.
//
// The pool automatically manages object lifecycles and includes safeguards against
// memory leaks by limiting the maximum size of objects that can be returned to the pool.
// Objects that grow beyond maxSize are discarded rather than pooled.
//
// Usage example:
//
//	pool := newMemoryPool(1024 * 1024) // 1MB max buffer size
//	buffer := pool.getByteBuffer()
//	defer pool.putByteBuffer(buffer)
//	// Use buffer...
//
// Thread Safety: All methods are safe for concurrent use by multiple goroutines.
type memoryPool struct {
	bytePool   sync.Pool // Pool for []byte slices
	recordPool sync.Pool // Pool for []record slices
	stringPool sync.Pool // Pool for []string slices
	mu         sync.RWMutex
	maxSize    int // Maximum buffer size to pool
}

// newMemoryPool creates a new memory pool with configurable max buffer size
func newMemoryPool(maxSize int) *memoryPool {
	if maxSize <= 0 {
		maxSize = defaultMemoryPoolSize
	}

	return &memoryPool{
		maxSize: maxSize,
		bytePool: sync.Pool{
			New: func() any {
				return &pooledByteSlice{
					data: make([]byte, 0, defaultByteSliceCapacity),
				}
			},
		},
		recordPool: sync.Pool{
			New: func() any {
				return &pooledRecordSlice{
					data: make([]record, 0, defaultRecordSliceCapacity),
				}
			},
		},
		stringPool: sync.Pool{
			New: func() any {
				return &pooledStringSlice{
					data: make([]string, 0, defaultStringSliceCapacity),
				}
			},
		},
	}
}

// getByteBuffer gets a byte buffer from the pool
func (mp *memoryPool) getByteBuffer() []byte {
	pooled, ok := mp.bytePool.Get().(*pooledByteSlice)
	if !ok {
		// This should never happen with our pool setup, but provide fallback
		return make([]byte, 0, defaultByteSliceCapacity)
	}
	pooled.data = pooled.data[:0] // Reset length but keep capacity
	return pooled.data
}

// putByteBuffer returns a byte buffer to the pool if it's not too large
func (mp *memoryPool) putByteBuffer(buf []byte) {
	if cap(buf) <= mp.maxSize {
		mp.bytePool.Put(&pooledByteSlice{data: buf})
	}
}

// getRecordSlice gets a record slice from the pool
func (mp *memoryPool) getRecordSlice() []record {
	pooled, ok := mp.recordPool.Get().(*pooledRecordSlice)
	if !ok {
		// This should never happen with our pool setup, but provide fallback
		return make([]record, 0, defaultRecordSliceCapacity)
	}
	pooled.data = pooled.data[:0] // Reset length but keep capacity
	return pooled.data
}

// putRecordSlice returns a record slice to the pool if it's not too large
func (mp *memoryPool) putRecordSlice(slice []record) {
	if cap(slice) <= mp.maxSize/averageRecordSizeFactor {
		mp.recordPool.Put(&pooledRecordSlice{data: slice})
	}
}

// getStringSlice gets a string slice from the pool
func (mp *memoryPool) getStringSlice() []string {
	pooled, ok := mp.stringPool.Get().(*pooledStringSlice)
	if !ok {
		// This should never happen with our pool setup, but provide fallback
		return make([]string, 0, defaultStringSliceCapacity)
	}
	pooled.data = pooled.data[:0] // Reset length but keep capacity
	return pooled.data
}

// putStringSlice returns a string slice to the pool if it's not too large
func (mp *memoryPool) putStringSlice(slice []string) {
	if cap(slice) <= mp.maxSize/averageStringSizeFactor {
		mp.stringPool.Put(&pooledStringSlice{data: slice})
	}
}

// forceGC forces garbage collection and clears pools if memory pressure is high
func (mp *memoryPool) forceGC() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// If heap size is over threshold, clear pools and force GC
	if memStats.HeapAlloc > forceGCThresholdMB*bytesPerMB {
		mp.mu.Lock()
		mp.bytePool = sync.Pool{
			New: mp.bytePool.New,
		}
		mp.recordPool = sync.Pool{
			New: mp.recordPool.New,
		}
		mp.stringPool = sync.Pool{
			New: mp.stringPool.New,
		}
		mp.mu.Unlock()

		runtime.GC()
	}
}

// memoryLimit provides configurable memory limits with graceful degradation
// for file processing operations. It monitors heap usage and can trigger
// memory management actions when thresholds are exceeded.
//
// It is internal, for the same reason as memoryPool: the only limit this package
// builds is newStreamingParser's hardcoded 512MB, and there is no way to supply
// another. "Configurable" describes the type, not anything a caller can reach.
//
// The system supports three states:
//   - OK: Memory usage is within acceptable limits
//   - WARNING: Memory usage approaches the limit, suggesting reduced chunk sizes
//   - EXCEEDED: Memory usage has exceeded the limit, processing should be halted
//
// Usage example:
//
//	limit := newMemoryLimit(512) // 512MB limit
//	if limit.checkMemoryUsage() == memoryStatusExceeded {
//	    return limit.createMemoryError("processing")
//	}
//
// Performance Note: checkMemoryUsage() calls runtime.ReadMemStats which can
// pause for milliseconds. Use sparingly in hot paths.
//
// Thread Safety: All methods are safe for concurrent use by multiple goroutines.
type memoryLimit struct {
	maxMemoryMB      int64   // Maximum memory limit in MB
	warningThreshold float64 // Warning threshold as percentage (0.0-1.0)
	enabled          int32   // Atomic flag for enable/disable
}

// newMemoryLimit creates a new memory limit configuration
func newMemoryLimit(maxMemoryMB int64) *memoryLimit {
	// Validate lower bound
	if maxMemoryMB <= 0 {
		maxMemoryMB = defaultMemoryLimit
	}

	// Validate upper bound to prevent unreasonable memory limits
	if maxMemoryMB > maxReasonableMemoryLimit {
		maxMemoryMB = maxReasonableMemoryLimit
	}

	return &memoryLimit{
		maxMemoryMB:      maxMemoryMB,
		warningThreshold: defaultWarningThreshold,
		enabled:          atomicEnabled,
	}
}

// IsEnabled returns whether memory limits are enabled
func (ml *memoryLimit) isEnabled() bool {
	return atomic.LoadInt32(&ml.enabled) == atomicEnabled
}

// Enable enables memory limit checking
func (ml *memoryLimit) enable() {
	atomic.StoreInt32(&ml.enabled, atomicEnabled)
}

// Disable disables memory limit checking
func (ml *memoryLimit) disable() {
	atomic.StoreInt32(&ml.enabled, atomicDisabled)
}

// setWarningThreshold sets the warning threshold (0.0-1.0)
func (ml *memoryLimit) setWarningThreshold(threshold float64) {
	if threshold > 0.0 && threshold <= 1.0 {
		ml.warningThreshold = threshold
	}
}

// checkMemoryUsage checks current memory usage against limits
func (ml *memoryLimit) checkMemoryUsage() memoryStatus {
	if !ml.isEnabled() {
		return memoryStatusOK
	}

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// Convert to MB safely to avoid potential overflow
	// Note: For extremely large heaps (>9 exabytes), precision may be lost due to
	// uint64 -> int64 conversion, but this is beyond realistic system limits
	heapAllocMB := memStats.HeapAlloc / bytesPerMB
	// Safe conversion: prevent overflow by checking bounds
	var currentMB int64
	if heapAllocMB > uint64(math.MaxInt64) {
		currentMB = math.MaxInt64 // Cap at max int64 (extremely unlikely scenario)
	} else {
		currentMB = int64(heapAllocMB)
	}
	maxMB := ml.maxMemoryMB

	if currentMB >= maxMB {
		return memoryStatusExceeded
	}

	usage := float64(currentMB) / float64(maxMB)
	if usage >= ml.warningThreshold {
		return memoryStatusWarning
	}

	return memoryStatusOK
}

// getMemoryInfo returns current memory usage information
func (ml *memoryLimit) getMemoryInfo() memoryInfo {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// Convert to MB safely to avoid potential overflow
	// Note: For extremely large heaps (>9 exabytes), precision may be lost due to
	// uint64 -> int64 conversion, but this is beyond realistic system limits
	heapAllocMB := memStats.HeapAlloc / bytesPerMB
	// Safe conversion: prevent overflow by checking bounds
	var currentMB int64
	if heapAllocMB > uint64(math.MaxInt64) {
		currentMB = math.MaxInt64 // Cap at max int64 (extremely unlikely scenario)
	} else {
		currentMB = int64(heapAllocMB)
	}
	maxMB := ml.maxMemoryMB
	usage := float64(currentMB) / float64(maxMB)

	return memoryInfo{
		currentMB: currentMB,
		limitMB:   maxMB,
		usage:     usage,
		status:    ml.checkMemoryUsage(),
	}
}

// shouldReduceChunkSize returns true if chunk size should be reduced for memory management
func (ml *memoryLimit) shouldReduceChunkSize(chunkSize int) (bool, int) {
	status := ml.checkMemoryUsage()

	switch status {
	case memoryStatusWarning:
		// Reduce chunk size by 50%
		return true, chunkSize / 2
	case memoryStatusExceeded:
		// Reduce chunk size by 75%
		return true, chunkSize / 4
	default:
		return false, chunkSize
	}
}

// createMemoryError creates a memory limit error with helpful context
func (ml *memoryLimit) createMemoryError(operation string) error {
	info := ml.getMemoryInfo()
	return fmt.Errorf(
		"memory limit exceeded during %s: using %d MB / %d MB (%.1f%%), "+
			"consider reducing chunk size or increasing memory limit",
		operation, info.currentMB, info.limitMB, info.usage*100,
	)
}

// memoryStatus represents the current memory status
type memoryStatus int

// Memory status constants
const (
	// memoryStatusOK indicates memory usage is within acceptable limits
	memoryStatusOK memoryStatus = iota
	// memoryStatusWarning indicates memory usage is approaching the limit
	memoryStatusWarning
	// memoryStatusExceeded indicates memory usage has exceeded the limit
	memoryStatusExceeded
)

// String returns string representation of memory status
func (ms memoryStatus) String() string {
	switch ms {
	case memoryStatusOK:
		return "OK"
	case memoryStatusWarning:
		return "WARNING"
	case memoryStatusExceeded:
		return "EXCEEDED"
	default:
		return "UNKNOWN"
	}
}

// memoryInfo contains detailed memory usage information
type memoryInfo struct {
	currentMB int64        // Current memory usage in MB
	limitMB   int64        // Memory limit in MB
	usage     float64      // usage percentage (0.0-1.0)
	status    memoryStatus // Current status
}
