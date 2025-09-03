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
	data []Record
}

// pooledStringSlice wraps []string for pooling
type pooledStringSlice struct {
	data []string
}

// MemoryPool manages a pool of reusable byte slices, record slices, and string slices
// to reduce memory allocations during file processing operations.
//
// The pool automatically manages object lifecycles and includes safeguards against
// memory leaks by limiting the maximum size of objects that can be returned to the pool.
// Objects that grow beyond maxSize are discarded rather than pooled.
//
// Usage example:
//
//	pool := NewMemoryPool(1024 * 1024) // 1MB max buffer size
//	buffer := pool.GetByteBuffer()
//	defer pool.PutByteBuffer(buffer)
//	// Use buffer...
//
// Thread Safety: All methods are safe for concurrent use by multiple goroutines.
type MemoryPool struct {
	bytePool   sync.Pool // Pool for []byte slices
	recordPool sync.Pool // Pool for []record slices
	stringPool sync.Pool // Pool for []string slices
	mu         sync.RWMutex
	maxSize    int // Maximum buffer size to pool
}

// NewMemoryPool creates a new memory pool with configurable max buffer size
func NewMemoryPool(maxSize int) *MemoryPool {
	if maxSize <= 0 {
		maxSize = defaultMemoryPoolSize
	}

	return &MemoryPool{
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
					data: make([]Record, 0, defaultRecordSliceCapacity),
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

// GetByteBuffer gets a byte buffer from the pool
func (mp *MemoryPool) GetByteBuffer() []byte {
	pooled, ok := mp.bytePool.Get().(*pooledByteSlice)
	if !ok {
		// This should never happen with our pool setup, but provide fallback
		return make([]byte, 0, defaultByteSliceCapacity)
	}
	pooled.data = pooled.data[:0] // Reset length but keep capacity
	return pooled.data
}

// PutByteBuffer returns a byte buffer to the pool if it's not too large
func (mp *MemoryPool) PutByteBuffer(buf []byte) {
	if cap(buf) <= mp.maxSize {
		mp.bytePool.Put(&pooledByteSlice{data: buf})
	}
}

// GetRecordSlice gets a record slice from the pool
func (mp *MemoryPool) GetRecordSlice() []Record {
	pooled, ok := mp.recordPool.Get().(*pooledRecordSlice)
	if !ok {
		// This should never happen with our pool setup, but provide fallback
		return make([]Record, 0, defaultRecordSliceCapacity)
	}
	pooled.data = pooled.data[:0] // Reset length but keep capacity
	return pooled.data
}

// PutRecordSlice returns a record slice to the pool if it's not too large
func (mp *MemoryPool) PutRecordSlice(slice []Record) {
	if cap(slice) <= mp.maxSize/averageRecordSizeFactor {
		mp.recordPool.Put(&pooledRecordSlice{data: slice})
	}
}

// GetStringSlice gets a string slice from the pool
func (mp *MemoryPool) GetStringSlice() []string {
	pooled, ok := mp.stringPool.Get().(*pooledStringSlice)
	if !ok {
		// This should never happen with our pool setup, but provide fallback
		return make([]string, 0, defaultStringSliceCapacity)
	}
	pooled.data = pooled.data[:0] // Reset length but keep capacity
	return pooled.data
}

// PutStringSlice returns a string slice to the pool if it's not too large
func (mp *MemoryPool) PutStringSlice(slice []string) {
	if cap(slice) <= mp.maxSize/averageStringSizeFactor {
		mp.stringPool.Put(&pooledStringSlice{data: slice})
	}
}

// ForceGC forces garbage collection and clears pools if memory pressure is high
func (mp *MemoryPool) ForceGC() {
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

// MemoryLimit provides configurable memory limits with graceful degradation
// for file processing operations. It monitors heap usage and can trigger
// memory management actions when thresholds are exceeded.
//
// The system supports three states:
//   - OK: Memory usage is within acceptable limits
//   - WARNING: Memory usage approaches the limit, suggesting reduced chunk sizes
//   - EXCEEDED: Memory usage has exceeded the limit, processing should be halted
//
// Usage example:
//
//	limit := NewMemoryLimit(512) // 512MB limit
//	if limit.CheckMemoryUsage() == MemoryStatusExceeded {
//	    return limit.CreateMemoryError("processing")
//	}
//
// Performance Note: CheckMemoryUsage() calls runtime.ReadMemStats which can
// pause for milliseconds. Use sparingly in hot paths.
//
// Thread Safety: All methods are safe for concurrent use by multiple goroutines.
type MemoryLimit struct {
	maxMemoryMB      int64   // Maximum memory limit in MB
	warningThreshold float64 // Warning threshold as percentage (0.0-1.0)
	enabled          int32   // Atomic flag for enable/disable
}

// NewMemoryLimit creates a new memory limit configuration
func NewMemoryLimit(maxMemoryMB int64) *MemoryLimit {
	// Validate lower bound
	if maxMemoryMB <= 0 {
		maxMemoryMB = defaultMemoryLimit
	}

	// Validate upper bound to prevent unreasonable memory limits
	if maxMemoryMB > maxReasonableMemoryLimit {
		maxMemoryMB = maxReasonableMemoryLimit
	}

	return &MemoryLimit{
		maxMemoryMB:      maxMemoryMB,
		warningThreshold: defaultWarningThreshold,
		enabled:          atomicEnabled,
	}
}

// IsEnabled returns whether memory limits are enabled
func (ml *MemoryLimit) IsEnabled() bool {
	return atomic.LoadInt32(&ml.enabled) == atomicEnabled
}

// Enable enables memory limit checking
func (ml *MemoryLimit) Enable() {
	atomic.StoreInt32(&ml.enabled, atomicEnabled)
}

// Disable disables memory limit checking
func (ml *MemoryLimit) Disable() {
	atomic.StoreInt32(&ml.enabled, atomicDisabled)
}

// SetWarningThreshold sets the warning threshold (0.0-1.0)
func (ml *MemoryLimit) SetWarningThreshold(threshold float64) {
	if threshold > 0.0 && threshold <= 1.0 {
		ml.warningThreshold = threshold
	}
}

// CheckMemoryUsage checks current memory usage against limits
func (ml *MemoryLimit) CheckMemoryUsage() MemoryStatus {
	if !ml.IsEnabled() {
		return MemoryStatusOK
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
		return MemoryStatusExceeded
	}

	usage := float64(currentMB) / float64(maxMB)
	if usage >= ml.warningThreshold {
		return MemoryStatusWarning
	}

	return MemoryStatusOK
}

// GetMemoryInfo returns current memory usage information
func (ml *MemoryLimit) GetMemoryInfo() MemoryInfo {
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

	return MemoryInfo{
		CurrentMB: currentMB,
		LimitMB:   maxMB,
		Usage:     usage,
		Status:    ml.CheckMemoryUsage(),
	}
}

// ShouldReduceChunkSize returns true if chunk size should be reduced for memory management
func (ml *MemoryLimit) ShouldReduceChunkSize(chunkSize int) (bool, int) {
	status := ml.CheckMemoryUsage()

	switch status {
	case MemoryStatusWarning:
		// Reduce chunk size by 50%
		return true, chunkSize / 2
	case MemoryStatusExceeded:
		// Reduce chunk size by 75%
		return true, chunkSize / 4
	default:
		return false, chunkSize
	}
}

// CreateMemoryError creates a memory limit error with helpful context
func (ml *MemoryLimit) CreateMemoryError(operation string) error {
	info := ml.GetMemoryInfo()
	return fmt.Errorf(
		"memory limit exceeded during %s: using %d MB / %d MB (%.1f%%), "+
			"consider reducing chunk size or increasing memory limit",
		operation, info.CurrentMB, info.LimitMB, info.Usage*100,
	)
}

// MemoryStatus represents the current memory status
type MemoryStatus int

// Memory status constants
const (
	// MemoryStatusOK indicates memory usage is within acceptable limits
	MemoryStatusOK MemoryStatus = iota
	// MemoryStatusWarning indicates memory usage is approaching the limit
	MemoryStatusWarning
	// MemoryStatusExceeded indicates memory usage has exceeded the limit
	MemoryStatusExceeded
)

// String returns string representation of memory status
func (ms MemoryStatus) String() string {
	switch ms {
	case MemoryStatusOK:
		return "OK"
	case MemoryStatusWarning:
		return "WARNING"
	case MemoryStatusExceeded:
		return "EXCEEDED"
	default:
		return "UNKNOWN"
	}
}

// MemoryInfo contains detailed memory usage information
type MemoryInfo struct {
	CurrentMB int64        // Current memory usage in MB
	LimitMB   int64        // Memory limit in MB
	Usage     float64      // Usage percentage (0.0-1.0)
	Status    MemoryStatus // Current status
}
