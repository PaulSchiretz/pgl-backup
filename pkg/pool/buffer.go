package pool

import (
	"fmt"
	"math/bits"
	"sync"
)

// BucketedBufferPool provides an O(1) lookup for reusable byte slices.
type BucketedBufferPool struct {
	minBucketExp int
	maxBucketExp int
	maxPoolSize  int64
	pools        []sync.Pool // Changed from array to slice for configurability
}

// NewBucketedBufferPool creates a pool based on raw size boundaries.
// Both minSize and maxSize MUST be powers of two (e.g., 1024, 4096, 1048576).
func NewBucketedBufferPool(minSize, maxSize int64) *BucketedBufferPool {
	// 1. Validate that inputs are powers of two
	if !isPowerOfTwo(minSize) {
		panic(fmt.Sprintf("minSize %d must be a power of two", minSize))
	}
	if !isPowerOfTwo(maxSize) {
		panic(fmt.Sprintf("maxSize %d must be a power of two", maxSize))
	}
	if maxSize <= minSize {
		panic("maxSize must be greater than minSize")
	}

	// 2. Convert raw sizes to exponents (2^n)
	// bits.TrailingZeros returns the exponent for a power-of-two number
	// e.g., 1024 (10000000000 in binary) has 10 trailing zeros.
	minExp := bits.TrailingZeros64(uint64(minSize))
	maxExp := bits.TrailingZeros64(uint64(maxSize))

	bp := &BucketedBufferPool{
		minBucketExp: minExp,
		maxBucketExp: maxExp,
		maxPoolSize:  int64(1) << maxExp,
		pools:        make([]sync.Pool, maxExp+1),
	}

	for i := minExp; i <= maxExp; i++ {
		size := int64(1) << i
		bp.pools[i].New = func() any {
			b := make([]byte, int(size))
			return &b
		}
	}
	return bp
}

// Get retrieves a pointer to a byte slice of at least 'size'.
func (bp *BucketedBufferPool) Get(size int64) *[]byte {
	// Handle size 0 or negative: make([]byte, 0) is backed by runtime.zerobase
	// which is practically free and should NOT be pooled.
	if size <= 0 {
		b := make([]byte, 0)
		return &b
	}

	// If size exceeds our largest bucket, allocate a fresh slice.
	// We don't pool huge buffers to prevent memory bloat.
	if size > bp.maxPoolSize {
		b := make([]byte, int(size))
		return &b
	}

	// bit.Len64 finds the smallest power of 2 that is >= size.
	// For example, if size is 700, Len64(699) returns 10 (2^10 = 1024)
	idx := bits.Len64(uint64(size - 1))
	if idx < bp.minBucketExp {
		idx = bp.minBucketExp
	}

	// Sub-slice to the exact requested size. This is critical so that
	// io.ReadFull or io.Copy only use the requested amount of space.
	bufPtr := bp.pools[idx].Get().(*[]byte)
	*bufPtr = (*bufPtr)[:int(size)]
	return bufPtr
}

// Put returns the buffer to the pool if it matches one of our bucket capacities.
func (bp *BucketedBufferPool) Put(bufPtr *[]byte) {
	if bufPtr == nil {
		return
	}

	capacity := int64(cap(*bufPtr))

	// THE BOUNCER: Only pool if it fits our constraints.
	// 1. Must be >= minSize
	// 2. Must be <= maxSize
	// 3. Must be a power of two (ensures it was likely one of ours)
	if capacity < (int64(1)<<bp.minBucketExp) || capacity > bp.maxPoolSize || !isPowerOfTwo(capacity) {
		return
	}

	idx := bits.TrailingZeros64(uint64(capacity))

	// Reset the slice to its full capacity before putting it back
	// so the next Get() has the full buffer available.
	*bufPtr = (*bufPtr)[:capacity]
	bp.pools[idx].Put(bufPtr)
}

// FixedBufferPool remains largely the same, just keeping sizes as int for slice compatibility
type FixedBufferPool struct {
	size int64
	pool sync.Pool
}

func NewFixedBuffer(size int64) *FixedBufferPool {
	return &FixedBufferPool{
		size: size,
		pool: sync.Pool{
			New: func() any {
				b := make([]byte, int(size))
				return &b
			},
		},
	}
}

func (fp *FixedBufferPool) Get() *[]byte {
	return fp.pool.Get().(*[]byte)
}

func (fp *FixedBufferPool) Put(b *[]byte) {
	// Only put it back if it's the right size.
	if b == nil || int64(cap(*b)) != fp.size {
		return
	}
	*b = (*b)[:fp.size]
	fp.pool.Put(b)
}
