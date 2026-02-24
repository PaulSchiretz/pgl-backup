package pool

// sync.Pool is a mechanism to cache allocated but unused objects for later reuse,
// relieving pressure on the garbage collector. It is safe for concurrent use.
//
// Mechanics:
//   - Get(): Retrieves an arbitrary item from the Pool, removing it. If the Pool
//     is empty, it calls New (if defined) or returns nil. It prioritizes local
//     per-P caches to minimize lock contention.
//   - Put(): Adds an item to the Pool.
//   - GC: Items in the Pool are automatically removed during garbage collection.
//     Therefore, sync.Pool is suitable for short-lived objects (like buffers)
//     but not for persistent resources like database connections.
func isPowerOfTwo(n int64) bool {
	return n > 0 && (n&(n-1)) == 0
}
