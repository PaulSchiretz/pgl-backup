package limiter

import (
	"sync"
)

// Memory limiter manages a shared memory budget to control concurrency
// based on memory usage rather than just a fixed number of workers.
// It is thread-safe.
type Memory struct {
	mu                sync.Mutex
	available         int64
	capacity          int64
	priorityThreshold int64 // Data smaller than this bypasses strict limits
}

// NewMemory creates a new memory limiter with the specified total capacity in bytes.
func NewMemory(limit int64, priorityThreshold int64) *Memory {
	return &Memory{
		available:         limit,
		capacity:          limit,
		priorityThreshold: priorityThreshold,
	}
}

// TryAcquire attempts to reserve 'n' bytes from the memory budget.
// It returns true if the reservation was successful.
// It returns false if there is not enough budget currently available,
// or if 'n' is greater than the total capacity of the limiter.
func (m *Memory) TryAcquire(n int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	// LOGIC A: The Priority Path (Greedy)
	// If the data is tiny, we take it even if we are out of budget.
	if n <= m.priorityThreshold {
		m.available -= n
		return true
	}

	// LOGIC B: The Strict Path
	// For large files, we only allow it if it fits in the remaining capacity.
	if n > m.capacity {
		return false
	}

	if m.available >= n {
		m.available -= n
		return true
	}

	return false
}

// Release returns 'n' bytes back to the budget.
// This must be called after a successful TryAcquire.
func (m *Memory) Release(n int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.available += n

	// IMPORTANT: Sanity check, prevent available memory from exceeding capacity
	// in case we have a lot of data < priorityThreshold this could happen
	if m.available > m.capacity {
		m.available = m.capacity
	}
}

// Available returns the amount of memory currently available.
// Useful for metrics or debugging.
func (m *Memory) Available() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.available
}

// Capacity returns the total capacity of the limiter.
func (m *Memory) Capacity() int64 {
	// Capacity is immutable, no lock needed technically, but for consistency:
	return m.capacity
}
