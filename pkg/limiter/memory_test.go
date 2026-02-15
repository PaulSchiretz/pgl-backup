package limiter

import (
	"sync"
	"testing"
	"time"
)

func TestMemoryLimiter_Basics(t *testing.T) {
	limit := int64(100)
	ml := NewMemory(limit)

	// 1. Acquire valid amount
	if !ml.TryAcquire(60) {
		t.Errorf("Expected to acquire 60, but failed")
	}
	if got := ml.Available(); got != 40 {
		t.Errorf("Expected 40 available, got %d", got)
	}

	// 2. Fail to acquire exceeding amount
	if ml.TryAcquire(50) {
		t.Errorf("Expected to fail acquiring 50 (only 40 left), but succeeded")
	}

	// 3. Release and re-acquire
	ml.Release(60)
	if got := ml.Available(); got != 100 {
		t.Errorf("Expected 100 available after release, got %d", got)
	}
	if !ml.TryAcquire(50) {
		t.Errorf("Expected to acquire 50 after release, but failed")
	}
}

func TestMemoryLimiter_OversizedRequest(t *testing.T) {
	limit := int64(100)
	ml := NewMemory(limit)

	// Requesting more than total capacity should always fail
	if ml.TryAcquire(101) {
		t.Errorf("Expected to fail acquiring 101 (cap 100), but succeeded")
	}
}

func TestMemoryLimiter_ReleaseSanity(t *testing.T) {
	limit := int64(100)
	ml := NewMemory(limit)

	ml.TryAcquire(50)

	// Accidental double release or logic bug in caller
	ml.Release(50)
	ml.Release(50)

	if got := ml.Available(); got != 100 {
		t.Errorf("Expected available to be capped at 100, got %d", got)
	}
}

func TestMemoryLimiter_Concurrency(t *testing.T) {
	// This test simulates multiple workers trying to grab memory simultaneously.
	// We set a small limit to force contention.
	totalCapacity := int64(1000)
	ml := NewMemory(totalCapacity)

	var wg sync.WaitGroup
	workers := 100
	requestSize := int64(100)

	// We will track how many times we successfully acquired the lock
	successCount := 0
	var countMu sync.Mutex

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Try to acquire. If successful, hold it briefly, then release.
			if ml.TryAcquire(requestSize) {
				countMu.Lock()
				successCount++
				countMu.Unlock()

				// Simulate work
				time.Sleep(1 * time.Millisecond)

				ml.Release(requestSize)
			}
		}()
	}

	wg.Wait()

	// Verify that the limiter is back to full capacity
	if got := ml.Available(); got != totalCapacity {
		t.Errorf("Expected full capacity %d after concurrent usage, got %d", totalCapacity, got)
	}

	// Note: We can't assert exactly how many succeeded because TryAcquire is non-blocking.
	// Some might fail immediately if the pool is empty at that microsecond.
	// But we expect at least some to have succeeded.
	if successCount == 0 {
		t.Log("Warning: No workers succeeded, which is statistically unlikely but possible.")
	}
}
