package limiter

import (
	"sync"
	"testing"
	"time"
)

func TestMemoryLimiter_Basics(t *testing.T) {
	limit := int64(100)
	// Use a small priority threshold to test the standard (non-greedy) acquire/release logic.
	ml := NewMemory(limit, 10)

	// 1. Acquire valid amount (larger than priority threshold)
	if !ml.TryAcquire(60) {
		t.Errorf("Expected to acquire 60, but failed")
	}
	if got := ml.Available(); got != 40 {
		t.Errorf("Expected 40 available, got %d", got)
	}

	// 2. Fail to acquire exceeding amount (larger than priority threshold)
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
	// Use a priority threshold smaller than the request to test the strict path.
	ml := NewMemory(limit, 50)

	// Requesting more than total capacity should always fail on the strict path.
	if ml.TryAcquire(101) {
		t.Errorf("Expected to fail acquiring 101 (cap 100), but succeeded")
	}
}

func TestMemoryLimiter_ReleaseSanity(t *testing.T) {
	limit := int64(100)
	ml := NewMemory(limit, 128)

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
	// Use a priority threshold smaller than the request size to ensure we test contention
	// on the strict path, not just the always-succeeding greedy path.
	ml := NewMemory(totalCapacity, 50)

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

func TestMemoryLimiter_PriorityPath(t *testing.T) {
	limit := int64(100)
	priorityThreshold := int64(10)
	ml := NewMemory(limit, priorityThreshold)

	// 1. Acquire most of the memory, forcing subsequent large acquires to fail.
	if !ml.TryAcquire(95) {
		t.Fatalf("Expected to acquire 95, but failed")
	}
	if got := ml.Available(); got != 5 {
		t.Fatalf("Expected 5 available, got %d", got)
	}

	// 2. Fail to acquire a chunk that is over the available limit and not on the priority path.
	if ml.TryAcquire(11) { // 11 is > priorityThreshold and > available
		t.Errorf("Expected to fail acquiring 11, but succeeded")
	}

	// 3. Succeed in acquiring a small chunk via the priority path (greedy).
	if !ml.TryAcquire(10) { // 10 is <= priorityThreshold
		t.Errorf("Expected to succeed acquiring 10 via priority path, but failed")
	}

	// 4. Check that available memory is now negative.
	if got := ml.Available(); got != -5 {
		t.Errorf("Expected -5 available after greedy acquire, got %d", got)
	}

	// 5. Another greedy acquire should still work.
	if !ml.TryAcquire(5) { // 5 is <= priorityThreshold
		t.Errorf("Expected to succeed acquiring another 5 via priority path, but failed")
	}
	if got := ml.Available(); got != -10 {
		t.Errorf("Expected -10 available after second greedy acquire, got %d", got)
	}

	// 6. Release the greedy amounts.
	ml.Release(10)
	ml.Release(5)
	if got := ml.Available(); got != 5 {
		t.Errorf("Expected 5 available after releasing greedy amounts, got %d", got)
	}

	// 7. Release the initial large amount.
	ml.Release(95)
	if got := ml.Available(); got != 100 {
		t.Errorf("Expected 100 available after all releases, got %d", got)
	}
}

func TestMemoryLimiter_ReleaseSanity_WithPriority(t *testing.T) {
	limit := int64(100)
	priorityThreshold := int64(10)
	ml := NewMemory(limit, priorityThreshold)

	// Acquire all memory via the strict path.
	if !ml.TryAcquire(100) {
		t.Fatal("Failed to acquire initial 100")
	}

	// Now, perform several "greedy" acquires that drive `available` negative.
	for i := 0; i < 5; i++ {
		if !ml.TryAcquire(priorityThreshold) {
			t.Fatalf("Greedy acquire %d failed unexpectedly", i+1)
		}
	}

	// Available should be 100 (limit) - 100 (strict) - (5 * 10) (greedy) = -50
	if got := ml.Available(); got != -50 {
		t.Fatalf("Expected -50 available, got %d", got)
	}

	// Now release everything. The sanity check in Release() should prevent `available` from exceeding `capacity`.
	ml.Release(100) // Release the initial strict acquire.
	for i := 0; i < 5; i++ {
		ml.Release(priorityThreshold) // Release the greedy acquires.
	}

	if got := ml.Available(); got != limit {
		t.Errorf("Expected available to be capped at %d, but got %d", limit, got)
	}
}
