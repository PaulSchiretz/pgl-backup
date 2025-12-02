package sharded

import (
	"fmt"
	"sync"
	"testing"
)

// TestShardedSet_Basic tests the fundamental Store, Has, and Delete operations.
func TestShardedSet_Basic(t *testing.T) {
	s := NewShardedSet()
	key := "test_key"

	// 1. Test Has on a non-existent key
	if s.Has(key) {
		t.Errorf("Has(%q) = true; want false for non-existent key", key)
	}

	// 2. Test Store and Has
	s.Store(key)
	if !s.Has(key) {
		t.Errorf("Has(%q) = false; want true after storing", key)
	}

	// 3. Test Store on an existing key (idempotency)
	s.Store(key)
	if !s.Has(key) {
		t.Errorf("Has(%q) = false; want true after storing again", key)
	}

	// 4. Test Delete
	s.Delete(key)
	if s.Has(key) {
		t.Errorf("Has(%q) = true; want false after deleting", key)
	}

	// 5. Test Delete on a non-existent key (idempotency)
	s.Delete(key)
	if s.Has(key) {
		t.Errorf("Has(%q) = true; want false after deleting again", key)
	}
}

// TestShardedSet_MultipleKeys tests functionality with multiple keys,
// ensuring they don't interfere with each other.
func TestShardedSet_MultipleKeys(t *testing.T) {
	s := NewShardedSet()
	keys := []string{"key1", "key2", "another_key", "long/path/style/key"}

	// Store all keys
	for _, key := range keys {
		s.Store(key)
	}

	// Verify all keys are present
	for _, key := range keys {
		if !s.Has(key) {
			t.Errorf("Has(%q) = false; want true", key)
		}
	}

	// Delete one key and verify
	s.Delete(keys[1])
	if s.Has(keys[1]) {
		t.Errorf("Has(%q) = true after delete; want false", keys[1])
	}

	// Verify other keys are still present
	if !s.Has(keys[0]) {
		t.Errorf("Has(%q) = false after deleting another key; want true", keys[0])
	}
	if !s.Has(keys[2]) {
		t.Errorf("Has(%q) = false after deleting another key; want true", keys[2])
	}
}

// TestShardedSet_Concurrency tests concurrent access to the ShardedSet.
// It runs Store, Has, and Delete operations from multiple goroutines.
func TestShardedSet_Concurrency(t *testing.T) {
	s := NewShardedSet()
	numGoroutines := 100
	numKeysPerGoroutine := 100
	var wg sync.WaitGroup

	// Concurrent Store
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < numKeysPerGoroutine; j++ {
				key := fmt.Sprintf("key-%d-%d", goroutineID, j)
				s.Store(key)
			}
		}(i)
	}
	wg.Wait()

	// Concurrent Has and Delete
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < numKeysPerGoroutine; j++ {
				key := fmt.Sprintf("key-%d-%d", goroutineID, j)
				if !s.Has(key) {
					t.Errorf("concurrent Has failed for key %s", key)
				}
				s.Delete(key)
			}
		}(i)
	}
	wg.Wait()
}
