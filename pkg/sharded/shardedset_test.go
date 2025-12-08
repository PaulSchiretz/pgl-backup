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

// TestShardedSet_Count tests the Count method.
func TestShardedSet_Count(t *testing.T) {
	s := NewShardedSet()

	// 1. Test count on an empty set
	if count := s.Count(); count != 0 {
		t.Errorf("Count() on empty set = %d; want 0", count)
	}

	// 2. Test count after adding items
	s.Store("key1")
	s.Store("key2")
	if count := s.Count(); count != 2 {
		t.Errorf("Count() after adding 2 items = %d; want 2", count)
	}

	// 3. Test count after adding a duplicate item (should not change)
	s.Store("key1")
	if count := s.Count(); count != 2 {
		t.Errorf("Count() after adding a duplicate item = %d; want 2", count)
	}

	// 4. Test count after deleting an item
	s.Delete("key2")
	if count := s.Count(); count != 1 {
		t.Errorf("Count() after deleting an item = %d; want 1", count)
	}
}

// TestShardedSet_Keys tests the Keys method.
func TestShardedSet_Keys(t *testing.T) {
	s := NewShardedSet()

	// 1. Test on an empty set
	if len(s.Keys()) != 0 {
		t.Errorf("Keys() on empty set should return empty slice, got %v", s.Keys())
	}

	// 2. Test after adding items
	expectedKeys := map[string]bool{"key1": true, "key2": true, "key3": true}
	for k := range expectedKeys {
		s.Store(k)
	}

	keys := s.Keys()
	if len(keys) != len(expectedKeys) {
		t.Errorf("Keys() returned %d keys; want %d", len(keys), len(expectedKeys))
	}

	// Convert slice to map for easy lookup
	returnedKeys := make(map[string]bool)
	for _, k := range keys {
		returnedKeys[k] = true
	}

	for k := range expectedKeys {
		if !returnedKeys[k] {
			t.Errorf("Expected key %q was not found in Keys() result", k)
		}
	}
}

// TestShardedSet_Range tests the Range method.
func TestShardedSet_Range(t *testing.T) {
	s := NewShardedSet()

	// Populate the set
	expectedKeys := map[string]bool{
		"key1": true,
		"key2": true,
		"key3": true,
		"key4": true,
	}
	for k := range expectedKeys {
		s.Store(k)
	}

	// 1. Test full iteration
	t.Run("FullIteration", func(t *testing.T) {
		visitedKeys := make(map[string]bool)
		s.Range(func(key string) bool {
			visitedKeys[key] = true
			return true // Continue iteration
		})

		if len(visitedKeys) != len(expectedKeys) {
			t.Errorf("Range visited %d keys; want %d", len(visitedKeys), len(expectedKeys))
		}

		for k := range expectedKeys {
			if !visitedKeys[k] {
				t.Errorf("Range did not visit key %q", k)
			}
		}
	})

	// 2. Test early exit
	t.Run("EarlyExit", func(t *testing.T) {
		visitedCount := 0
		s.Range(func(key string) bool {
			visitedCount++
			return false // Stop iteration immediately
		})

		if visitedCount != 1 {
			t.Errorf("Range should have stopped after 1 item, but visited %d", visitedCount)
		}
	})
}

// TestShardedSet_Clear tests the Clear method.
func TestShardedSet_Clear(t *testing.T) {
	s := NewShardedSet()

	// Populate the set
	s.Store("key1")
	s.Store("key2")

	if s.Count() != 2 {
		t.Fatalf("Expected count of 2 before Clear(), got %d", s.Count())
	}

	// Clear the set
	s.Clear()

	if count := s.Count(); count != 0 {
		t.Errorf("Count() after Clear() = %d; want 0", count)
	}

	if s.Has("key1") {
		t.Errorf("Has(\"key1\") after Clear() = true; want false")
	}
}

// TestShardedSet_ShardCount tests the ShardCount method.
func TestShardedSet_ShardCount(t *testing.T) {
	s := NewShardedSet()

	// 1. Test invalid indices
	if count := s.ShardCount(-1); count != -1 {
		t.Errorf("ShardCount(-1) = %d; want -1", count)
	}
	if count := s.ShardCount(numSetShards); count != -1 {
		t.Errorf("ShardCount(%d) = %d; want -1", numSetShards, count)
	}

	// 2. Test on an empty set
	if count := s.ShardCount(0); count != 0 {
		t.Errorf("ShardCount(0) on empty set = %d; want 0", count)
	}

	// 3. Populate set and check sum of shard counts
	totalItems := 1000
	for i := 0; i < totalItems; i++ {
		s.Store(fmt.Sprintf("key-%d", i))
	}

	sumOfShardCounts := 0
	for i := 0; i < numSetShards; i++ {
		sumOfShardCounts += s.ShardCount(i)
	}

	if sumOfShardCounts != totalItems {
		t.Errorf("Sum of all ShardCount() results = %d; want %d", sumOfShardCounts, totalItems)
	}
}

// TestShardedSet_GetShardIndex tests the GetShardIndex method.
func TestShardedSet_GetShardIndex(t *testing.T) {
	s := NewShardedSet()
	key := "my-test-key"

	// 1. Get the shard index for a key
	index := s.GetShardIndex(key)
	if index < 0 || index >= numSetShards {
		t.Fatalf("GetShardIndex(%q) returned an out-of-bounds index: %d", key, index)
	}

	// 2. Verify that storing the key increases the count of that specific shard
	initialShardCount := s.ShardCount(index)

	s.Store(key)

	finalShardCount := s.ShardCount(index)
	if finalShardCount != initialShardCount+1 {
		t.Errorf("Shard count for index %d did not increase after storing key. Got %d, want %d", index, finalShardCount, initialShardCount+1)
	}
}

// TestShardedSet_LoadOrStore tests the LoadOrStore method.
func TestShardedSet_LoadOrStore(t *testing.T) {
	s := NewShardedSet()
	key := "test_key"

	// 1. First time storing the key
	loaded := s.LoadOrStore(key)
	if loaded {
		t.Errorf("LoadOrStore on new key returned true; want false")
	}

	// Verify the key is now in the set
	if !s.Has(key) {
		t.Errorf("Has(%q) returned false after LoadOrStore; want true", key)
	}

	// 2. Second time calling with the same key
	loaded = s.LoadOrStore(key)
	if !loaded {
		t.Errorf("LoadOrStore on existing key returned false; want true")
	}

	// Verify count is still 1
	if count := s.Count(); count != 1 {
		t.Errorf("Count() after second LoadOrStore is %d; want 1", count)
	}
}
