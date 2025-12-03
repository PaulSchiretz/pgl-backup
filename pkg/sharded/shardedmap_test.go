package sharded

import (
	"fmt"
	"sync"
	"testing"
)

// TestShardedMap_Basic tests the fundamental Store, Load, Has, and Delete operations.
func TestShardedMap_Basic(t *testing.T) {
	m := NewShardedMap()
	key := "test_key"
	value := "test_value"

	// 1. Test Load/Has on a non-existent key
	if val, ok := m.Load(key); ok {
		t.Errorf("Load(%q) = %v, %v; want nil, false for non-existent key", key, val, ok)
	}
	if m.Has(key) {
		t.Errorf("Has(%q) = true; want false for non-existent key", key)
	}

	// 2. Test Store and Load/Has
	m.Store(key, value)
	if val, ok := m.Load(key); !ok || val != value {
		t.Errorf("Load(%q) = %v, %v; want %v, true", key, val, ok, value)
	}
	if !m.Has(key) {
		t.Errorf("Has(%q) = false; want true after storing", key)
	}

	// 3. Test Store to overwrite an existing key
	newValue := "new_value"
	m.Store(key, newValue)
	if val, ok := m.Load(key); !ok || val != newValue {
		t.Errorf("Load(%q) = %v, %v; want %v, true after overwrite", key, val, ok, newValue)
	}

	// 4. Test Delete
	m.Delete(key)
	if val, ok := m.Load(key); ok {
		t.Errorf("Load(%q) = %v, %v; want nil, false after deleting", key, val, ok)
	}
	if m.Has(key) {
		t.Errorf("Has(%q) = true; want false after deleting", key)
	}
}

// TestShardedMap_MultipleKeys tests functionality with multiple keys,
// ensuring they don't interfere with each other.
func TestShardedMap_MultipleKeys(t *testing.T) {
	m := NewShardedMap()
	testData := map[string]int{
		"key1": 100,
		"key2": 200,
		"key3": 300,
	}

	// Store all key-value pairs
	for k, v := range testData {
		m.Store(k, v)
	}

	// Verify all pairs are present and correct
	for k, v := range testData {
		val, ok := m.Load(k)
		if !ok {
			t.Errorf("Load(%q) failed; key not found", k)
			continue
		}
		if val.(int) != v {
			t.Errorf("Load(%q) = %v; want %v", k, val, v)
		}
	}

	// Delete one key and verify
	m.Delete("key2")
	if m.Has("key2") {
		t.Errorf("Has(\"key2\") = true after delete; want false")
	}

	// Verify other keys are still present
	if !m.Has("key1") {
		t.Errorf("Has(\"key1\") = false after deleting another key; want true")
	}
	if !m.Has("key3") {
		t.Errorf("Has(\"key3\") = false after deleting another key; want true")
	}
}

// TestShardedMap_Concurrency tests concurrent access to the ShardedMap.
// It runs Store, Load, and Delete operations from multiple goroutines.
func TestShardedMap_Concurrency(t *testing.T) {
	m := NewShardedMap()
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
				value := goroutineID*1000 + j
				m.Store(key, value)
			}
		}(i)
	}
	wg.Wait()

	// Concurrent Load and Delete
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < numKeysPerGoroutine; j++ {
				key := fmt.Sprintf("key-%d-%d", goroutineID, j)
				expectedValue := goroutineID*1000 + j

				val, ok := m.Load(key)
				if !ok || val.(int) != expectedValue {
					t.Errorf("concurrent Load failed for key %s: got %v, %v; want %v, true", key, val, ok, expectedValue)
				}

				m.Delete(key)
			}
		}(i)
	}
	wg.Wait()
}

// TestShardedMap_Count tests the Count method.
func TestShardedMap_Count(t *testing.T) {
	m := NewShardedMap()

	// 1. Test count on an empty map
	if count := m.Count(); count != 0 {
		t.Errorf("Count() on empty map = %d; want 0", count)
	}

	// 2. Test count after adding items
	m.Store("key1", 1)
	m.Store("key2", 2)
	if count := m.Count(); count != 2 {
		t.Errorf("Count() after adding 2 items = %d; want 2", count)
	}

	// 3. Test count after overwriting an item (should not change)
	m.Store("key1", 100)
	if count := m.Count(); count != 2 {
		t.Errorf("Count() after overwriting an item = %d; want 2", count)
	}

	// 4. Test count after deleting an item
	m.Delete("key2")
	if count := m.Count(); count != 1 {
		t.Errorf("Count() after deleting an item = %d; want 1", count)
	}
}

// TestShardedMap_Keys tests the Keys method.
func TestShardedMap_Keys(t *testing.T) {
	m := NewShardedMap()

	// 1. Test on an empty map
	if len(m.Keys()) != 0 {
		t.Errorf("Keys() on empty map should return empty slice, got %v", m.Keys())
	}

	// 2. Test after adding items
	expectedKeys := map[string]bool{"key1": true, "key2": true, "key3": true}
	for k := range expectedKeys {
		m.Store(k, "some-value")
	}

	keys := m.Keys()
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

// TestShardedMap_Range tests the Range method.
func TestShardedMap_Range(t *testing.T) {
	m := NewShardedMap()

	// Populate the map
	expectedItems := map[string]interface{}{
		"key1": 1,
		"key2": "two",
		"key3": true,
		"key4": 4.0,
	}
	for k, v := range expectedItems {
		m.Store(k, v)
	}

	// 1. Test full iteration
	t.Run("FullIteration", func(t *testing.T) {
		visitedItems := make(map[string]interface{})
		m.Range(func(key string, value interface{}) bool {
			visitedItems[key] = value
			return true // Continue iteration
		})

		if len(visitedItems) != len(expectedItems) {
			t.Errorf("Range visited %d items; want %d", len(visitedItems), len(expectedItems))
		}

		for k, expectedV := range expectedItems {
			if v, ok := visitedItems[k]; !ok || v != expectedV {
				t.Errorf("Range did not visit or has wrong value for key %q: got %v, %v; want %v, true", k, v, ok, expectedV)
			}
		}
	})

	// 2. Test early exit
	t.Run("EarlyExit", func(t *testing.T) {
		visitedCount := 0
		m.Range(func(key string, value interface{}) bool {
			visitedCount++
			return false // Stop iteration immediately
		})

		if visitedCount != 1 {
			t.Errorf("Range should have stopped after 1 item, but visited %d", visitedCount)
		}
	})
}

// TestShardedMap_Items tests the Items method.
func TestShardedMap_Items(t *testing.T) {
	m := NewShardedMap()

	// 1. Test on an empty map
	if len(m.Items()) != 0 {
		t.Errorf("Items() on empty map should return empty map, got %v", m.Items())
	}

	// 2. Test after adding items
	expectedItems := map[string]interface{}{
		"key1": 123,
		"key2": "value2",
		"key3": true,
	}
	for k, v := range expectedItems {
		m.Store(k, v)
	}

	items := m.Items()
	if len(items) != len(expectedItems) {
		t.Fatalf("Items() returned %d items; want %d", len(items), len(expectedItems))
	}

	for k, expectedV := range expectedItems {
		if v, ok := items[k]; !ok || v != expectedV {
			t.Errorf("Items() map is missing or has wrong value for key %q: got %v, %v; want %v, true", k, v, ok, expectedV)
		}
	}
}

// TestShardedMap_Clear tests the Clear method.
func TestShardedMap_Clear(t *testing.T) {
	m := NewShardedMap()

	// Populate the map
	m.Store("key1", 1)
	m.Store("key2", "two")

	if m.Count() != 2 {
		t.Fatalf("Expected count of 2 before Clear(), got %d", m.Count())
	}

	// Clear the map
	m.Clear()

	if count := m.Count(); count != 0 {
		t.Errorf("Count() after Clear() = %d; want 0", count)
	}

	if m.Has("key1") {
		t.Errorf("Has(\"key1\") after Clear() = true; want false")
	}
}

// TestShardedMap_ShardCount tests the ShardCount method.
func TestShardedMap_ShardCount(t *testing.T) {
	m := NewShardedMap()

	// 1. Test invalid indices
	if count := m.ShardCount(-1); count != -1 {
		t.Errorf("ShardCount(-1) = %d; want -1", count)
	}
	if count := m.ShardCount(numMapShards); count != -1 {
		t.Errorf("ShardCount(%d) = %d; want -1", numMapShards, count)
	}

	// 2. Test on an empty map
	if count := m.ShardCount(0); count != 0 {
		t.Errorf("ShardCount(0) on empty map = %d; want 0", count)
	}

	// 3. Populate map and check sum of shard counts
	totalItems := 1000
	for i := 0; i < totalItems; i++ {
		m.Store(fmt.Sprintf("key-%d", i), i)
	}

	sumOfShardCounts := 0
	for i := 0; i < numMapShards; i++ {
		sumOfShardCounts += m.ShardCount(i)
	}

	if sumOfShardCounts != totalItems {
		t.Errorf("Sum of all ShardCount() results = %d; want %d", sumOfShardCounts, totalItems)
	}
}

// TestShardedMap_GetShardIndex tests the GetShardIndex method.
func TestShardedMap_GetShardIndex(t *testing.T) {
	m := NewShardedMap()
	key := "my-test-key"

	// 1. Get the shard index for a key
	index := m.GetShardIndex(key)
	if index < 0 || index >= numMapShards {
		t.Fatalf("GetShardIndex(%q) returned an out-of-bounds index: %d", key, index)
	}

	// 2. Verify that storing the key increases the count of that specific shard
	initialShardCount := m.ShardCount(index)

	m.Store(key, "some-value")

	finalShardCount := m.ShardCount(index)
	if finalShardCount != initialShardCount+1 {
		t.Errorf("Shard count for index %d did not increase after storing key. Got %d, want %d", index, finalShardCount, initialShardCount+1)
	}
}
