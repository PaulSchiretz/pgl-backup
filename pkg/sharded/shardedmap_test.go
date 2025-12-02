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
