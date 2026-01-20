package sharded

import (
	"fmt"
	"sync"
)

// Define a sharded structure
const numMapShards = 64 // Power of 2 for fast bitwise mod

type mapShard struct {
	mu    sync.RWMutex
	items map[string]interface{}
}

type ShardedMap []*mapShard

func NewShardedMap() (*ShardedMap, error) {
	if !isPowerOfTwo(numMapShards) {
		return nil, fmt.Errorf("numMapShards must be a power of 2")
	}
	s := make(ShardedMap, numMapShards)
	for i := 0; i < numMapShards; i++ {
		s[i] = &mapShard{items: make(map[string]interface{})}
	}
	return &s, nil
}

func (s *ShardedMap) getShard(key string) *mapShard {
	shardIndex := getShardIndex(key, numMapShards)
	return (*s)[shardIndex]
}

// Store adds a key-value pair to the map.
func (s *ShardedMap) Store(key string, value interface{}) {
	shard := s.getShard(key)
	shard.mu.Lock()
	// Store the interface{} value
	shard.items[key] = value
	shard.mu.Unlock()
}

// Load retrieves the value associated with a key.
// It returns the value and a boolean indicating if the key was present.
func (s *ShardedMap) Load(key string) (value interface{}, ok bool) {
	shard := s.getShard(key)
	shard.mu.RLock()
	// Access the map using the standard Go map return pattern
	value, ok = shard.items[key]
	shard.mu.RUnlock()
	return value, ok
}

// Has checks only for the presence of a key.
func (s *ShardedMap) Has(key string) bool {
	shard := s.getShard(key)
	shard.mu.RLock()
	_, exists := shard.items[key]
	shard.mu.RUnlock()
	return exists
}

// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (s *ShardedMap) LoadOrStore(key string, value interface{}) (actual interface{}, loaded bool) {
	shard := s.getShard(key)
	shard.mu.Lock()
	actual, loaded = shard.items[key]
	if !loaded {
		actual = value
		shard.items[key] = value
	}
	shard.mu.Unlock()
	return actual, loaded
}

func (s *ShardedMap) Delete(key string) {
	shard := s.getShard(key)
	shard.mu.Lock()
	delete(shard.items, key)
	shard.mu.Unlock()
}

// Count returns the total number of elements in the map.
func (s *ShardedMap) Count() int {
	count := 0
	for i := 0; i < numMapShards; i++ {
		shard := (*s)[i]
		shard.mu.RLock()
		count += len(shard.items)
		shard.mu.RUnlock()
	}
	return count
}

// Keys returns a slice of all keys in the map.
// The order of keys is not guaranteed.
func (s *ShardedMap) Keys() []string {
	// Pre-allocate the slice with the total number of elements to avoid re-allocations.
	keys := make([]string, 0, s.Count())
	for i := 0; i < numMapShards; i++ {
		shard := (*s)[i]
		shard.mu.RLock()
		for k := range shard.items {
			keys = append(keys, k)
		}
		shard.mu.RUnlock()
	}
	return keys
}

// Items returns a map containing all key-value pairs.
// This creates a snapshot of the map's data at the time of the call.
func (s *ShardedMap) Items() map[string]interface{} {
	// Pre-allocate the map with the total number of elements to avoid re-allocations.
	items := make(map[string]interface{}, s.Count())
	for i := 0; i < numMapShards; i++ {
		shard := (*s)[i]
		shard.mu.RLock()
		for k, v := range shard.items {
			items[k] = v
		}
		shard.mu.RUnlock()
	}
	return items
}

// Range calls f sequentially for each key and value present in the map.
// If f returns false, range stops the iteration.
//
// The iteration is performed by locking one shard at a time, so it does not
// block the entire map. However, the map should not be modified by the
// callback function f.
func (s *ShardedMap) Range(f func(key string, value interface{}) bool) {
	for i := 0; i < numMapShards; i++ {
		shard := (*s)[i]
		shard.mu.RLock()
		for k, v := range shard.items {
			if !f(k, v) {
				shard.mu.RUnlock()
				return
			}
		}
		shard.mu.RUnlock()
	}
}

// Clear removes all key-value pairs from the map.
func (s *ShardedMap) Clear() {
	for i := 0; i < numMapShards; i++ {
		shard := (*s)[i]
		shard.mu.Lock()
		shard.items = make(map[string]interface{})
		shard.mu.Unlock()
	}
}

// ShardCount returns the number of elements in a specific shard.
// It returns -1 if the shardIndex is out of bounds.
func (s *ShardedMap) ShardCount(shardIndex int) int {
	if shardIndex < 0 || shardIndex >= numMapShards {
		return -1
	}
	shard := (*s)[shardIndex]
	shard.mu.RLock()
	count := len(shard.items)
	shard.mu.RUnlock()
	return count
}

// GetShardIndex returns the shard index for a given key.
// This is useful for diagnostics or understanding key distribution.
func (s *ShardedMap) GetShardIndex(key string) int {
	return getShardIndex(key, numMapShards)
}
