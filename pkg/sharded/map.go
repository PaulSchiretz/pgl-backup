package sharded

import (
	"sync"
)

// Define a sharded structure
type mapShard struct {
	mu    sync.RWMutex
	items map[string]any
}

type Map []*mapShard

func NewMap(numShards int) *Map {
	if !isPowerOfTwo(numShards) {
		panic("num shards must be a power of 2")
	}
	s := make(Map, numShards)
	for i := range numShards {
		s[i] = &mapShard{items: make(map[string]any)}
	}
	return &s
}

func (s *Map) getShard(key string) *mapShard {
	shardIndex := getShardIndex(key, len(*s))
	return (*s)[shardIndex]
}

// Store adds a key-value pair to the map.
func (s *Map) Store(key string, value any) {
	shard := s.getShard(key)
	shard.mu.Lock()
	// Store the interface{} value
	shard.items[key] = value
	shard.mu.Unlock()
}

// Load retrieves the value associated with a key.
// It returns the value and a boolean indicating if the key was present.
func (s *Map) Load(key string) (value any, ok bool) {
	shard := s.getShard(key)
	shard.mu.RLock()
	// Access the map using the standard Go map return pattern
	value, ok = shard.items[key]
	shard.mu.RUnlock()
	return value, ok
}

// Has checks only for the presence of a key.
func (s *Map) Has(key string) bool {
	shard := s.getShard(key)
	shard.mu.RLock()
	_, exists := shard.items[key]
	shard.mu.RUnlock()
	return exists
}

// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (s *Map) LoadOrStore(key string, value any) (actual any, loaded bool) {
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

func (s *Map) Delete(key string) {
	shard := s.getShard(key)
	shard.mu.Lock()
	delete(shard.items, key)
	shard.mu.Unlock()
}

// Count returns the total number of elements in the map.
func (s *Map) Count() int {
	count := 0
	for i := range len(*s) {
		shard := (*s)[i]
		shard.mu.RLock()
		count += len(shard.items)
		shard.mu.RUnlock()
	}
	return count
}

// Keys returns a slice of all keys in the map.
// The order of keys is not guaranteed.
func (s *Map) Keys() []string {
	// Pre-allocate the slice with the total number of elements to avoid re-allocations.
	keys := make([]string, 0, s.Count())
	for i := range len(*s) {
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
func (s *Map) Items() map[string]any {
	// Pre-allocate the map with the total number of elements to avoid re-allocations.
	items := make(map[string]any, s.Count())
	for i := range len(*s) {
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
func (s *Map) Range(f func(key string, value any) bool) {
	for i := range len(*s) {
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
func (s *Map) Clear() {
	for i := range len(*s) {
		shard := (*s)[i]
		shard.mu.Lock()
		shard.items = make(map[string]any)
		shard.mu.Unlock()
	}
}

// ShardCount returns the number of elements in a specific shard.
// It returns -1 if the shardIndex is out of bounds.
func (s *Map) ShardCount(shardIndex int) int {
	if shardIndex < 0 || shardIndex >= len(*s) {
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
func (s *Map) GetShardIndex(key string) int {
	return getShardIndex(key, len(*s))
}
