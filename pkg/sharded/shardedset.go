package sharded

import (
	"sync"
)

// Define a sharded structure
const numSetShards = 64 // Power of 2 for fast bitwise mod

type setShard struct {
	mu    sync.RWMutex
	items map[string]struct{}
}

type ShardedSet []*setShard

func NewShardedSet() *ShardedSet {
	s := make(ShardedSet, numSetShards)
	for i := 0; i < numSetShards; i++ {
		s[i] = &setShard{items: make(map[string]struct{})}
	}
	return &s
}

func (s *ShardedSet) getShard(key string) *setShard {
	shardIndex := getShardIndex(key, numSetShards)
	return (*s)[shardIndex]
}

// Store adds a key-value pair to the map.
func (s *ShardedSet) Store(key string) {
	shard := s.getShard(key)
	shard.mu.Lock()
	shard.items[key] = struct{}{}
	shard.mu.Unlock()
}

// Has checks only for the presence of a key.
func (s *ShardedSet) Has(key string) bool {
	shard := s.getShard(key)
	shard.mu.RLock()
	_, exists := shard.items[key]
	shard.mu.RUnlock()
	return exists
}

// LoadOrStore ensures a key is present in the set, returning true if it was already present.
// It returns false if the key was newly stored. This is an atomic operation.
func (s *ShardedSet) LoadOrStore(key string) (loaded bool) {
	shard := s.getShard(key)
	shard.mu.Lock()
	_, loaded = shard.items[key]
	if !loaded {
		shard.items[key] = struct{}{}
	}
	shard.mu.Unlock()
	return loaded
}

func (s *ShardedSet) Delete(key string) {
	shard := s.getShard(key)
	shard.mu.Lock()
	delete(shard.items, key)
	shard.mu.Unlock()
}

// Count returns the total number of elements in the set.
func (s *ShardedSet) Count() int {
	count := 0
	for i := 0; i < numSetShards; i++ {
		shard := (*s)[i]
		shard.mu.RLock()
		count += len(shard.items)
		shard.mu.RUnlock()
	}
	return count
}

// Keys returns a slice of all keys in the set.
// The order of keys is not guaranteed.
func (s *ShardedSet) Keys() []string {
	// Pre-allocate the slice with the total number of elements to avoid re-allocations.
	keys := make([]string, 0, s.Count())
	for i := 0; i < numSetShards; i++ {
		shard := (*s)[i]
		shard.mu.RLock()
		for k := range shard.items {
			keys = append(keys, k)
		}
		shard.mu.RUnlock()
	}
	return keys
}

// Range calls f sequentially for each key present in the set.
// If f returns false, range stops the iteration.
//
// The iteration is performed by locking one shard at a time, so it does not
// block the entire set. However, the set should not be modified by the
// callback function f.
func (s *ShardedSet) Range(f func(key string) bool) {
	for i := 0; i < numSetShards; i++ {
		shard := (*s)[i]
		shard.mu.RLock()
		for k := range shard.items {
			if !f(k) {
				shard.mu.RUnlock()
				return
			}
		}
		shard.mu.RUnlock()
	}
}

// Clear removes all keys from the set.
func (s *ShardedSet) Clear() {
	for i := 0; i < numSetShards; i++ {
		shard := (*s)[i]
		shard.mu.Lock()
		shard.items = make(map[string]struct{})
		shard.mu.Unlock()
	}
}

// ShardCount returns the number of elements in a specific shard.
// It returns -1 if the shardIndex is out of bounds.
func (s *ShardedSet) ShardCount(shardIndex int) int {
	if shardIndex < 0 || shardIndex >= numSetShards {
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
func (s *ShardedSet) GetShardIndex(key string) int {
	return getShardIndex(key, numSetShards)
}
