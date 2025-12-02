package sharded

import (
	"hash/fnv"
	"sync"
)

// Define a sharded structure
const numMapShards = 64 // Power of 2 for fast bitwise mod

type mapShard struct {
	mu    sync.Mutex
	items map[string]interface{}
}

type ShardedMap []*mapShard

func NewShardedMap() *ShardedMap {
	s := make(ShardedMap, numMapShards)
	for i := 0; i < numMapShards; i++ {
		s[i] = &mapShard{items: make(map[string]interface{})}
	}
	return &s
}

// FNV-1a hash function (fast, low collision)
func (s *ShardedMap) getShard(key string) *mapShard {
	// 1. Create a 32-bit FNV-1a hash.
	h := fnv.New32a()
	// 2. Write the key bytes to the hash.
	// NOTE: Write() for FNV-1a never returns an error, so we ignore the second return value.
	h.Write([]byte(key))
	// 3. Get the 32-bit hash sum.
	hashValue := h.Sum32()

	// Optimization: Use bitwise AND for power-of-2 modulus.
	// This efficiently maps the hash to a shard index [0, 63].
	shardIndex := hashValue & (numSetShards - 1)

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
	shard.mu.Lock()
	// Access the map using the standard Go map return pattern
	value, ok = shard.items[key]
	shard.mu.Unlock()
	return value, ok
}

// Has checks only for the presence of a key.
func (s *ShardedMap) Has(key string) bool {
	shard := s.getShard(key)
	shard.mu.Lock()
	_, exists := shard.items[key]
	shard.mu.Unlock()
	return exists
}

func (s *ShardedMap) Delete(key string) {
	shard := s.getShard(key)
	shard.mu.Lock()
	delete(shard.items, key)
	shard.mu.Unlock()
}
