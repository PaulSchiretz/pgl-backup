package sharded

import (
	"hash/fnv"
	"sync"
)

// Define a sharded structure
const numSetShards = 64 // Power of 2 for fast bitwise mod

type setShard struct {
	mu    sync.Mutex
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

// FNV-1a hash function (fast, low collision)
func (s *ShardedSet) getShard(key string) *setShard {
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
func (s *ShardedSet) Store(key string) {
	shard := s.getShard(key)
	shard.mu.Lock()
	shard.items[key] = struct{}{}
	shard.mu.Unlock()
}

// Has checks only for the presence of a key.
func (s *ShardedSet) Has(key string) bool {
	shard := s.getShard(key)
	shard.mu.Lock()
	_, exists := shard.items[key]
	shard.mu.Unlock()
	return exists
}

func (s *ShardedSet) Delete(key string) {
	shard := s.getShard(key)
	shard.mu.Lock()
	delete(shard.items, key)
	shard.mu.Unlock()
}
