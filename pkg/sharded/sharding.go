package sharded

import "hash/fnv"

// getShardIndex calculates the shard index for a given key.
// It uses the FNV-1a hash algorithm.
// numShards must be a power of 2 for the bitwise AND optimization to work correctly.
func getShardIndex(key string, numShards int) int {
	h := fnv.New32a()
	// Write never returns an error for FNV-1a, so we ignore the return value.
	h.Write([]byte(key))
	hashValue := h.Sum32()
	// Optimization: Use bitwise AND for power-of-2 modulus.
	return int(hashValue & uint32(numShards-1))
}
