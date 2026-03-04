package sharded

import "math/bits"

const offset32 = 2166136261
const prime32 = 16777619

// getShardIndex calculates the shard index for a given key using the FNV-1a hash algorithm.
//
// PERFORMANCE NOTE: This is implemented inline rather than using the
// standard library's hash/fnv to avoid heap allocations and interface
// overhead. By performing the math directly on the stack, we achieve
// significantly higher throughput and zero GC pressure in hot paths.
//
// numShards must be a power of 2 for the bitwise AND optimization.
func getShardIndex(key string, numShards int) int {
	var hash uint32 = offset32
	for i := range len(key) {
		hash ^= uint32(key[i])
		hash *= prime32
	}

	// Optimization: 'hash & (N-1)' is mathematically equivalent
	// to 'hash % N' when N is a power of 2.
	return int(hash & uint32(numShards-1))
}

func isPowerOfTwo(n int) bool {
	return n > 0 && (n&(n-1)) == 0
}

func nextPowerOfTwo(n int) int {
	if n <= 0 {
		return 1
	}
	// If already power of two, return it
	if n&(n-1) == 0 {
		return n
	}
	// Find next power of two
	return 1 << (64 - bits.LeadingZeros64(uint64(n)))
}
