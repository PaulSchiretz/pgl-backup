package pathsync

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// lstatInfo is an internal, memory-efficient representation of file metadata.
// Storing this directly instead of the os.FileInfo interface avoids a pointer
// lookup and reduces GC pressure, as the data is inlined in the parent struct.
type lstatInfo struct {
	ModTime   int64       // Unix Nano. Stored as int64 to avoid GC overhead of time.Time's internal pointer.
	Size      int64       // Size in bytes.
	Mode      os.FileMode // File mode bits.
	IsDir     bool        // True if the path is a directory.
	IsRegular bool        // True if the path is a regular file.
	IsSymlink bool        // True if the path is a symlink.
}

// LStatSnapshotStore manages a thread-safe, lazy-loading cache of filesystem metadata.
// It is designed to optimize the "sync" phase by caching the results of ReadDir calls
// on destination directories. This allows workers to check for file existence and
// metadata (size, modtime) without performing individual Lstat syscalls for every file.
//
// We use a sync.Map because the workload is "Write-Once, Read-Many":
//   - No Overwrites: A directory's metadata snapshot is never updated once stored.
//   - No Deletions: Entries persist for the duration of the Sync phase.
//   - High Read Efficiency: sync.Map allows lock-free concurrent reads once the "dirty"
//     map is promoted to "read," which is ideal for folders containing many files.
//
// CRITICAL: The inner map[string]lstatInfo MUST be treated as READ-ONLY after storage.
type LStatSnapshotStore struct {
	// data stores the directory snapshots.
	// Key:   Relative path of the parent directory (string).
	// Value: map[string]lstatInfo (filename -> metadata).
	//
	// The inner map is treated as immutable once stored. This allows lock-free reads
	// by multiple workers processing files within the same directory.
	data sync.Map
}

func newLStatSnapshotStore() *LStatSnapshotStore {
	return &LStatSnapshotStore{}
}

// LoadLstatInfo retrieves lstatInfo for a path from the internal cache.
// It returns three values:
//   - info: The metadata if found.
//   - found: True if the specific file entry exists in the cache.
//   - parentFound: True if the parent directory's snapshot exists in the cache.
//
// This distinction allows the caller to differentiate between "cache miss" (parent not cached,
// need to hit disk) and "definitively does not exist" (parent cached, file not in it).
func (s *LStatSnapshotStore) LoadLstatInfo(relPathKey string) (info lstatInfo, found bool, parentFound bool) {
	// CRITICAL: Re-normalize the parent key. after `filepath.Dir` as it can return a path with
	// OS-specific separators (e.g., '\' on Windows)
	parentKey := util.NormalizePath(filepath.Dir(relPathKey))
	entryKey := filepath.Base(relPathKey)

	if val, ok := s.data.Load(parentKey); ok {
		snapshotData := val.(map[string]lstatInfo)
		if i, exists := snapshotData[entryKey]; exists {
			return i, true, true // Parent found, entry exists
		}
		return lstatInfo{}, false, true // Parent found, entry definitively DOES NOT exist
	}
	return lstatInfo{}, false, false // Parent not found
}

// StoreSnapshot saves a complete directory listing snapshot into the cache.
// The snapshot map should contain all entries in the directory at 'snapshotRelPathKey'.
// Once stored, the map must not be modified.
func (s *LStatSnapshotStore) StoreSnapshot(snapshotRelPathKey string, snapshot map[string]lstatInfo) {
	s.data.Store(snapshotRelPathKey, snapshot)
}
