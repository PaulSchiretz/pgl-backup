package pathsync

import (
	"os"
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

func makeLstatInfo(fi os.FileInfo) lstatInfo {
	return lstatInfo{
		ModTime:   fi.ModTime().UnixNano(),
		Size:      fi.Size(),
		Mode:      fi.Mode(),
		IsDir:     fi.IsDir(),
		IsRegular: fi.Mode().IsRegular(),
		IsSymlink: fi.Mode()&os.ModeSymlink != 0,
	}
}
