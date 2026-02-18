package pathcompression

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

// extractor defines the interface for extracting archives to a target directory.
type extractor interface {
	Extract(ctx context.Context, absArchiveFilePath, absExtractTargetPath string) error
}

// handleOverwrite checks if a file should be written to absTargetPath based on the overwrite behavior.
// It returns true if the file should be written.
// As a side effect, it removes the existing file/symlink at absTargetPath if overwriting is decided.
func handleOverwrite(absTargetPath string, archiveFileModTime time.Time, archiveFileSize int64, overwrite OverwriteBehavior, modTimeWindow time.Duration) (bool, error) {
	destInfo, err := os.Lstat(absTargetPath)
	if os.IsNotExist(err) {
		return true, nil // Path is clear.
	}
	if err != nil {
		return false, fmt.Errorf("failed to stat destination path %s: %w", absTargetPath, err)
	}

	if destInfo.IsDir() {
		return false, fmt.Errorf("cannot overwrite directory with a file: %s", absTargetPath)
	}

	// Helper to truncate time based on window
	truncate := func(t time.Time) time.Time {
		if modTimeWindow > 0 {
			return t.Truncate(modTimeWindow)
		}
		return t
	}

	// Default to 'never' if not specified
	if overwrite == "" {
		overwrite = OverwriteNever
	}

	switch overwrite {
	case OverwriteNever:
		plog.Debug("Skipping existing file (overwrite=never)", "path", absTargetPath)
		return false, nil
	case OverwriteIfNewer:
		if !truncate(archiveFileModTime).After(truncate(destInfo.ModTime())) {
			plog.Debug("Skipping up-to-date file (overwrite=if-newer)", "path", absTargetPath)
			return false, nil
		}
	case OverwriteUpdate:
		if destInfo.Size() == archiveFileSize && truncate(destInfo.ModTime()).Equal(truncate(archiveFileModTime)) {
			plog.Debug("Skipping up-to-date file (overwrite=update)", "path", absTargetPath)
			return false, nil
		}
	case OverwriteAlways:
		// Proceed to overwrite.
	default:
		return false, fmt.Errorf("unsupported overwrite behavior: %s", overwrite)
	}

	// If we reach here, we overwrite.
	// Security: Remove existing file/symlink before creating the new one.
	if err := os.Remove(absTargetPath); err != nil {
		return false, fmt.Errorf("failed to remove existing file for overwrite at %s: %w", absTargetPath, err)
	}
	return true, nil
}

// extractMetricReader wraps an io.Reader and updates metrics on every read.
type extractMetricReader struct {
	r       io.Reader
	metrics Metrics
}

func (mr *extractMetricReader) Read(p []byte) (n int, err error) {
	n, err = mr.r.Read(p)
	if n > 0 {
		mr.metrics.AddBytesRead(int64(n))
	}
	return
}

func (mr *extractMetricReader) Reset(r io.Reader) {
	mr.r = r
}

// extractMetricReaderAt wraps an io.ReaderAt and updates metrics on every read.
type extractMetricReaderAt struct {
	r       io.ReaderAt
	metrics Metrics
}

func (mr *extractMetricReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	n, err = mr.r.ReadAt(p, off)
	if n > 0 {
		mr.metrics.AddBytesRead(int64(n))
	}
	return
}
