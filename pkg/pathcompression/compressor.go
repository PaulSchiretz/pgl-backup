package pathcompression

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/paulschiretz/pgl-backup/pkg/pathcompressionmetrics"
)

// compressor defines the interface for compressing a directory into an archive file.
type compressor interface {
	Compress(ctx context.Context, sourceDir, archivePath string) error
}

// compressMetricWriter wraps an io.Writer and updates metrics on every write.
type compressMetricWriter struct {
	w       io.Writer
	metrics pathcompressionmetrics.Metrics
}

func (mw *compressMetricWriter) Write(p []byte) (n int, err error) {
	n, err = mw.w.Write(p)
	if n > 0 {
		mw.metrics.AddBytesWritten(int64(n))
	}
	return
}

func (mw *compressMetricWriter) Reset(w io.Writer) {
	mw.w = w
}

// secureFileOpen verifies that the file at path is the same one we expected(TOCTOU check).
// Ensure the file we opened is the same one we discovered in the walk.
// This prevents attacks where a file is swapped for a symlink after discovery.
func secureFileOpen(absFilePath string, expected os.FileInfo) (*os.File, error) {
	f, err := os.Open(absFilePath)
	if err != nil {
		return nil, err
	}

	openedInfo, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to stat opened file: %w", err)
	}

	// 1. Check if it's the same physical file (Inode check)
	if !os.SameFile(expected, openedInfo) {
		f.Close()
		return nil, fmt.Errorf("file changed during backup (TOCTOU): %s", absFilePath)
	}

	// 2. Check if the size changed (Tar Header Integrity check)
	// If you already calculated the Tar header based on 'expected',
	// a size change will corrupt the archive.
	if openedInfo.Size() != expected.Size() {
		f.Close()
		return nil, fmt.Errorf("file size changed during backup: %s", absFilePath)
	}

	return f, nil
}
