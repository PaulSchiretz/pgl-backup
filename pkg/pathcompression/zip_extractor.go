package pathcompression

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/util"
)

type zipExtractor struct {
	format        Format
	bufferPool    *sync.Pool
	metrics       Metrics
	overwrite     OverwriteBehavior
	modTimeWindow time.Duration
}

func newZipExtractor(format Format, bufferPool *sync.Pool, metrics Metrics, overwrite OverwriteBehavior, modTimeWindow time.Duration) *zipExtractor {
	return &zipExtractor{
		format:        Zip,
		bufferPool:    bufferPool,
		metrics:       metrics,
		overwrite:     overwrite,
		modTimeWindow: modTimeWindow,
	}
}

func (e *zipExtractor) Extract(ctx context.Context, absArchiveFilePath, absExtractTargetPath string) error {
	f, err := os.Open(absArchiveFilePath)
	if err != nil {
		return fmt.Errorf("failed to open zip file: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat zip file: %w", err)
	}

	mr := &extractMetricReaderAt{r: f, metrics: e.metrics}
	r, err := zip.NewReader(mr, info.Size())
	if err != nil {
		return fmt.Errorf("failed to create zip reader: %w", err)
	}

	for _, f := range r.File {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		e.metrics.AddEntriesProcessed(1)

		// Security: Zip Slip protection:
		// Ensure that the target path is within the extraction directory.
		// This prevents malicious archives from writing to arbitrary paths via relative paths like "../../etc/passwd".
		relPath := util.NormalizePath(f.Name)
		absTarget := filepath.Join(absExtractTargetPath, relPath)
		if !strings.HasPrefix(absTarget, filepath.Clean(absExtractTargetPath)+string(os.PathSeparator)) {
			return fmt.Errorf("illegal file path in archive: %s", f.Name)
		}

		// Security: Strip SUID and SGID bits to prevent privilege escalation.
		mode := f.Mode() &^ (os.ModeSetuid | os.ModeSetgid)

		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(absTarget, mode); err != nil {
				return err
			}
			continue
		}

		if err := os.MkdirAll(filepath.Dir(absTarget), util.UserWritableDirPerms); err != nil {
			return err
		}

		rc, err := f.Open()
		if err != nil {
			return err
		}

		// Handle Symlinks
		if f.Mode()&os.ModeSymlink != 0 {
			linkTarget, err := io.ReadAll(rc)
			rc.Close()
			if err != nil {
				return err
			}
			e.metrics.AddBytesWritten(int64(len(linkTarget)))

			shouldWrite, err := handleOverwrite(absTarget, f.Modified, int64(f.UncompressedSize64), e.overwrite, e.modTimeWindow)
			if err != nil {
				return err
			}
			if !shouldWrite {
				continue
			}

			// Security: Remove the file if it exists to prevent following a symlink
			// created by a previous entry (Symlink Interception).
			_ = os.Remove(absTarget)
			if err := os.Symlink(string(linkTarget), absTarget); err != nil {
				return err
			}
			continue
		}

		// Handle Regular Files

		shouldWrite, err := handleOverwrite(absTarget, f.Modified, int64(f.UncompressedSize64), e.overwrite, e.modTimeWindow)
		if err != nil {
			rc.Close()
			return err
		}
		if !shouldWrite {
			rc.Close()
			continue
		}

		// Security: Remove the file if it exists to prevent following a symlink
		// created by a previous entry (Symlink Interception).
		_ = os.Remove(absTarget)

		outFile, err := os.OpenFile(absTarget, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
		if err != nil {
			rc.Close()
			return err
		}

		bufPtr := e.bufferPool.Get().(*[]byte)
		n, err := io.CopyBuffer(outFile, rc, *bufPtr)
		e.bufferPool.Put(bufPtr)
		e.metrics.AddBytesWritten(n)
		outFile.Close()
		rc.Close()
		if err != nil {
			return err
		}

		os.Chtimes(absTarget, f.Modified, f.Modified)
	}
	return nil
}
