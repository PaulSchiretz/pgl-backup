package pathcompression

import (
	"archive/tar"
	"archive/zip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/klauspost/pgzip"
	"github.com/paulschiretz/pgl-backup/pkg/pathcompressionmetrics"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

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

// extractMetricWriter wraps an io.Writer and updates metrics on every write.
type extractMetricWriter struct {
	w       io.Writer
	metrics pathcompressionmetrics.Metrics
}

func (mw *extractMetricWriter) Write(p []byte) (n int, err error) {
	n, err = mw.w.Write(p)
	if n > 0 {
		mw.metrics.AddBytesWritten(int64(n))
	}
	return
}

// extractMetricReader wraps an io.Reader and updates metrics on every read.
type extractMetricReader struct {
	r       io.Reader
	metrics pathcompressionmetrics.Metrics
}

func (mr *extractMetricReader) Read(p []byte) (n int, err error) {
	n, err = mr.r.Read(p)
	if n > 0 {
		mr.metrics.AddBytesRead(int64(n))
	}
	return
}

// extractMetricReaderAt wraps an io.ReaderAt and updates metrics on every read.
type extractMetricReaderAt struct {
	r       io.ReaderAt
	metrics pathcompressionmetrics.Metrics
}

func (mr *extractMetricReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	n, err = mr.r.ReadAt(p, off)
	if n > 0 {
		mr.metrics.AddBytesRead(int64(n))
	}
	return
}

// extractor defines the interface for extracting archives to a target directory.
type extractor interface {
	Extract(ctx context.Context, absArchiveFilePath, absExtractTargetPath string, bufferPool *sync.Pool, metrics pathcompressionmetrics.Metrics, overwrite OverwriteBehavior, modTimeWindow time.Duration) error
}

// newExtractor returns the correct implementation based on the format.
func newExtractor(format Format) (extractor, error) {
	switch format {
	case Zip:
		return &zipExtractor{}, nil
	case TarGz:
		return &tarExtractor{compression: TarGz}, nil
	case TarZst:
		return &tarExtractor{compression: TarZst}, nil
	default:
		return nil, fmt.Errorf("unsupported format: %s", format)
	}
}

type zipExtractor struct{}

func (e *zipExtractor) Extract(ctx context.Context, absArchiveFilePath, absExtractTargetPath string, bufferPool *sync.Pool, metrics pathcompressionmetrics.Metrics, overwrite OverwriteBehavior, modTimeWindow time.Duration) error {
	f, err := os.Open(absArchiveFilePath)
	if err != nil {
		return fmt.Errorf("failed to open zip file: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat zip file: %w", err)
	}

	mr := &extractMetricReaderAt{r: f, metrics: metrics}
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

		metrics.AddEntriesProcessed(1)

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
			metrics.AddBytesWritten(int64(len(linkTarget)))

			shouldWrite, err := handleOverwrite(absTarget, f.Modified, int64(f.UncompressedSize64), overwrite, modTimeWindow)
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

		shouldWrite, err := handleOverwrite(absTarget, f.Modified, int64(f.UncompressedSize64), overwrite, modTimeWindow)
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

		mw := &extractMetricWriter{w: outFile, metrics: metrics}
		bufPtr := bufferPool.Get().(*[]byte)
		_, err = io.CopyBuffer(mw, rc, *bufPtr)
		bufferPool.Put(bufPtr)
		outFile.Close()
		rc.Close()
		if err != nil {
			return err
		}

		os.Chtimes(absTarget, f.Modified, f.Modified)
	}
	return nil
}

type tarExtractor struct {
	compression Format
}

func (e *tarExtractor) Extract(ctx context.Context, absArchiveFilePath, absExtractTargetPath string, bufferPool *sync.Pool, metrics pathcompressionmetrics.Metrics, overwrite OverwriteBehavior, modTimeWindow time.Duration) error {
	f, err := os.Open(absArchiveFilePath)
	if err != nil {
		return err
	}
	defer f.Close()

	mr := &extractMetricReader{r: f, metrics: metrics}
	var r io.Reader = mr
	switch e.compression {
	case TarGz:
		gz, err := pgzip.NewReader(r)
		if err != nil {
			return err
		}
		defer gz.Close()
		r = gz
	case TarZst:
		zstdR, err := zstd.NewReader(r)
		if err != nil {
			return err
		}
		defer zstdR.Close()
		r = zstdR
	}

	tr := tar.NewReader(r)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		metrics.AddEntriesProcessed(1)

		// Security: Zip Slip protection:
		// Ensure that the target path is within the extraction directory.
		// This prevents malicious archives from writing to arbitrary paths via relative paths like "../../etc/passwd".
		relPath := util.NormalizePath(header.Name)
		absTarget := filepath.Join(absExtractTargetPath, relPath)
		if !strings.HasPrefix(absTarget, filepath.Clean(absExtractTargetPath)+string(os.PathSeparator)) {
			return fmt.Errorf("illegal file path in archive: %s", header.Name)
		}

		// Security: Strip SUID and SGID bits to prevent privilege escalation.
		mode := os.FileMode(header.Mode) &^ (os.ModeSetuid | os.ModeSetgid)

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(absTarget, mode); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(absTarget), util.UserWritableDirPerms); err != nil {
				return err
			}

			shouldWrite, err := handleOverwrite(absTarget, header.ModTime, header.Size, overwrite, modTimeWindow)
			if err != nil {
				return err
			}
			if !shouldWrite {
				continue
			}

			// Security: Remove the file if it exists to prevent following a symlink
			// created by a previous entry (Symlink Interception).
			_ = os.Remove(absTarget)

			outFile, err := os.OpenFile(absTarget, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
			if err != nil {
				return err
			}
			mw := &extractMetricWriter{w: outFile, metrics: metrics}
			bufPtr := bufferPool.Get().(*[]byte)
			_, err = io.CopyBuffer(mw, tr, *bufPtr)
			bufferPool.Put(bufPtr)
			outFile.Close()
			if err != nil {
				return err
			}
			os.Chtimes(absTarget, header.AccessTime, header.ModTime)
		case tar.TypeSymlink:
			if err := os.MkdirAll(filepath.Dir(absTarget), util.UserWritableDirPerms); err != nil {
				return err
			}

			shouldWrite, err := handleOverwrite(absTarget, header.ModTime, header.Size, overwrite, modTimeWindow)
			if err != nil {
				return err
			}
			if !shouldWrite {
				continue
			}

			// Security: Remove the file if it exists to prevent following a symlink
			// created by a previous entry (Symlink Interception).
			_ = os.Remove(absTarget)
			if err := os.Symlink(header.Linkname, absTarget); err != nil {
				return err
			}
		}
	}
	return nil
}
