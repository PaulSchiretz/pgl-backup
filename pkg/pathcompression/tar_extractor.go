package pathcompression

import (
	"archive/tar"
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
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

type tarExtractor struct {
	dryRun        bool
	format        Format
	bufferPool    *sync.Pool
	metrics       Metrics
	overwrite     OverwriteBehavior
	modTimeWindow time.Duration
}

func newTarExtractor(dryRun bool, format Format, bufferPool *sync.Pool, metrics Metrics, overwrite OverwriteBehavior, modTimeWindow time.Duration) *tarExtractor {
	return &tarExtractor{
		dryRun:        dryRun,
		format:        format,
		bufferPool:    bufferPool,
		metrics:       metrics,
		overwrite:     overwrite,
		modTimeWindow: modTimeWindow,
	}
}

func (e *tarExtractor) Extract(ctx context.Context, absArchiveFilePath, absExtractTargetPath string) error {

	if e.dryRun {
		plog.Notice("[DRY RUN] EXTRACT", "source", absArchiveFilePath, "target", absExtractTargetPath)
		return nil
	}

	plog.Notice("EXTRACT", "source", absArchiveFilePath, "target", absExtractTargetPath)

	f, err := os.Open(absArchiveFilePath)
	if err != nil {
		return err
	}
	defer f.Close()

	mr := &extractMetricReader{r: f, metrics: e.metrics}
	var r io.Reader = mr
	switch e.format {
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

		e.metrics.AddEntriesProcessed(1)

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

			shouldWrite, err := handleOverwrite(absTarget, header.ModTime, header.Size, e.overwrite, e.modTimeWindow)
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
			bufPtr := e.bufferPool.Get().(*[]byte)
			n, err := io.CopyBuffer(outFile, tr, *bufPtr)
			e.bufferPool.Put(bufPtr)
			e.metrics.AddBytesWritten(n)
			outFile.Close()
			if err != nil {
				return err
			}
			os.Chtimes(absTarget, header.AccessTime, header.ModTime)
		case tar.TypeSymlink:
			if err := os.MkdirAll(filepath.Dir(absTarget), util.UserWritableDirPerms); err != nil {
				return err
			}

			shouldWrite, err := handleOverwrite(absTarget, header.ModTime, header.Size, e.overwrite, e.modTimeWindow)
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
