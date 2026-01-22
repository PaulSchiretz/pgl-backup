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

	"github.com/klauspost/compress/zstd"
	"github.com/klauspost/pgzip"
	"github.com/paulschiretz/pgl-backup/pkg/pathcompressionmetrics"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// extractor defines the interface for extracting archives to a target directory.
type extractor interface {
	Extract(ctx context.Context, absArchiveFilePath, absExtractTargetPath string, bufferPool *sync.Pool, metrics pathcompressionmetrics.Metrics) error
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

func (e *zipExtractor) Extract(ctx context.Context, absArchiveFilePath, absExtractTargetPath string, bufferPool *sync.Pool, metrics pathcompressionmetrics.Metrics) error {
	r, err := zip.OpenReader(absArchiveFilePath)
	if err != nil {
		return fmt.Errorf("failed to open zip archive: %w", err)
	}
	defer r.Close()

	for _, f := range r.File {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		metrics.AddEntriesProcessed(1)
		metrics.AddOriginalBytes(int64(f.UncompressedSize64))

		// Zip Slip protection:
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
			_ = os.Remove(absTarget) // Remove existing if any
			if err := os.Symlink(string(linkTarget), absTarget); err != nil {
				return err
			}
			continue
		}

		// Handle Regular Files
		// Security: Remove the file if it exists to prevent following a symlink
		// created by a previous entry (Symlink Interception).
		_ = os.Remove(absTarget)

		outFile, err := os.OpenFile(absTarget, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
		if err != nil {
			rc.Close()
			return err
		}

		bufPtr := bufferPool.Get().(*[]byte)
		_, err = io.CopyBuffer(outFile, rc, *bufPtr)
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

func (e *tarExtractor) Extract(ctx context.Context, absArchiveFilePath, absExtractTargetPath string, bufferPool *sync.Pool, metrics pathcompressionmetrics.Metrics) error {
	f, err := os.Open(absArchiveFilePath)
	if err != nil {
		return err
	}
	defer f.Close()

	var r io.Reader = f
	switch e.compression {
	case TarGz:
		gz, err := pgzip.NewReader(f)
		if err != nil {
			return err
		}
		defer gz.Close()
		r = gz
	case TarZst:
		zstdR, err := zstd.NewReader(f)
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
		metrics.AddOriginalBytes(header.Size)

		// Zip Slip protection:
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
			// Security: Remove the file if it exists to prevent following a symlink
			// created by a previous entry (Symlink Interception).
			_ = os.Remove(absTarget)

			outFile, err := os.OpenFile(absTarget, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
			if err != nil {
				return err
			}
			bufPtr := bufferPool.Get().(*[]byte)
			_, err = io.CopyBuffer(outFile, tr, *bufPtr)
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
			_ = os.Remove(absTarget)
			if err := os.Symlink(header.Linkname, absTarget); err != nil {
				return err
			}
		}
	}
	return nil
}
