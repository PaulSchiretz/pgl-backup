package pathcompression

import (
	"archive/tar"
	"archive/zip"
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/klauspost/compress/flate"
	"github.com/klauspost/compress/zstd"
	"github.com/klauspost/pgzip"
	"github.com/paulschiretz/pgl-backup/pkg/pathcompressionmetrics"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// compressor defines the interface for compressing a directory into an archive file.
type compressor interface {
	Compress(ctx context.Context, sourceDir, archivePath string, writerPool, bufferPool *sync.Pool, metrics pathcompressionmetrics.Metrics) error
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

// compressMetricReader wraps an io.Reader and updates metrics on every read.
type compressMetricReader struct {
	r       io.Reader
	metrics pathcompressionmetrics.Metrics
}

func (mr *compressMetricReader) Read(p []byte) (n int, err error) {
	n, err = mr.r.Read(p)
	if n > 0 {
		mr.metrics.AddBytesRead(int64(n))
	}
	return
}

// newCompressor returns the correct implementation based on the format.
func newCompressor(format Format, level Level) (compressor, error) {
	switch format {
	case Zip:
		return &zipCompressor{level: level}, nil
	case TarGz:
		return &tarCompressor{compression: TarGz, level: level}, nil
	case TarZst:
		return &tarCompressor{compression: TarZst, level: level}, nil
	default:
		return nil, fmt.Errorf("unsupported format: %s", format)
	}
}

type zipCompressor struct {
	level Level
}

func (c *zipCompressor) Compress(ctx context.Context, absSourcePath, absArchiveFilePath string, writerPool, bufferPool *sync.Pool, metrics pathcompressionmetrics.Metrics) (retErr error) {
	// 1. Create Temp File
	// We create it in the same directory as the target to ensure atomic rename.
	targetF, err := os.CreateTemp(filepath.Dir(absArchiveFilePath), "pgl-backup-*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temp archive: %w", err)
	}
	tempName := targetF.Name()

	// Ensure cleanup on error
	defer func() {
		if retErr != nil {
			targetF.Close()     // Ensure closed
			os.Remove(tempName) // Delete temp file
		}
	}()

	// 2. Write Archive Content
	if err := c.writeArchive(ctx, absSourcePath, targetF, writerPool, bufferPool, metrics); err != nil {
		return err
	}

	// 3. Close explicitly to flush to disk before rename
	if err := targetF.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	// 4. Atomic Rename
	if err := os.Rename(tempName, absArchiveFilePath); err != nil {
		return fmt.Errorf("failed to rename temp archive to final path: %w", err)
	}

	return nil
}

func (c *zipCompressor) writeArchive(ctx context.Context, absSourcePath string, targetF *os.File, writerPool, bufferPool *sync.Pool, metrics pathcompressionmetrics.Metrics) (retErr error) {

	// Get a Buffer from the pool
	mw := &compressMetricWriter{w: targetF, metrics: metrics}
	bufWriter := writerPool.Get().(*bufio.Writer)
	bufWriter.Reset(mw)
	defer func() {
		bufWriter.Reset(io.Discard)
		writerPool.Put(bufWriter)
	}()

	zipWriter := zip.NewWriter(bufWriter)
	zipWriter.RegisterCompressor(zip.Deflate, func(out io.Writer) (io.WriteCloser, error) {
		var lvl int
		switch c.level {
		case Fastest:
			lvl = flate.BestSpeed
		case Better:
			lvl = 6 // Good balance
		case Best:
			lvl = flate.BestCompression
		default:
			lvl = flate.DefaultCompression
		}
		return flate.NewWriter(out, lvl)
	})

	// Robust cleanup
	defer func() {
		if err := zipWriter.Close(); err != nil && retErr == nil {
			retErr = fmt.Errorf("zip writer close failed: %w", err)
		}
		if err := bufWriter.Flush(); err != nil && retErr == nil {
			retErr = fmt.Errorf("buffer flush failed: %w", err)
		}
	}()

	return walkAndCompress(ctx, absSourcePath, bufferPool, metrics, func(absSrcPath, relPath string, info os.FileInfo, buf []byte) error {
		// Add File Logic
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return fmt.Errorf("failed to create zip header for %s: %w", relPath, err)
		}
		header.Name = relPath
		header.Method = zip.Deflate

		writer, err := zipWriter.CreateHeader(header)
		if err != nil {
			return fmt.Errorf("failed to create entry for %s: %w", relPath, err)
		}

		fileToZip, err := os.Open(absSrcPath)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %w", absSrcPath, err)
		}
		defer fileToZip.Close()

		// Security: TOCTOU check
		// Ensure the file we opened is the same one we discovered in the walk.
		// This prevents attacks where a file is swapped for a symlink after discovery.
		if openedInfo, err := fileToZip.Stat(); err != nil {
			return fmt.Errorf("failed to stat opened file %s: %w", absSrcPath, err)
		} else if !os.SameFile(info, openedInfo) {
			return fmt.Errorf("file changed during backup (possible security attack): %s", absSrcPath)
		}

		mr := &compressMetricReader{r: fileToZip, metrics: metrics}
		_, err = io.CopyBuffer(writer, mr, buf)
		return err
	}, func(absSrcPath, relPath string, info os.FileInfo) error {
		// Add Symlink Logic
		target, err := os.Readlink(absSrcPath)
		if err != nil {
			return fmt.Errorf("failed to read link target for %s: %w", absSrcPath, err)
		}
		metrics.AddBytesRead(int64(len(target)))

		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return fmt.Errorf("failed to create zip header for %s: %w", relPath, err)
		}
		header.Name = relPath
		header.Method = zip.Store // Symlinks are stored, not compressed

		writer, err := zipWriter.CreateHeader(header)
		if err != nil {
			return fmt.Errorf("failed to create entry for %s: %w", relPath, err)
		}
		_, err = writer.Write([]byte(target))
		return err
	})
}

type tarCompressor struct {
	compression Format
	level       Level
}

func (c *tarCompressor) Compress(ctx context.Context, absSourcePath, absArchiveFilePath string, writerPool, bufferPool *sync.Pool, metrics pathcompressionmetrics.Metrics) (retErr error) {
	// 1. Create Temp File
	targetF, err := os.CreateTemp(filepath.Dir(absArchiveFilePath), "pgl-backup-*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temp archive: %w", err)
	}
	tempName := targetF.Name()

	// Ensure cleanup on error
	defer func() {
		if retErr != nil {
			targetF.Close()
			os.Remove(tempName)
		}
	}()

	// 2. Write Archive Content
	if err := c.writeArchive(ctx, absSourcePath, targetF, writerPool, bufferPool, metrics); err != nil {
		return err
	}

	// 3. Close explicitly
	if err := targetF.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	// 4. Atomic Rename
	if err := os.Rename(tempName, absArchiveFilePath); err != nil {
		return fmt.Errorf("failed to rename temp archive to final path: %w", err)
	}

	return nil
}

func (c *tarCompressor) writeArchive(ctx context.Context, absSourcePath string, targetF *os.File, writerPool, bufferPool *sync.Pool, metrics pathcompressionmetrics.Metrics) (retErr error) {

	mw := &compressMetricWriter{w: targetF, metrics: metrics}
	bufWriter := writerPool.Get().(*bufio.Writer)
	bufWriter.Reset(mw)
	defer func() {
		bufWriter.Reset(io.Discard)
		writerPool.Put(bufWriter)
	}()

	var compressedWriter io.WriteCloser
	if c.compression == TarZst {
		opts := []zstd.EOption{}
		var encoderLevel zstd.EncoderLevel
		switch c.level {
		case Fastest:
			encoderLevel = zstd.SpeedFastest
		case Better:
			encoderLevel = zstd.SpeedBetterCompression
		case Best:
			encoderLevel = zstd.SpeedBestCompression
		default:
			encoderLevel = zstd.SpeedDefault
		}
		opts = append(opts, zstd.WithEncoderLevel(encoderLevel))

		zstdWriter, err := zstd.NewWriter(bufWriter, opts...)
		if err != nil {
			return fmt.Errorf("failed to create zstd writer: %w", err)
		}
		compressedWriter = zstdWriter
	} else {
		var lvl int
		switch c.level {
		case Fastest:
			lvl = pgzip.BestSpeed
		case Better:
			lvl = 6 // Good balance
		case Best:
			lvl = pgzip.BestCompression
		default:
			lvl = pgzip.DefaultCompression
		}
		pgzipWriter, err := pgzip.NewWriterLevel(bufWriter, lvl)
		if err != nil {
			return fmt.Errorf("failed to create gzip writer: %w", err)
		}
		compressedWriter = pgzipWriter
	}

	tarWriter := tar.NewWriter(compressedWriter)

	// Robust cleanup
	defer func() {
		if err := tarWriter.Close(); err != nil && retErr == nil {
			retErr = fmt.Errorf("tar writer close failed: %w", err)
		}
		if err := compressedWriter.Close(); err != nil && retErr == nil {
			retErr = fmt.Errorf("compressed writer close failed: %w", err)
		}
		if err := bufWriter.Flush(); err != nil && retErr == nil {
			retErr = fmt.Errorf("buffer flush failed: %w", err)
		}
	}()

	return walkAndCompress(ctx, absSourcePath, bufferPool, metrics, func(absSrcPath, relPath string, info os.FileInfo, buf []byte) error {
		// Add File Logic
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return fmt.Errorf("failed to create tar header for %s: %w", absSrcPath, err)
		}
		header.Name = relPath

		if err := tarWriter.WriteHeader(header); err != nil {
			return fmt.Errorf("failed to write tar header for %s: %w", relPath, err)
		}

		fileToTar, err := os.Open(absSrcPath)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %w", absSrcPath, err)
		}
		defer fileToTar.Close()

		// Security: TOCTOU check
		// Ensure the file we opened is the same one we discovered in the walk.
		// This prevents attacks where a file is swapped for a symlink after discovery.
		if openedInfo, err := fileToTar.Stat(); err != nil {
			return fmt.Errorf("failed to stat opened file %s: %w", absSrcPath, err)
		} else if !os.SameFile(info, openedInfo) {
			return fmt.Errorf("file changed during backup (possible security attack): %s", absSrcPath)
		}

		mr := &compressMetricReader{r: fileToTar, metrics: metrics}
		_, err = io.CopyBuffer(tarWriter, mr, buf)
		return err
	}, func(absSrcPath, relPath string, info os.FileInfo) error {
		// Add Symlink Logic
		target, err := os.Readlink(absSrcPath)
		if err != nil {
			return fmt.Errorf("failed to read link target for %s: %w", absSrcPath, err)
		}
		metrics.AddBytesRead(int64(len(target)))

		header, err := tar.FileInfoHeader(info, target)
		if err != nil {
			return fmt.Errorf("failed to create tar header for %s: %w", absSrcPath, err)
		}
		header.Name = relPath

		return tarWriter.WriteHeader(header)
	})
}

// walkAndCompress is a helper to reduce code duplication between zip and tar walkers.
func walkAndCompress(
	ctx context.Context,
	absSourcePath string,
	bufferPool *sync.Pool,
	metrics pathcompressionmetrics.Metrics,
	addFile func(absSourcePathEntry, relTargetPathKey string, info os.FileInfo, buf []byte) error,
	addSymlink func(absSourcePathEntry, relTargetPathKey string, info os.FileInfo) error,
) error {
	return filepath.WalkDir(absSourcePath, func(absSourcePathEntry string, d os.DirEntry, walkErr error) error {
		select {
		case <-ctx.Done():
			return context.Canceled
		default:
		}

		if walkErr != nil {
			return walkErr
		}

		if d.IsDir() {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("failed to get file info for %s: %w", absSourcePathEntry, err)
		}

		relTargetPathKey, err := filepath.Rel(absSourcePath, absSourcePathEntry)
		if err != nil {
			return fmt.Errorf("failed to get relative path for %s: %w", absSourcePathEntry, err)
		}
		relTargetPathKey = util.NormalizePath(relTargetPathKey)

		plog.Notice("ADD", "source", absSourcePath, "file", relTargetPathKey)
		metrics.AddEntriesProcessed(1)

		if info.Mode()&os.ModeSymlink != 0 {
			return addSymlink(absSourcePathEntry, relTargetPathKey, info)
		}

		// Handle Regular Files
		return func() error {
			bufPtr := bufferPool.Get().(*[]byte)
			defer bufferPool.Put(bufPtr)
			return addFile(absSourcePathEntry, relTargetPathKey, info, *bufPtr)
		}()
	})
}
