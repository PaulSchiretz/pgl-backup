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
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/flate"
	"github.com/klauspost/compress/zstd"
	"github.com/klauspost/pgzip"
	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/pathcompressionmetrics"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// task holds the mutable state for a single execution.
// This makes the PathCompressionManager itself stateless and safe for concurrent use if needed.
type task struct {
	*PathCompressor
	absTargetBasePath string
	relContentPathKey string
	ctx               context.Context
	toCompress        []metafile.MetafileInfo
	format            Format
	timestampUTC      time.Time
	metrics           pathcompressionmetrics.Metrics
	dryRun            bool
	compressTasksChan chan metafile.MetafileInfo
	compressWg        sync.WaitGroup
}

// execute runs the compression tasks in parallel.
func (t *task) execute() error {

	plog.Info("Compressing backups", "count", len(t.toCompress))

	// Start progress reporting
	t.metrics.StartProgress("Compression progress", 10*time.Second)
	defer func() {
		t.metrics.StopProgress()
		t.metrics.LogSummary("Compression finished")
	}()

	// Start workers
	for i := 0; i < t.numWorkers; i++ {
		t.compressWg.Add(1)
		go t.compressWorker()
	}

	// Start producer
	go t.compressTaskProducer()

	t.compressWg.Wait()

	return nil
}

// compressTaskProducer feeds the eligible backups into the channel for workers.
func (t *task) compressTaskProducer() {
	defer close(t.compressTasksChan)
	for _, b := range t.toCompress {
		select {
		case <-t.ctx.Done():
			plog.Debug("Cancellation received, stopping compression job feeding.")
			return // Stop feeding on cancel.
		case t.compressTasksChan <- b:
		}
	}
}

// compressWorker consumes tasks from the channel and compresses the backups.
func (t *task) compressWorker() {
	defer t.compressWg.Done()

	for b := range t.compressTasksChan {
		// Check for cancellation before processing.
		select {
		case <-t.ctx.Done():
			return
		default:
		}

		if t.dryRun {
			plog.Notice("[DRY RUN] COMPRESS", "path", b.RelPathKey)
			continue
		}

		absPathToCompress := util.DenormalizePath(filepath.Join(t.absTargetBasePath, b.RelPathKey))
		// Check if the backup directory still exists.
		if _, err := os.Stat(absPathToCompress); os.IsNotExist(err) {
			continue
		}

		plog.Notice("COMPRESS", "path", b.RelPathKey)
		if err := t.compressDirectory(absPathToCompress); err != nil {
			// If the error is a cancellation, we should not treat it as a failure.
			if err == context.Canceled {
				return
			}

			// A failure to compress a single backup is logged as a warning but does not
			// stop the overall process.
			t.metrics.AddArchivesFailed(1)
			plog.Warn("Failed to compress directory", "path", b.RelPathKey, "error", err)
		} else {
			t.metrics.AddArchivesCreated(1)
			// On successful archive creation, we first update the metafile.
			b.Metadata.IsCompressed = true
			if writeErr := metafile.Write(absPathToCompress, b.Metadata); writeErr != nil {
				plog.Error("Failed to write updated metafile after compression success. Original content has been preserved.", "path", b.RelPathKey, "error", writeErr)
			} else {
				// Only after the metafile is successfully updated do we remove the original content.
				absContentPathToCompress := util.DenormalizePath(filepath.Join(absPathToCompress, t.relContentPathKey))
				if err := os.RemoveAll(absContentPathToCompress); err != nil {
					plog.Error("Failed to remove original content directory after successful compression. Manual cleanup may be required.", "path", b.RelPathKey, "error", err)
				}
			}
		}
		plog.Notice("COMPRESSED", "path", b.RelPathKey)
	}
}

// compressDirectory creates an archive of the directory's contents,
// then deletes the original files, leaving only the archive and essential metadata.
// It does NOT modify metadata or delete the original content, leaving that to the caller.
func (t *task) compressDirectory(dirPath string) error {
	contentDir := filepath.Join(dirPath, t.relContentPathKey)
	// The archive is named after its parent backup directory (e.g., "PGL_Backup_2023-10-27...zip").
	// This makes the archive file easily identifiable and self-describing even if it's
	// moved out of its original context.
	archiveFileName := filepath.Base(dirPath) + "." + t.format.String()
	finalArchivePath := filepath.Join(dirPath, archiveFileName)

	// Cleanup stale tmp files from previous crashed runs
	// We do this first to ensure we aren't wasting disk space or
	// potentially confusing os.CreateTemp.
	t.cleanupStaleTempFiles(dirPath)

	// Safety check: Handle the "Half-Finished" state
	// If the content directory is gone but the archive exists, just update metadata.
	if _, err := os.Stat(contentDir); os.IsNotExist(err) {
		// If archive exists but contentDir doesn't, maybe we crashed last time?
		if _, errArch := os.Stat(finalArchivePath); errArch == nil {
			plog.Warn("Content directory missing but archive exists. Repairing metadata.", "path", dirPath)
			// Return nil to signal to the worker that the file operation is "complete" and it can proceed to update the metafile.
			return nil
		}
	}

	// 1. Create the archive in a temporary file.
	tempArchivePath, err := t.createArchive(contentDir)
	if err != nil {
		if err == context.Canceled {
			plog.Debug("Compression was canceled during archive creation", "path", dirPath)
			return err
		}
		return fmt.Errorf("failed to create archive: %w", err)
	}
	defer os.Remove(tempArchivePath)

	// Capture compressed size
	if info, err := os.Stat(tempArchivePath); err == nil {
		t.metrics.AddCompressedBytes(info.Size())
	}

	// 2. Move the completed temporary archive into the parent backup directory.

	// os.Rename is atomic on POSIX and uses MoveFileEx with MOVEFILE_REPLACE_EXISTING on Windows.
	if err := os.Rename(tempArchivePath, finalArchivePath); err != nil {
		return fmt.Errorf("failed to rename temporary archive to final destination: %w", err)
	}
	return nil
}

// createArchive provides a generic, robust, and atomic way to compress a directory.
// It uses a temporary file and an atomic rename to prevent partial/corrupt archives.
func (t *task) createArchive(sourceDir string) (tempPath string, err error) {
	// 1. Create a temporary file in the destination directory to ensure atomic rename is possible.
	tempFile, err := os.CreateTemp(filepath.Dir(sourceDir), "pgl-backup-*."+t.format.String()+".tmp")
	if err != nil {
		return "", fmt.Errorf("failed to create temporary archive file: %w", err)
	}
	tempPath = tempFile.Name()

	// 1. Get a Buffer from the pool (The "Middleman")
	// This sits between the compressor and the disk to reduce syscalls.
	// We reuse buffers to reduce GC pressure.
	bufWriter := t.ioWriterPool.Get().(*bufio.Writer)
	bufWriter.Reset(tempFile)
	defer func() {
		bufWriter.Reset(io.Discard)
		t.ioWriterPool.Put(bufWriter)
	}()

	// 2. Set up the appropriate archive writer based on the format.
	var archiver archiveWriter
	switch t.format {
	case Zip:
		zipWriter := zip.NewWriter(bufWriter)
		zipWriter.RegisterCompressor(zip.Deflate, func(out io.Writer) (io.WriteCloser, error) {
			return flate.NewWriter(out, flate.DefaultCompression)
		})
		archiver = &zipArchiveWriter{zipWriter: zipWriter}
	case TarGz:
		gzipWriter := pgzip.NewWriter(bufWriter)
		tarWriter := tar.NewWriter(gzipWriter)
		archiver = &tarArchiveWriter{tarWriter: tarWriter, compressedWriter: gzipWriter}
	case TarZst:
		zstdWriter, err := zstd.NewWriter(bufWriter)
		if err != nil {
			return "", fmt.Errorf("failed to create zstd writer: %w", err)
		}
		tarWriter := tar.NewWriter(zstdWriter)
		archiver = &tarArchiveWriter{tarWriter: tarWriter, compressedWriter: zstdWriter}
	default:
		// This should be caught earlier, but we handle it here for safety.
		tempFile.Close()
		os.Remove(tempPath)
		return "", fmt.Errorf("unsupported compression format: %s", t.format)
	}

	// Defer a function to handle cleanup. It checks the named return `err`.
	// If an error has occurred at any point, it cleans up the temp file.
	defer func() {
		// 1. Close archiver first (flushes its internal state to the buffer)
		if closeErr := archiver.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("archiver close failed: %w", closeErr)
		}

		// 2. Flush the buffer (ensures everything is in the OS file buffer)
		if flushErr := bufWriter.Flush(); flushErr != nil && err == nil {
			err = fmt.Errorf("buffer flush failed: %w", flushErr)
		}

		// 3. Close the underlying file (actually writes to disk)
		if fileErr := tempFile.Close(); fileErr != nil && err == nil {
			err = fmt.Errorf("temp file close failed: %w", fileErr)
		}

		// 4. Cleanup on failure
		if err != nil {
			os.Remove(tempPath)
		}
	}()

	// Walk the directory and add files to the archive.
	walkErr := filepath.WalkDir(sourceDir, func(absSrcPath string, d os.DirEntry, walkErr error) error {
		select {
		case <-t.ctx.Done():
			return context.Canceled
		default:
		}

		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() || absSrcPath == tempPath {
			return nil
		}

		// We need FileInfo for the archive headers (permissions, timestamps).
		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("failed to get file info for %s: %w", absSrcPath, err)
		}

		// Calculate the relative path for the archive entry.
		relPath, err := filepath.Rel(sourceDir, absSrcPath)
		if err != nil {
			return fmt.Errorf("failed to get relative path for %s: %w", absSrcPath, err)
		}
		// Normalize to forward slashes for archive compatibility.
		relPath = util.NormalizePath(relPath)

		plog.Notice("ADD", "source", sourceDir, "file", relPath)
		t.metrics.AddEntriesProcessed(1)
		t.metrics.AddOriginalBytes(info.Size())

		// Handle Symlinks
		if info.Mode()&os.ModeSymlink != 0 {
			return archiver.AddSymlink(absSrcPath, relPath, info)
		}

		// Handle Regular Files
		// Get a buffer from the pool for the copy operation.
		// Use a closure to ensure the buffer is returned to the pool
		// immediately after the file is processed, NOT after the walk ends.
		return func() error {
			bufPtr := t.ioBufferPool.Get().(*[]byte)
			defer t.ioBufferPool.Put(bufPtr)
			return archiver.AddFile(absSrcPath, relPath, info, *bufPtr)
		}()
	})

	if walkErr != nil {
		err = walkErr // Assign to the named return variable so the defer block sees it.
		return "", err
	}

	// On success, return the path to the completed temporary file. The defer will not remove it because `err` is nil.
	return tempPath, nil
}

func (t *task) cleanupStaleTempFiles(dirPath string) {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return
	}
	for _, entry := range entries {
		// Look for our specific temp pattern: pgl-backup-*.tmp
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "pgl-backup-") && strings.HasSuffix(entry.Name(), ".tmp") {
			plog.Debug("Removing stale temporary archive", "file", entry.Name())
			os.Remove(filepath.Join(dirPath, entry.Name()))
		}
	}
}
