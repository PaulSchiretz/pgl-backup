// --- ARCHITECTURAL OVERVIEW: Compression Strategy ---
//
// The compression logic follows a "Compress Once / Fail-Forward" strategy.
//
// Instead of scanning the entire history for uncompressed backups on every run,
// the engine only attempts to compress the specific backup created during the
// current run (the new incremental archive or the new snapshot).
//
// Rationale:
//  1. Robustness: If a specific backup contains corrupt data that causes the
//     compression process to crash or hang, retrying it on every subsequent run
//     would permanently break the backup job ("poison pill"). By only trying once,
//     a bad backup is left behind uncompressed, but future runs continue to succeed.
//  2. Performance: Avoids the I/O overhead of scanning and checking metadata for
//     potentially thousands of historical archives.
//  3. Simplicity: Removes complex state tracking for retries and failure counts.

// Package pathcompression implements the logic for compressing backup directories
// into archive files (zip, tar.gz, etc.) to save space and consolidate files.
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
	"github.com/paulschiretz/pgl-backup/pkg/config"
	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/pathcompressionmetrics"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// compressionRun holds the mutable state for a single execution of the compression manager.
// This makes the CompressionEngine itself stateless and safe for concurrent use if needed.
type compressionRun struct {
	ctx             context.Context
	contentSubDir   string
	format          config.CompressionFormat
	eligibleBackups []metafile.MetafileInfo
	metrics         pathcompressionmetrics.Metrics
	dryRun          bool
	ioWriterPool    *sync.Pool
	ioBufferPool    *sync.Pool
	numWorkers      int
}

type PathCompressionManager struct {
	config config.Config
}

// CompressionManager defines the interface for a component that applies a compression policy to backups.
type CompressionManager interface {
	Compress(ctx context.Context, backups []string, policy config.CompressionPolicyConfig) error
}

// Statically assert that *PathCompressionManager implements the CompressionManager interface.
var _ CompressionManager = (*PathCompressionManager)(nil)

// NewPathCompressionManager creates a new PathCompressionManager with the given configuration.
func NewPathCompressionManager(cfg config.Config) *PathCompressionManager {
	return &PathCompressionManager{
		config: cfg,
	}
}

// Compress processes the specific list of backups provided by the engine.
func (c *PathCompressionManager) Compress(ctx context.Context, backups []string, policy config.CompressionPolicyConfig) error {

	var m pathcompressionmetrics.Metrics
	if c.config.Metrics {
		m = &pathcompressionmetrics.CompressionMetrics{}
	} else {
		// Use the No-op implementation if metrics are disabled.
		m = &pathcompressionmetrics.NoopMetrics{}
	}

	run := &compressionRun{
		ctx:           ctx,
		contentSubDir: c.config.Paths.ContentSubDir,
		format:        policy.Format,
		dryRun:        c.config.DryRun,
		ioWriterPool: &sync.Pool{
			New: func() interface{} {
				return bufio.NewWriterSize(io.Discard, c.config.Engine.Performance.BufferSizeKB*1024)
			},
		},
		ioBufferPool: &sync.Pool{
			New: func() interface{} {
				b := make([]byte, c.config.Engine.Performance.BufferSizeKB*1024)
				return &b
			},
		},
		metrics:         m,
		eligibleBackups: c.identifyEligibleBackups(backups),
		numWorkers:      c.config.Engine.Performance.CompressWorkers,
	}

	// Check if we need compressing
	if len(run.eligibleBackups) == 0 {
		if c.config.DryRun {
			plog.Debug("[DRY RUN] No backups need compressing")
		} else {
			plog.Debug("No backups need compressing")
		}
		return nil
	}
	return run.execute()
}

// execute runs the compression jobs in parallel.
func (r *compressionRun) execute() error {

	plog.Info("Compressing backups", "count", len(r.eligibleBackups))

	// Start progress reporting
	r.metrics.StartProgress("Compression progress", 10*time.Second)
	defer func() {
		r.metrics.StopProgress()
		r.metrics.LogSummary("Compression finished")
	}()

	// --- 4. Compress backups in parallel using a worker pool ---
	// This is especially effective for network drives where latency is a factor.
	// Buffer it to 2x the workers to keep the pipeline full without wasting memory
	compressDirTasksChan := make(chan metafile.MetafileInfo, r.numWorkers*2)
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < r.numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for b := range compressDirTasksChan {
				// Check for cancellation before each deletion.
				select {
				case <-r.ctx.Done():
					// Don't process any more jobs if the context is cancelled.
					return
				default:
				}

				fullPathToCompress := b.RelPathKey

				if r.dryRun {
					plog.Notice("[DRY RUN] COMPRESS", "path", fullPathToCompress)
					continue
				}

				plog.Notice("COMPRESS", "path", fullPathToCompress, "worker", workerID)
				if err := r.compressDirectory(fullPathToCompress); err != nil {
					// If the error is a cancellation, we should not treat it as a failure.
					// We just stop processing this item.
					if err == context.Canceled {
						return
					}

					// A failure to compress a single backup is logged as a warning but does not
					// stop the overall process. The original uncompressed backup is left untouched.
					// We now update the metafile to track the failure.
					r.metrics.AddArchivesFailed(1)
					plog.Warn("Failed to compress directory", "path", fullPathToCompress, "error", err)
				} else {
					r.metrics.AddArchivesCreated(1)
					// On successful archive creation, we first update the metafile.
					b.Metadata.IsCompressed = true
					if writeErr := metafile.Write(fullPathToCompress, b.Metadata); writeErr != nil {
						// If we fail to mark it as compressed, we must not delete the original content.
						// The next run will find the archive and repair the metadata.
						plog.Error("Failed to write updated metafile after compression success. Original content has been preserved.", "path", fullPathToCompress, "error", writeErr)
					} else {
						// Only after the metafile is successfully updated do we remove the original content.
						// This makes the operation more atomic.
						contentDir := filepath.Join(fullPathToCompress, r.contentSubDir)
						if err := os.RemoveAll(contentDir); err != nil {
							plog.Error("Failed to remove original content directory after successful compression and metadata update. The compressed archive is safe, but the original content remains. Manual cleanup may be required.", "path", contentDir, "error", err)
						}
					}
				}
				plog.Notice("COMPRESSED", "path", fullPathToCompress)
			}
		}(i + 1)
	}

	// Feed the jobs in a separate goroutine so the main function can simply wait for the workers to finish.
	go func() {
		defer close(compressDirTasksChan)
		for _, b := range r.eligibleBackups {
			select {
			case <-r.ctx.Done():
				plog.Debug("Cancellation received, stopping compression job feeding.")
				return // Stop feeding on cancel.
			case compressDirTasksChan <- b:
			}
		}
	}()

	wg.Wait()

	return nil
}

// identifyEligibleBackups scans the provided backup paths and returns a list of those
// that have not yet been compressed, based on their metadata.
func (c *PathCompressionManager) identifyEligibleBackups(backups []string) []metafile.MetafileInfo {
	var eligible []metafile.MetafileInfo
	for _, backupPath := range backups {
		// Read metadata
		metadata, err := metafile.Read(backupPath)
		if err != nil {
			plog.Warn("Skipping compression check; cannot read metadata", "path", backupPath, "reason", err)
			continue
		}

		if !metadata.IsCompressed {
			// Store full path in RelPathKey
			eligible = append(eligible, metafile.MetafileInfo{RelPathKey: backupPath, Metadata: metadata})
		}
	}
	return eligible
}

// compressDirectory creates an archive of the directory's contents,
// then deletes the original files, leaving only the archive and essential metadata.
// It does NOT modify metadata or delete the original content, leaving that to the caller.
func (r *compressionRun) compressDirectory(dirPath string) error {
	contentDir := filepath.Join(dirPath, r.contentSubDir)
	// The archive is named after its parent backup directory (e.g., "PGL_Backup_2023-10-27...zip").
	// This makes the archive file easily identifiable and self-describing even if it's
	// moved out of its original context.
	archiveFileName := filepath.Base(dirPath) + "." + r.format.String()
	finalArchivePath := filepath.Join(dirPath, archiveFileName)

	// Cleanup stale tmp files from previous crashed runs
	// We do this first to ensure we aren't wasting disk space or
	// potentially confusing os.CreateTemp.
	r.cleanupStaleTempFiles(dirPath)

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
	tempArchivePath, err := r.createArchive(contentDir)
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
		r.metrics.AddCompressedBytes(info.Size())
	}

	// 2. Move the completed temporary archive into the parent backup directory.

	// os.Rename is atomic on POSIX and uses MoveFileEx with MOVEFILE_REPLACE_EXISTING on Windows.
	if err := os.Rename(tempArchivePath, finalArchivePath); err != nil {
		return fmt.Errorf("failed to rename temporary archive to final destination: %w", err)
	}
	return nil
}

// archiveWriter defines an interface for a generic archive creation utility.
// This allows the main compression logic to be format-agnostic.
type archiveWriter interface {
	// AddFile adds a file from the filesystem to the archive using a pre-calculated relative path.
	AddFile(absPath, relPath string, info os.FileInfo, buf []byte) error
	// AddSymlink adds a symbolic link to the archive.
	AddSymlink(absPath, relPath string, info os.FileInfo) error
	// Close finalizes and closes the archive writer.
	Close() error
}

// zipArchiveWriter implements archiveWriter for .zip files.
type zipArchiveWriter struct {
	zipWriter *zip.Writer
}

func (zw *zipArchiveWriter) AddFile(absSrcPath, relPath string, info os.FileInfo, buf []byte) error {
	// Create a zip header from the file info.
	// This is crucial: zip.Create() uses default permissions and the current time.
	// By using FileInfoHeader, we preserve the original file's permissions (Mode) and modification time.
	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return fmt.Errorf("failed to create zip header for %s: %w", relPath, err)
	}
	header.Name = relPath
	header.Method = zip.Deflate

	writer, err := zw.zipWriter.CreateHeader(header)
	if err != nil {
		return fmt.Errorf("failed to create entry for %s in zip: %w", relPath, err)
	}

	// The file header is created, now open the file on disk to copy its contents.
	fileToZip, err := os.Open(absSrcPath)
	if err != nil {
		return fmt.Errorf("failed to open file %s for zipping: %w", absSrcPath, err)
	}
	defer fileToZip.Close()

	// Copy the file content into the archive writer.
	_, err = io.CopyBuffer(writer, fileToZip, buf)
	if err != nil {
		return fmt.Errorf("failed to copy file %s to zip: %w", absSrcPath, err)
	}
	return nil
}

func (zw *zipArchiveWriter) AddSymlink(absSrcPath, relPath string, info os.FileInfo) error {
	target, err := os.Readlink(absSrcPath)
	if err != nil {
		return fmt.Errorf("failed to read link target for %s: %w", absSrcPath, err)
	}

	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return fmt.Errorf("failed to create zip header for %s: %w", relPath, err)
	}
	header.Name = relPath
	header.Method = zip.Store

	writer, err := zw.zipWriter.CreateHeader(header)
	if err != nil {
		return fmt.Errorf("failed to create entry for %s in zip: %w", relPath, err)
	}
	_, err = writer.Write([]byte(target))
	return err
}

func (zw *zipArchiveWriter) Close() error {
	if err := zw.zipWriter.Close(); err != nil {
		return fmt.Errorf("failed to close zip writer: %w", err)
	}

	return nil
}

// tarArchiveWriter implements archiveWriter for .tar.gz or .tar.zst files.
type tarArchiveWriter struct {
	tarWriter        *tar.Writer
	compressedWriter io.WriteCloser
}

func (tw *tarArchiveWriter) AddFile(absSrcPath, relPath string, info os.FileInfo, buf []byte) error {
	// Create a tar header from the file's info.
	// FileInfoHeader automatically preserves file permissions (Mode) and modification time.
	header, err := tar.FileInfoHeader(info, relPath)
	if err != nil {
		return fmt.Errorf("failed to create tar header for %s: %w", absSrcPath, err)
	}
	// The FileInfoHeader uses the second argument for the link name.
	// We must explicitly set the Name field to the normalized relative path.
	header.Name = relPath

	if err := tw.tarWriter.WriteHeader(header); err != nil {
		return fmt.Errorf("failed to write tar header for %s: %w", relPath, err)
	}

	fileToTar, err := os.Open(absSrcPath)
	if err != nil {
		return fmt.Errorf("failed to open file %s for taring: %w", absSrcPath, err)
	}
	defer fileToTar.Close()

	// Copy the file content into the tar writer.
	_, err = io.CopyBuffer(tw.tarWriter, fileToTar, buf)
	if err != nil {
		return fmt.Errorf("failed to copy file %s to tar: %w", absSrcPath, err)
	}
	return nil
}

func (tw *tarArchiveWriter) AddSymlink(absSrcPath, relPath string, info os.FileInfo) error {
	target, err := os.Readlink(absSrcPath)
	if err != nil {
		return fmt.Errorf("failed to read link target for %s: %w", absSrcPath, err)
	}

	header, err := tar.FileInfoHeader(info, target)
	if err != nil {
		return fmt.Errorf("failed to create tar header for %s: %w", absSrcPath, err)
	}
	header.Name = relPath

	if err := tw.tarWriter.WriteHeader(header); err != nil {
		return fmt.Errorf("failed to write tar header for %s: %w", relPath, err)
	}
	return nil
}

// Close finalizes and closes the tar and underlying compressors in the correct order.
func (tw *tarArchiveWriter) Close() error {
	// Writers must be closed in the correct order: tar first, then compressor.
	if err := tw.tarWriter.Close(); err != nil {
		return err
	}
	return tw.compressedWriter.Close()
}

// createArchive provides a generic, robust, and atomic way to compress a directory.
// It uses a temporary file and an atomic rename to prevent partial/corrupt archives.
func (r *compressionRun) createArchive(sourceDir string) (tempPath string, err error) {
	// 1. Create a temporary file in the destination directory to ensure atomic rename is possible.
	tempFile, err := os.CreateTemp(filepath.Dir(sourceDir), "pgl-backup-*."+r.format.String()+".tmp")
	if err != nil {
		return "", fmt.Errorf("failed to create temporary archive file: %w", err)
	}
	tempPath = tempFile.Name()

	// 1. Get a Buffer from the pool (The "Middleman")
	// This sits between the compressor and the disk to reduce syscalls.
	// We reuse buffers to reduce GC pressure.
	bufWriter := r.ioWriterPool.Get().(*bufio.Writer)
	bufWriter.Reset(tempFile)
	defer func() {
		bufWriter.Reset(io.Discard)
		r.ioWriterPool.Put(bufWriter)
	}()

	// 2. Set up the appropriate archive writer based on the format.
	var archiver archiveWriter
	switch r.format {
	case config.ZipFormat:
		zipWriter := zip.NewWriter(bufWriter)
		zipWriter.RegisterCompressor(zip.Deflate, func(out io.Writer) (io.WriteCloser, error) {
			return flate.NewWriter(out, flate.DefaultCompression)
		})
		archiver = &zipArchiveWriter{zipWriter: zipWriter}
	case config.TarGzFormat:
		gzipWriter := pgzip.NewWriter(bufWriter)
		tarWriter := tar.NewWriter(gzipWriter)
		archiver = &tarArchiveWriter{tarWriter: tarWriter, compressedWriter: gzipWriter}
	case config.TarZstFormat:
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
		return "", fmt.Errorf("unsupported compression format: %s", r.format)
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
	walkErr := filepath.Walk(sourceDir, func(absSrcPath string, info os.FileInfo, walkErr error) error {
		select {
		case <-r.ctx.Done():
			return context.Canceled
		default:
		}

		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() || absSrcPath == tempPath {
			return nil
		}

		// Calculate the relative path for the archive entry.
		relPath, err := filepath.Rel(sourceDir, absSrcPath)
		if err != nil {
			return fmt.Errorf("failed to get relative path for %s: %w", absSrcPath, err)
		}
		// Normalize to forward slashes for archive compatibility.
		relPath = util.NormalizePath(relPath)

		plog.Notice("ADD", "source", sourceDir, "file", relPath)
		r.metrics.AddEntriesProcessed(1)
		r.metrics.AddOriginalBytes(info.Size())

		// Handle Symlinks
		if info.Mode()&os.ModeSymlink != 0 {
			return archiver.AddSymlink(absSrcPath, relPath, info)
		}

		// Handle Regular Files
		// Get a buffer from the pool for the copy operation.
		bufPtr := r.ioBufferPool.Get().(*[]byte)
		defer r.ioBufferPool.Put(bufPtr)

		return archiver.AddFile(absSrcPath, relPath, info, *bufPtr)
	})

	if walkErr != nil {
		err = walkErr // Assign to the named return variable so the defer block sees it.
		return "", err
	}

	// On success, return the path to the completed temporary file. The defer will not remove it because `err` is nil.
	return tempPath, nil
}

func (r *compressionRun) cleanupStaleTempFiles(dirPath string) {
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
