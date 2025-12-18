package pathcompression

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
	"pixelgardenlabs.io/pgl-backup/pkg/config"
	"pixelgardenlabs.io/pgl-backup/pkg/metafile"
	"pixelgardenlabs.io/pgl-backup/pkg/plog"
	"pixelgardenlabs.io/pgl-backup/pkg/util"
)

// retentionRunState holds the state specific to a single apply operation.
type compressionRunState struct {
	compressionPolicyTitle string
	dirPath                string
	excludeDir             string
	format                 config.CompressionFormat
	maxRetries             int
	backups                []backupInfo
}

type PathCompressionManager struct {
	config config.Config
}

// backupInfo holds the parsed metadata and rel directory path of a backup found on disk.
type backupInfo struct {
	RelPathKey string // Normalized, forward-slash and maybe otherwise modified key. NOT for direct FS access.
	Metadata   metafile.MetafileContent
}

// CompressionManager defines the interface for a component that applies a compression policy to backups.
type CompressionManager interface {
	Compress(ctx context.Context, compressionPolicyTitle string, dirPath string, excludeDir string, policy config.CompressionPolicyConfig) error
}

// Statically assert that *PathCompressionManager implements the CompressionManager interface.
var _ CompressionManager = (*PathCompressionManager)(nil)

// NewPathCompressionManager creates a new PathCompressionManager with the given configuration.
func NewPathCompressionManager(cfg config.Config) *PathCompressionManager {
	return &PathCompressionManager{
		config: cfg,
	}
}

// Compress scans a given directory and compresses backups if needed
func (c *PathCompressionManager) Compress(ctx context.Context, compressionPolicyTitle string, dirPath string, excludeDir string, policy config.CompressionPolicyConfig) error {

	runState := &compressionRunState{
		compressionPolicyTitle: compressionPolicyTitle,
		dirPath:                dirPath,
		excludeDir:             excludeDir,
		format:                 policy.Format,
		maxRetries:             policy.MaxRetries,
	}

	// Prepare for the compression run
	if err := c.prepareRun(ctx, runState); err != nil {
		return err
	}

	if len(runState.backups) == 0 {
		plog.Debug("No backups to compress", "policy", runState.compressionPolicyTitle)
		return nil
	}

	// Determine which backups to compress
	eligibleBackups := c.filterBackupsToCompress(runState)

	if len(eligibleBackups) == 0 {
		if c.config.DryRun {
			plog.Debug("[DRY RUN] No backups need compressing", "policy", runState.compressionPolicyTitle)
		} else {
			plog.Debug("No backups need compressing", "policy", runState.compressionPolicyTitle)
		}
		return nil
	}

	plog.Info("Compressing backups", "policy", runState.compressionPolicyTitle, "count", len(eligibleBackups))

	// --- 4. Compress backups in parallel using a worker pool ---
	// This is especially effective for network drives where latency is a factor.
	numWorkers := c.config.Engine.Performance.CompressWorkers
	var wg sync.WaitGroup
	compressDirTasksChan := make(chan backupInfo, len(eligibleBackups))

	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for b := range compressDirTasksChan {
				// Check for cancellation before each deletion.
				select {
				case <-ctx.Done():
					// Don't process any more jobs if the context is cancelled.
					return
				default:
				}

				fullPathToCompress := filepath.Join(runState.dirPath, util.DenormalizePath(b.RelPathKey))

				if c.config.DryRun {
					plog.Notice("[DRY RUN] COMPRESS", "policy", runState.compressionPolicyTitle, "path", fullPathToCompress)
					continue
				}

				plog.Notice("COMPRESS", "policy", runState.compressionPolicyTitle, "path", fullPathToCompress, "worker", workerID)
				if err := c.compressDirectory(ctx, fullPathToCompress, runState.format); err != nil {
					// A failure to compress a single backup is logged as a warning but does not
					// stop the overall process. The original uncompressed backup is left untouched.
					// We now update the metafile to track the failure.
					plog.Warn("Failed to compress directory, incrementing attempt count", "policy", runState.compressionPolicyTitle, "path", fullPathToCompress, "error", err, "current_attempts", b.Metadata.CompressionAttempts)

					b.Metadata.CompressionAttempts++ // Increment the attempt count on the in-memory copy
					if writeErr := metafile.Write(fullPathToCompress, b.Metadata); writeErr != nil {
						// This should stay an error, as we'll try to compress over and over again on subsequent runs and this needs attention
						plog.Error("Failed to write updated metafile after compression failure. Attempt count not saved.", "path", fullPathToCompress, "error", writeErr)
					}
				}
				plog.Notice("COMPRESSED", "policy", runState.compressionPolicyTitle, "path", fullPathToCompress)
			}
		}(i + 1)
	}

	// Feed the jobs in a separate goroutine so the main function can simply wait for the workers to finish.
	go func() {
		defer close(compressDirTasksChan)
		for _, b := range eligibleBackups {
			select {
			case <-ctx.Done():
				plog.Debug("Cancellation received, stopping compression job feeding.")
				return // Stop feeding on cancel.
			case compressDirTasksChan <- b:
			}
		}
	}()

	wg.Wait()

	return nil
}

// prepareRun prepares the runState for an retention operation.
func (c *PathCompressionManager) prepareRun(ctx context.Context, runState *compressionRunState) error {

	// Get a sorted list of all valid backups
	err := c.fetchSortedBackups(ctx, runState)
	if err != nil {
		return err
	}
	return nil
}

// fetchSortedBackups scans a directory for valid backup folders, parses their
// metadata to get an accurate timestamp, and returns them sorted from newest to oldest.
// It relies exclusively on the `.pgl-backup.meta.json` file; directories without a
// readable metafile are ignored for retention purposes.
func (c *PathCompressionManager) fetchSortedBackups(ctx context.Context, runState *compressionRunState) error {
	prefix := c.config.Naming.Prefix

	entries, err := os.ReadDir(runState.dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			plog.Debug("Directory does not exist, no backups to compress.", "policy", runState.compressionPolicyTitle, "path", runState.dirPath)
			return nil // Not an error, just means no archives exist yet.
		}
		return fmt.Errorf("failed to read backup directory %s: %w", runState.dirPath, err)
	}

	var foundBackups []backupInfo
	for _, entry := range entries {
		// Check for cancellation during the directory scan.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		dirName := entry.Name()
		if !entry.IsDir() || !strings.HasPrefix(dirName, prefix) || dirName == runState.excludeDir {
			continue
		}

		backupPath := filepath.Join(runState.dirPath, dirName)
		metadata, err := metafile.Read(backupPath)
		if err != nil {
			plog.Warn("Skipping compression check; cannot read metadata", "policy", runState.compressionPolicyTitle, "directory", dirName, "reason", err)
			continue
		}

		// The metafile is the sole source of truth for the backup time.
		foundBackups = append(foundBackups, backupInfo{RelPathKey: util.NormalizePath(dirName), Metadata: metadata})
	}

	// Sort all backups from newest to oldest for consistent processing.
	sort.Slice(foundBackups, func(i, j int) bool {
		return foundBackups[i].Metadata.TimestampUTC.After(foundBackups[j].Metadata.TimestampUTC)
	})

	// fill the runstate
	runState.backups = foundBackups
	return nil
}

// filterBackupsToCompress filters a list of backups to find those eligible for compression.
func (c *PathCompressionManager) filterBackupsToCompress(runState *compressionRunState) []backupInfo {
	var eligibleBackups []backupInfo

	for _, b := range runState.backups {
		// Skip if already compressed.
		if b.Metadata.IsCompressed {
			continue
		}

		// Skip if we have exceeded the max number of retries.
		if b.Metadata.CompressionAttempts >= runState.maxRetries {
			plog.Debug("Skipping compression; reached max retries", "policy", runState.compressionPolicyTitle, "path", b.RelPathKey, "attempts", b.Metadata.CompressionAttempts)
			continue
		}

		// If not compressed and not over the retry limit, mark it for compression.
		eligibleBackups = append(eligibleBackups, b)
	}

	plog.Debug("Total backups to compress", "policy", runState.compressionPolicyTitle, "count", len(eligibleBackups))

	return eligibleBackups
}

// compressDirectory creates an archive of the directory's contents,
// then deletes the original files, leaving only the archive and essential metadata.
// The final structure will be the original directory containing the updated .meta.json file and the compressed archive.
func (c *PathCompressionManager) compressDirectory(ctx context.Context, dirPath string, format config.CompressionFormat) error {
	contentDir := filepath.Join(dirPath, c.config.Paths.ContentSubDir)
	// The archive is named after its parent backup directory (e.g., "PGL_Backup_2023-10-27...zip").
	// This makes the archive file easily identifiable and self-describing even if it's
	// moved out of its original context.
	archiveFileName := filepath.Base(dirPath) + "." + format.String()
	finalArchivePath := filepath.Join(dirPath, archiveFileName)

	// 1. Create the archive in a temporary file.
	tempArchivePath, err := c.createArchive(ctx, contentDir, format)
	if err != nil {
		if err == context.Canceled {
			plog.Debug("Compression was canceled during archive creation", "path", dirPath)
			return err
		}
		return fmt.Errorf("failed to create archive: %w", err)
	}
	defer os.Remove(tempArchivePath)

	// 2. Move the completed temporary archive into the parent backup directory.
	// Ensure destination doesn't exist before renaming (crucial for Windows)
	_ = os.Remove(finalArchivePath)
	if err := os.Rename(tempArchivePath, finalArchivePath); err != nil {
		return fmt.Errorf("failed to rename temporary archive to final destination: %w", err)
	}

	// 3. Remove the original content directory now that the archive is in place.
	if err := os.RemoveAll(contentDir); err != nil {
		return fmt.Errorf("failed to remove original content directory %s: %w", contentDir, err)
	}

	// 4. Update the metafile to mark this backup as compressed.
	metadata, err := metafile.Read(dirPath)
	if err != nil {
		// This is a significant problem. The backup is compressed, but we can't mark it as such.
		// We should log this clearly. The next run will likely try to re-compress.
		return fmt.Errorf("failed to read metafile to update compression status: %w", err)
	}
	metadata.IsCompressed = true
	if err := metafile.Write(dirPath, metadata); err != nil {
		os.Remove(finalArchivePath)
		return fmt.Errorf("failed to write updated metafile to mark as compressed: %w", err)
	}
	return nil
}

// archiveWriter defines an interface for a generic archive creation utility.
// This allows the main compression logic to be format-agnostic.
type archiveWriter interface {
	// AddFile adds a file from the filesystem to the archive using a pre-calculated relative path.
	AddFile(absPath, relPath string, info os.FileInfo) error
	// Close finalizes and closes the archive writer.
	Close() error
}

// zipArchiveWriter implements archiveWriter for .zip files.
type zipArchiveWriter struct {
	zipWriter *zip.Writer
}

func (zw *zipArchiveWriter) AddFile(absSrcPath, relPath string, info os.FileInfo) error {
	writer, err := zw.zipWriter.Create(relPath)
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
	_, err = io.Copy(writer, fileToZip)
	if err != nil {
		return fmt.Errorf("failed to copy file %s to zip: %w", absSrcPath, err)
	}
	return nil
}

func (zw *zipArchiveWriter) Close() error {
	if err := zw.zipWriter.Close(); err != nil {
		return fmt.Errorf("failed to close zip writer: %w", err)
	}

	return nil
}

// tarGzArchiveWriter implements archiveWriter for .tar.gz files.
type tarGzArchiveWriter struct {
	tarWriter  *tar.Writer
	gzipWriter *gzip.Writer
}

func (tw *tarGzArchiveWriter) AddFile(absSrcPath, relPath string, info os.FileInfo) error {
	// Create a tar header from the file's info.
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
	_, err = io.Copy(tw.tarWriter, fileToTar)
	if err != nil {
		return fmt.Errorf("failed to copy file %s to tar: %w", absSrcPath, err)
	}
	return nil
}

// tarZstdArchiveWriter implements archiveWriter for .tar.zst files.
type tarZstdArchiveWriter struct {
	tarWriter  *tar.Writer
	zstdWriter *zstd.Encoder
}

func (tw *tarZstdArchiveWriter) AddFile(absSrcPath, relPath string, info os.FileInfo) error {
	// Create a tar header from the file's info.
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
	_, err = io.Copy(tw.tarWriter, fileToTar)
	if err != nil {
		return fmt.Errorf("failed to copy file %s to tar: %w", absSrcPath, err)
	}
	return nil
}

func (tw *tarGzArchiveWriter) Close() error {
	// Writers must be closed in the correct order: tar first, then gzip.
	// This ensures all data is written to the underlying gzip stream before it's closed.
	// The error check is combined to ensure both are attempted.
	errTar := tw.tarWriter.Close()
	errGzip := tw.gzipWriter.Close()
	if errTar != nil {
		return fmt.Errorf("failed to close tar writer: %w", errTar)
	}
	if errGzip != nil {
		return fmt.Errorf("failed to close gzip writer: %w", errGzip)
	}
	return nil
}

// Close finalizes and closes the tar and zstd writers in the correct order.
func (tw *tarZstdArchiveWriter) Close() error {
	// Writers must be closed in the correct order: tar first, then zstd.
	errTar := tw.tarWriter.Close()
	errZstd := tw.zstdWriter.Close()

	if errTar != nil {
		return fmt.Errorf("failed to close tar writer: %w", errTar)
	}
	if errZstd != nil {
		return fmt.Errorf("failed to close zstd writer: %w", errZstd)
	}
	return nil
}

// createArchive provides a generic, robust, and atomic way to compress a directory.
// It uses a temporary file and an atomic rename to prevent partial/corrupt archives.
func (c *PathCompressionManager) createArchive(ctx context.Context, sourceDir string, format config.CompressionFormat) (tempPath string, err error) {
	// 1. Create a temporary file in the destination directory to ensure atomic rename is possible.
	tempFile, err := os.CreateTemp(filepath.Dir(sourceDir), "pgl-backup-*."+format.String()+".tmp")
	if err != nil {
		return "", fmt.Errorf("failed to create temporary archive file: %w", err)
	}
	tempPath = tempFile.Name()

	// 2. Set up the appropriate archive writer based on the format.
	var archiver archiveWriter
	switch format {
	case config.ZipFormat:
		zipWriter := zip.NewWriter(tempFile)
		archiver = &zipArchiveWriter{zipWriter: zipWriter}
	case config.TarGzFormat:
		gzipWriter := gzip.NewWriter(tempFile)
		tarWriter := tar.NewWriter(gzipWriter)
		archiver = &tarGzArchiveWriter{tarWriter: tarWriter, gzipWriter: gzipWriter}
	case config.TarZstFormat:
		zstdWriter, err := zstd.NewWriter(tempFile)
		if err != nil {
			return "", fmt.Errorf("failed to create zstd writer: %w", err)
		}
		tarWriter := tar.NewWriter(zstdWriter)
		archiver = &tarZstdArchiveWriter{tarWriter: tarWriter, zstdWriter: zstdWriter}
	default:
		// This should be caught earlier, but we handle it here for safety.
		tempFile.Close()
		os.Remove(tempPath)
		return "", fmt.Errorf("unsupported compression format: %s", format)
	}

	// Defer the core cleanup logic. This function will be executed when compressWith returns.
	// It checks the named return variable 'err' to decide its course of action.
	// We use a separate function for the defer to capture the original error `err`
	// and handle any new errors during cleanup without overwriting the original, more important error.
	defer func(originalErr *error) {
		// If the main operation has already failed (e.g., a read error during walk),
		// we just need to clean up. We don't care about any new errors from closing,
		// as the original error is the root cause and more important.
		if *originalErr != nil {
			archiver.Close() // Attempt to close, but ignore errors.
			tempFile.Close() // Attempt to close, but ignore errors.
			os.Remove(tempPath)
			return
		}
		// If the main operation was successful, we must handle close errors properly,
		// as an error on close means the archive is corrupt.
		if err := archiver.Close(); err != nil {
			*originalErr = err // This error is critical, as the archive is corrupt.
		}
		// Only assign file close error if archiver close was successful.
		if err := tempFile.Close(); err != nil && *originalErr == nil {
			*originalErr = err // Report this error if closing the archiver was successful.
		}
	}(&err)

	// Walk the directory and add files to the archive.
	walkErr := filepath.Walk(sourceDir, func(absSrcPath string, info os.FileInfo, walkErr error) error {
		select {
		case <-ctx.Done():
			return context.Canceled
		default:
		}

		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() || absSrcPath == tempPath || (info.Mode()&os.ModeSymlink != 0) {
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
		return archiver.AddFile(absSrcPath, relPath, info)
	})

	if walkErr != nil {
		err = walkErr // Assign to the named return variable so the defer block sees it.
		return "", err
	}

	// On success, return the path to the completed temporary file. The deferred cleanup will not remove it.
	return tempPath, nil
}
