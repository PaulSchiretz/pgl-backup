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
	Compress(ctx context.Context, compressionTitle string, dirPath string, excludeDir string, format config.CompressionFormat) error
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
func (c *PathCompressionManager) Compress(ctx context.Context, compressionPolicyTitle string, dirPath string, excludeDir string, format config.CompressionFormat) error {

	runState := &compressionRunState{
		compressionPolicyTitle: compressionPolicyTitle,
		dirPath:                dirPath,
		excludeDir:             excludeDir,
		format:                 format,
	}

	// Prepare for the compression run
	if err := c.prepareRun(ctx, runState); err != nil {
		return err
	}

	if len(runState.backups) == 0 {
		plog.Debug(fmt.Sprintf("No %s backups found to compress", runState.compressionPolicyTitle))
		return nil
	}

	plog.Info(fmt.Sprintf("Compressing %s backups", runState.compressionPolicyTitle))

	// Determine which backups to compress
	backupsToCompress := c.determineBackupsToCompress(runState)

	// Collect all backups that should be compressed
	var dirsToCompress []string
	for _, backup := range runState.backups {
		if _, shouldCompress := backupsToCompress[backup.RelPathKey]; shouldCompress {
			dirsToCompress = append(dirsToCompress, util.DenormalizePath(backup.RelPathKey))
		}
	}

	if len(dirsToCompress) == 0 && !c.config.DryRun {
		plog.Debug("No backups to compress", "policy", runState.compressionPolicyTitle)
		return nil
	}

	plog.Info("Preparing to compress backups", "policy", runState.compressionPolicyTitle, "count", len(dirsToCompress))

	// --- 4. Compress backups in parallel using a worker pool ---
	// This is especially effective for network drives where latency is a factor.
	numWorkers := c.config.Engine.Performance.CompressWorkers // Use the configured number of workers.
	var wg sync.WaitGroup
	compressDirTasksChan := make(chan string, len(dirsToCompress))

	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for dirToCompress := range compressDirTasksChan {
				// Check for cancellation before each deletion.
				select {
				case <-ctx.Done():
					// Don't process any more jobs if context is cancelled.
					return
				default:
				}

				fullPathToCompress := filepath.Join(runState.dirPath, dirToCompress)

				plog.Debug("Compressing backup", "policy", runState.compressionPolicyTitle, "path", fullPathToCompress, "worker", workerID)
				if c.config.DryRun {
					plog.Info("[DRY RUN] Would compress directory", "policy", runState.compressionPolicyTitle, "path", fullPathToCompress)
					continue
				}

				if err := c.compressDirectory(ctx, fullPathToCompress, runState.format); err != nil {
					plog.Warn("Failed to compress directory", "policy", runState.compressionPolicyTitle, "path", fullPathToCompress, "error", err)
				}
			}
		}(i + 1)
	}

	// Feed the jobs in a separate goroutine so the main function can simply wait for the workers to finish.
	go func() {
		defer close(compressDirTasksChan)
		for _, dir := range dirsToCompress {
			select {
			case <-ctx.Done():
				plog.Info("Cancellation received, stopping compression job feeding.")
				return // Stop feeding on cancel.
			case compressDirTasksChan <- dir:
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
			plog.Debug("Directory does not exist, no backups to compress.", "path", runState.dirPath)
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
			plog.Warn("Skipping directory for compression check; cannot read metadata", "directory", dirName, "reason", err)
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

// determineBackupsToCompress applies the retention policy to a sorted list of backups.
func (c *PathCompressionManager) determineBackupsToCompress(runState *compressionRunState) map[string]bool {
	backupsToCompress := make(map[string]bool)

	for _, b := range runState.backups {
		markerPath := filepath.Join(runState.dirPath, util.DenormalizePath(b.RelPathKey), config.CompressedFileName)
		_, err := os.Stat(markerPath)

		if os.IsNotExist(err) {
			// Marker does not exist, so this backup needs to be compressed.
			backupsToCompress[b.RelPathKey] = true
		} else if err != nil {
			// Some other error occurred trying to stat the marker file.
			plog.Warn("Could not stat compression marker, skipping backup", "path", markerPath, "error", err)
		}
		// If err is nil, the marker exists, and we do nothing.
	}

	plog.Debug("Total unique backups to compress", "policy", runState.compressionPolicyTitle, "count", len(backupsToCompress))

	return backupsToCompress
}

// compressDirectory creates an archive of the directory's contents,
// then deletes the original files, leaving only the archive and essential metadata.
// The final structure will be the original directory containing the .meta.json file,
// the compressed archive, and a .pgl-backup-compressed marker.
func (c *PathCompressionManager) compressDirectory(ctx context.Context, dirPath string, format config.CompressionFormat) error {
	contentDir := filepath.Join(dirPath, c.config.Paths.ContentSubDir)
	archiveFileName := filepath.Base(dirPath) + "." + format.String()
	finalArchivePath := filepath.Join(dirPath, archiveFileName)

	plog.Debug("Creating archive for content directory", "path", contentDir, "format", format)

	// 1. Create the archive in a temporary file.
	tempArchivePath, err := c.createArchive(ctx, contentDir, format)
	if err != nil {
		if err == context.Canceled {
			plog.Info("Compression was canceled", "path", dirPath)
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

	// 4. Create a marker file to indicate that this directory is now compressed.
	markerPath := filepath.Join(dirPath, config.CompressedFileName)
	if err := os.WriteFile(markerPath, []byte{}, util.UserWritableFilePerms); err != nil {
		// If creating the marker fails, try to clean up the archive to avoid an inconsistent state.
		os.Remove(finalArchivePath)
		return fmt.Errorf("failed to create compression marker file %s: %w", markerPath, err)
	}

	plog.Debug("Successfully compressed backup contents", "path", dirPath, "archive", archiveFileName)
	return nil
}

// archiveWriter defines an interface for a generic archive creation utility.
// This allows the main compression logic to be format-agnostic.
type archiveWriter interface {
	// AddFile adds a file from the filesystem to the archive.
	AddFile(path string, info os.FileInfo, sourceDir string) error
	// Close finalizes and closes the archive writer.
	Close() error
}

// zipArchiveWriter implements archiveWriter for .zip files.
type zipArchiveWriter struct {
	zipWriter *zip.Writer
}

func (zw *zipArchiveWriter) AddFile(path string, info os.FileInfo, sourceDir string) error {
	relPath, err := filepath.Rel(sourceDir, path)
	if err != nil {
		return fmt.Errorf("failed to get relative path for %s: %w", path, err)
	}
	// Archive formats (zip, tar) expect forward slashes for path separators.
	relPath = util.NormalizePath(relPath)

	writer, err := zw.zipWriter.Create(relPath)
	if err != nil {
		return fmt.Errorf("failed to create entry for %s in zip: %w", relPath, err)
	}

	fileToZip, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file %s for zipping: %w", path, err)
	}
	defer fileToZip.Close()

	_, err = io.Copy(writer, fileToZip)
	if err != nil {
		return fmt.Errorf("failed to copy file %s to zip: %w", path, err)
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

func (tw *tarGzArchiveWriter) AddFile(path string, info os.FileInfo, sourceDir string) error {
	relPath, err := filepath.Rel(sourceDir, path)
	if err != nil {
		return fmt.Errorf("failed to get relative path for %s: %w", path, err)
	}
	// Archive formats (zip, tar) expect forward slashes for path separators.
	relPath = util.NormalizePath(relPath)

	header, err := tar.FileInfoHeader(info, relPath)
	if err != nil {
		return fmt.Errorf("failed to create tar header for %s: %w", path, err)
	}
	header.Name = relPath

	if err := tw.tarWriter.WriteHeader(header); err != nil {
		return fmt.Errorf("failed to write tar header for %s: %w", relPath, err)
	}

	fileToTar, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file %s for taring: %w", path, err)
	}
	defer fileToTar.Close()

	_, err = io.Copy(tw.tarWriter, fileToTar)
	if err != nil {
		return fmt.Errorf("failed to copy file %s to tar: %w", path, err)
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
	walkErr := filepath.Walk(sourceDir, func(path string, info os.FileInfo, walkErr error) error {
		select {
		case <-ctx.Done():
			return context.Canceled
		default:
		}

		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() || path == tempPath || (info.Mode()&os.ModeSymlink != 0) {
			return nil
		}

		return archiver.AddFile(path, info, sourceDir)
	})

	if walkErr != nil {
		err = walkErr // Assign to the named return variable so the defer block sees it.
		return "", err
	}

	// On success, return the path to the completed temporary file. The deferred cleanup will not remove it.
	return tempPath, nil
}
