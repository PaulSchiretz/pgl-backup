package pathcompression

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/pathcompressionmetrics"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// compressTask holds the mutable state for a single compression execution.
// This makes the PathCompressor itself stateless and safe for concurrent use if needed.
type compressTask struct {
	*PathCompressor
	absBasePath       string
	relContentPathKey string
	ctx               context.Context
	toCompress        metafile.MetafileInfo
	format            Format
	level             Level
	timestampUTC      time.Time
	metrics           pathcompressionmetrics.Metrics
	dryRun            bool
}

// compressor defines the interface for compressing a directory into an archive file.
type compressor interface {
	Compress(ctx context.Context, sourceDir, archivePath string) error
}

// execute runs the compression task.
func (t *compressTask) execute() error {
	if t.toCompress.RelPathKey == "" {
		return ErrNothingToCompress
	}

	plog.Info("Compressing backup", "path", t.toCompress.RelPathKey)

	// Start progress reporting
	t.metrics.StartProgress("Compression progress", 10*time.Second)
	defer func() {
		t.metrics.StopProgress()
		t.metrics.LogSummary("Compression finished")
	}()

	b := t.toCompress

	if t.dryRun {
		plog.Notice("[DRY RUN] COMPRESS", "path", b.RelPathKey)
		return nil
	}

	absToCompressPath := util.DenormalizePath(filepath.Join(t.absBasePath, b.RelPathKey))
	absToCompressContentPath := util.DenormalizePath(filepath.Join(absToCompressPath, t.relContentPathKey))

	// Check if the backup directory still exists.
	if _, err := os.Stat(absToCompressPath); os.IsNotExist(err) {
		return fmt.Errorf("backup directory not found: %s", b.RelPathKey)
	}

	plog.Notice("COMPRESS", "path", b.RelPathKey)
	if err := t.compressBackup(absToCompressPath, absToCompressContentPath); err != nil {
		t.metrics.AddArchivesFailed(1)
		return err
	}

	t.metrics.AddArchivesCreated(1)
	// 1. On successful archive creation, we update the metafile.
	b.Metadata.IsCompressed = true
	b.Metadata.CompressionFormat = t.format.String()
	if writeErr := metafile.Write(absToCompressPath, &b.Metadata); writeErr != nil {
		plog.Error("Failed to write updated metafile after compression success. Original content has been preserved.", "path", b.RelPathKey, "error", writeErr)
		return fmt.Errorf("failed to update metafile: %w", writeErr)
	}

	// 2. Only after the metafile is successfully updated we remove the original content.
	if err := os.RemoveAll(absToCompressContentPath); err != nil {
		plog.Error("Failed to remove original content directory after successful compression. Manual cleanup may be required.", "path", b.RelPathKey, "error", err)
	}

	plog.Notice("COMPRESSED", "path", b.RelPathKey)
	return nil
}

// compressBackup creates the compressed archive of the backup's contents
// It does NOT modify metadata or delete the original content, leaving that to the caller.
func (t *compressTask) compressBackup(absToCompressPath, absToCompressContentPath string) error {
	// The archive is named after its parent backup directory (e.g., "PGL_Backup_2023-10-27...zip").
	// This makes the archive file easily identifiable and self-describing even if it's
	// moved out of its original context.
	archiveFileName := filepath.Base(absToCompressPath) + "." + t.format.String()
	absArchiveFilePath := util.DenormalizePath(filepath.Join(absToCompressPath, archiveFileName))

	// Cleanup stale tmp files from crashed runs.
	defer t.cleanupStaleTempFiles(absToCompressPath)

	var comp compressor
	switch t.format {
	case Zip:
		comp = newZipCompressor(t.format, t.level, t.ioBufferPool, t.ioBufferSize, t.readAheadLimiter, t.readAheadLimitSize, t.numCompressWorkers, t.metrics)
	case TarGz, TarZst:
		comp = newTarCompressor(t.format, t.level, t.ioBufferPool, t.ioBufferSize, t.readAheadLimiter, t.readAheadLimitSize, t.numCompressWorkers, t.metrics)
	default:
		return fmt.Errorf("unsupported format: %s", t.format)
	}
	if err := comp.Compress(t.ctx, absToCompressContentPath, absArchiveFilePath); err != nil {
		return err
	}
	return nil
}

func (t *compressTask) cleanupStaleTempFiles(dirPath string) {
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
