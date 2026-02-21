package pathcompression

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// compressTask holds the mutable state for a single compression execution.
// This makes the PathCompressor itself stateless and safe for concurrent use if needed.
type compressTask struct {
	*PathCompressor
	absToCompressPath        string
	absToCompressContentPath string
	ctx                      context.Context
	format                   Format
	level                    Level
	timestampUTC             time.Time
	metrics                  Metrics
	dryRun                   bool
}

// execute runs the compression task.
func (t *compressTask) execute() error {

	plog.Info("Compressing backup", "source", t.absToCompressPath)

	// Start progress reporting
	t.metrics.StartProgress("Compression progress", 10*time.Second)
	defer func() {
		t.metrics.StopProgress()
		t.metrics.LogSummary("Compression finished")
	}()

	if !t.dryRun {
		// Check if the backup directory still exists.
		if _, err := os.Stat(t.absToCompressPath); os.IsNotExist(err) {
			return fmt.Errorf("backup directory not found: %s", t.absToCompressPath)
		}
	}

	if err := t.compressBackup(t.absToCompressPath, t.absToCompressContentPath); err != nil {
		t.metrics.AddArchivesFailed(1)
		return err
	}
	t.metrics.AddArchivesCreated(1)
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
		comp = newZipCompressor(t.dryRun, t.format, t.level, t.ioBufferPool, t.ioBufferSize, t.readAheadLimiter, t.readAheadLimit, t.numCompressWorkers, t.metrics)
	case TarGz, TarZst:
		comp = newTarCompressor(t.dryRun, t.format, t.level, t.ioBufferPool, t.ioBufferSize, t.readAheadLimiter, t.readAheadLimit, t.numCompressWorkers, t.metrics)
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
