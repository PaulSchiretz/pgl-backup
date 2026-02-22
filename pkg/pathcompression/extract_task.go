package pathcompression

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// extractTask holds the mutable state for a single decompression execution.
type extractTask struct {
	*PathCompressor
	ctx                  context.Context
	format               Format
	absToExtractPath     string
	absExtractTargetPath string
	overwriteBehavior    OverwriteBehavior
	modTimeWindow        time.Duration
	timestampUTC         time.Time
	metrics              Metrics
	dryRun               bool
}

func (t *extractTask) execute() error {

	plog.Info("Extracting backup", "source", t.absToExtractPath, "target", t.absExtractTargetPath)

	t.metrics.StartProgress("Extraction progress", 10*time.Second)
	defer func() {
		t.metrics.StopProgress()
		t.metrics.LogSummary("Extraction finished")
	}()

	if !t.dryRun {
		// Check if the backup directory still exists.
		if _, err := os.Stat(t.absToExtractPath); os.IsNotExist(err) {
			return fmt.Errorf("backup directory not found: %s", t.absToExtractPath)
		}
	}

	if err := t.extractBackup(t.absToExtractPath, t.absExtractTargetPath); err != nil {
		t.metrics.AddArchivesFailed(1)
		return err
	}
	t.metrics.AddArchivesExtracted(1)
	return nil
}

// extractBackup creates the extracts the archive of the backup's contents to the target path
func (t *extractTask) extractBackup(absToExtractPath, absExtractTargetPath string) error {

	// The archive is named after its parent backup directory (e.g., "PGL_Backup_2023-10-27...zip").
	archiveFileName := filepath.Base(absToExtractPath) + "." + t.format.String()
	absArchiveFilePath := util.DenormalizedAbsPath(absToExtractPath, archiveFileName)

	var extr extractor
	switch t.format {
	case Zip:
		extr = newZipExtractor(t.dryRun, t.format, t.ioBufferPool, t.metrics, t.overwriteBehavior, t.modTimeWindow)
	case TarGz, TarZst:
		extr = newTarExtractor(t.dryRun, t.format, t.ioBufferPool, t.metrics, t.overwriteBehavior, t.modTimeWindow)
	default:
		return fmt.Errorf("unsupported format: %s", t.format)
	}

	if err := extr.Extract(t.ctx, absArchiveFilePath, absExtractTargetPath); err != nil {
		return err
	}
	return nil
}
