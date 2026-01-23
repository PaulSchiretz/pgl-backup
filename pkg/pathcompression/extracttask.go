package pathcompression

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/pathcompressionmetrics"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// extractTask holds the mutable state for a single decompression execution.
type extractTask struct {
	*PathCompressor
	ctx                  context.Context
	absTargetBasePath    string
	toExtract            metafile.MetafileInfo
	absExtractTargetPath string
	format               Format
	overwriteBehavior    OverwriteBehavior
	timestampUTC         time.Time
	metrics              pathcompressionmetrics.Metrics
	dryRun               bool
}

func (t *extractTask) execute() error {

	if t.toExtract.RelPathKey == "" {
		return ErrNothingToExtract
	}

	plog.Info("Extracting backup", "path", t.toExtract.RelPathKey, "target", t.absExtractTargetPath)

	t.metrics.StartProgress("Extraction progress", 10*time.Second)
	defer func() {
		t.metrics.StopProgress()
		t.metrics.LogSummary("Extraction finished")
	}()

	b := t.toExtract

	if t.dryRun {
		plog.Notice("[DRY RUN] EXTRACT", "path", b.RelPathKey)
		return nil
	}

	absToExtractPath := util.DenormalizePath(filepath.Join(t.absTargetBasePath, b.RelPathKey))
	// Check if the backup directory still exists.
	if _, err := os.Stat(absToExtractPath); os.IsNotExist(err) {
		return fmt.Errorf("backup directory not found: %s", b.RelPathKey)
	}

	plog.Notice("EXTRACT", "path", b.RelPathKey)
	if err := t.extractBackup(absToExtractPath, t.absExtractTargetPath); err != nil {
		t.metrics.AddArchivesFailed(1)
		return err
	}
	t.metrics.AddArchivesExtracted(1)

	plog.Notice("EXTRACTED", "path", b.RelPathKey)
	return nil
}

// extractBackup creates the extracts the archive of the backup's contents to the target path
func (t *extractTask) extractBackup(absToExtractPath, absExtractTargetPath string) error {
	// The archive is named after its parent backup directory (e.g., "PGL_Backup_2023-10-27...zip").
	archiveFileName := filepath.Base(absToExtractPath) + "." + t.format.String()
	absArchiveFilePath := util.DenormalizePath(filepath.Join(absToExtractPath, archiveFileName))

	extractor, err := newExtractor(t.format)
	if err != nil {
		return err
	}

	if err := extractor.Extract(t.ctx, absArchiveFilePath, absExtractTargetPath, t.ioBufferPool, t.metrics, t.overwriteBehavior); err != nil {
		return err
	}
	return nil
}
