package pathcompression

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// extractTask holds the mutable state for a single decompression execution.
type extractTask struct {
	*PathCompressor
	ctx                  context.Context
	absBasePath          string
	toExtract            metafile.MetafileInfo
	absExtractTargetPath string
	overwriteBehavior    OverwriteBehavior
	modTimeWindow        time.Duration
	timestampUTC         time.Time
	metrics              Metrics
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

	absToExtractPath := util.DenormalizePath(filepath.Join(t.absBasePath, b.RelPathKey))
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
	format, err := t.determineCompressionFormat(absToExtractPath)
	if err != nil {
		return err
	}

	// The archive is named after its parent backup directory (e.g., "PGL_Backup_2023-10-27...zip").
	archiveFileName := filepath.Base(absToExtractPath) + "." + format.String()
	absArchiveFilePath := util.DenormalizePath(filepath.Join(absToExtractPath, archiveFileName))

	var extr extractor
	switch format {
	case Zip:
		extr = newZipExtractor(format, t.ioBufferPool, t.metrics, t.overwriteBehavior, t.modTimeWindow)
	case TarGz, TarZst:
		extr = newTarExtractor(format, t.ioBufferPool, t.metrics, t.overwriteBehavior, t.modTimeWindow)
	default:
		return fmt.Errorf("unsupported format: %s", format)
	}

	if err := extr.Extract(t.ctx, absArchiveFilePath, absExtractTargetPath); err != nil {
		return err
	}
	return nil
}

func (t *extractTask) determineCompressionFormat(absToExtractPath string) (Format, error) {
	// Determine format: prefer metadata, then file existence

	// Fetch compression format from metadata
	if t.toExtract.Metadata.CompressionFormat != "" {
		parsed, err := ParseFormat(t.toExtract.Metadata.CompressionFormat)
		if err == nil {
			return parsed, nil
		}
		plog.Warn("Invalid compression format in metadata, attempting auto-detection", "format", t.toExtract.Metadata.CompressionFormat, "error", err)
	}

	// Fetch compression format by iterating through extensions
	baseName := filepath.Base(absToExtractPath)
	for _, f := range []Format{Zip, TarGz, TarZst} {
		candidatePath := util.DenormalizePath(filepath.Join(absToExtractPath, baseName+"."+f.String()))
		if _, err := os.Stat(candidatePath); err == nil {
			plog.Debug("Auto-detected compression format from file extension", "format", f)
			return f, nil
		}
	}

	return "", fmt.Errorf("unable to determine compression format for %s", absToExtractPath)
}
