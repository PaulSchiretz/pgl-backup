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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/hints"
	"github.com/paulschiretz/pgl-backup/pkg/limiter"
	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

var ErrDisabled = hints.New("compression is disabled")
var ErrNothingToCompress = hints.New("nothing to compress")
var ErrNothingToExtract = hints.New("nothing to extract")

type PathCompressor struct {
	ioBufferPool *sync.Pool
	ioBufferSize int64

	readAheadLimiter *limiter.Memory
	readAheadLimit   int64

	numCompressWorkers int
}

// NewPathCompressor creates a new PathCompressor with the given configuration.
func NewPathCompressor(bufferSizeKB, readAheadLimitKB int64, numCompressWorkers int) *PathCompressor {
	ioBufferSize := bufferSizeKB * 1024
	readAheadLimit := readAheadLimitKB * 1024
	return &PathCompressor{
		ioBufferPool: &sync.Pool{
			New: func() any {
				// Allocate a slice with a specific capacity
				b := make([]byte, ioBufferSize)
				return &b // We store a pointer to the slice header
			},
		},
		ioBufferSize:       ioBufferSize,
		readAheadLimiter:   limiter.NewMemory(readAheadLimit, ioBufferSize),
		readAheadLimit:     readAheadLimit,
		numCompressWorkers: numCompressWorkers,
	}
}

// Compress processes the specific list of paths provided by the engine.
func (c *PathCompressor) Compress(ctx context.Context, absBasePath, relContentPathKey string, toCompress metafile.MetafileInfo, p *CompressPlan, timestampUTC time.Time) error {

	if !p.Enabled {
		plog.Debug("Compression is disabled, skipping compress")
		return ErrDisabled
	}

	// Check for cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if toCompress.RelPathKey == "" {
		return ErrNothingToCompress
	}

	absToCompressPath := util.DenormalizePath(filepath.Join(absBasePath, toCompress.RelPathKey))
	absToCompressContentPath := util.DenormalizePath(filepath.Join(absToCompressPath, relContentPathKey))

	var m Metrics
	if p.Metrics {
		m = &CompressionMetrics{}
	} else {
		// Use the No-op implementation if metrics are disabled.
		m = &NoopMetrics{}
	}

	t := &compressTask{
		PathCompressor:           c, // Just pass the compressor pointer
		absToCompressPath:        absToCompressPath,
		absToCompressContentPath: absToCompressContentPath,
		ctx:                      ctx,
		format:                   p.Format,
		level:                    p.Level,
		timestampUTC:             timestampUTC,
		metrics:                  m,
		dryRun:                   p.DryRun,
	}

	err := t.execute()
	if err != nil {
		return err
	}

	// In dryRun mode we exit here
	if t.dryRun {
		plog.Notice("COMPRESSED", "source", absToCompressPath)
		return nil
	}

	// 1. On successful archive creation, we update the metafile.
	toCompress.Metadata.IsCompressed = true
	toCompress.Metadata.CompressionFormat = t.format.String()
	if writeErr := metafile.Write(absToCompressPath, &toCompress.Metadata); writeErr != nil {
		plog.Error("Failed to write updated metafile after compression success. Original content has been preserved.", "path", absToCompressPath, "error", writeErr)
		return fmt.Errorf("failed to update metafile: %w", writeErr)
	}

	// 2. Only after the metafile is successfully updated we remove the original content.
	if err := os.RemoveAll(absToCompressContentPath); err != nil {
		plog.Error("Failed to remove original content directory after successful compression. Manual cleanup may be required.", "path", absToCompressPath, "error", err)
	}

	plog.Notice("COMPRESSED", "source", absToCompressPath)
	return nil
}

// Extract extracts an archive to a target directory.
// It uses the configured buffer pool for efficient I/O.
func (c *PathCompressor) Extract(ctx context.Context, absBasePath string, toExtract metafile.MetafileInfo, absExtractTargetPath string, p *ExtractPlan, timestampUTC time.Time) error {

	if !p.Enabled {
		plog.Debug("Extraction is disabled, skipping extract")
		return ErrDisabled
	}

	// Check for cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if toExtract.RelPathKey == "" {
		return ErrNothingToExtract
	}

	format, err := c.determineCompressionFormat(absBasePath, toExtract)
	if err != nil {
		return fmt.Errorf("failed to determine format for extraction: %w", err)
	}

	absToExtractPath := util.DenormalizePath(filepath.Join(absBasePath, toExtract.RelPathKey))

	var m Metrics
	if p.Metrics {
		m = &CompressionMetrics{}
	} else {
		m = &NoopMetrics{}
	}

	t := &extractTask{
		PathCompressor:       c,
		ctx:                  ctx,
		format:               format,
		absToExtractPath:     absToExtractPath,
		absExtractTargetPath: absExtractTargetPath,
		overwriteBehavior:    p.OverwriteBehavior,
		modTimeWindow:        p.ModTimeWindow,
		timestampUTC:         timestampUTC,
		metrics:              m,
		dryRun:               p.DryRun,
	}

	err = t.execute()
	if err != nil {
		return err
	}

	plog.Notice("EXTRACTED", "source", absToExtractPath)
	return nil
}

func (c *PathCompressor) determineCompressionFormat(absBasePath string, fileInfo metafile.MetafileInfo) (Format, error) {

	absFilePath := util.DenormalizePath(filepath.Join(absBasePath, fileInfo.RelPathKey))

	// Determine format: prefer metadata, then file existence
	// Fetch compression format from metadata
	if fileInfo.Metadata.CompressionFormat != "" {
		parsed, err := ParseFormat(fileInfo.Metadata.CompressionFormat)
		if err == nil {
			return parsed, nil
		}
		plog.Warn("Invalid compression format in metadata, attempting auto-detection", "format", fileInfo.Metadata.CompressionFormat, "error", err)
	}

	// Fetch compression format by iterating through extensions
	baseName := filepath.Base(absFilePath)
	for _, f := range []Format{Zip, TarGz, TarZst} {
		candidatePath := util.DenormalizePath(filepath.Join(absFilePath, baseName+"."+f.String()))
		if _, err := os.Stat(candidatePath); err == nil {
			plog.Debug("Auto-detected compression format from file extension", "format", f)
			return f, nil
		}
	}

	return "", fmt.Errorf("unable to determine compression format for %s", absFilePath)
}
