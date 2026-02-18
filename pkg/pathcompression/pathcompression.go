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
	"sync"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/hints"
	"github.com/paulschiretz/pgl-backup/pkg/limiter"
	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

var ErrDisabled = hints.New("compression is disabled")
var ErrNothingToCompress = hints.New("nothing to compress")
var ErrNothingToExtract = hints.New("nothing to extract")

type PathCompressor struct {
	ioBufferPool *sync.Pool
	ioBufferSize int64

	readAheadLimiter   *limiter.Memory
	readAheadLimitSize int64

	numCompressWorkers int
}

// NewPathCompressor creates a new PathCompressor with the given configuration.
func NewPathCompressor(bufferSizeKB, readAheadLimitKB int64, numCompressWorkers int) *PathCompressor {
	ioBufferSize := bufferSizeKB * 1024
	readAheadLimitSize := readAheadLimitKB * 1024
	return &PathCompressor{
		ioBufferPool: &sync.Pool{
			New: func() any {
				// Allocate a slice with a specific capacity
				b := make([]byte, ioBufferSize)
				return &b // We store a pointer to the slice header
			},
		},
		ioBufferSize:       ioBufferSize,
		readAheadLimiter:   limiter.NewMemory(readAheadLimitSize, ioBufferSize),
		readAheadLimitSize: readAheadLimitSize,
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

	var m Metrics
	if p.Metrics {
		m = &CompressionMetrics{}
	} else {
		// Use the No-op implementation if metrics are disabled.
		m = &NoopMetrics{}
	}

	t := &compressTask{
		PathCompressor:    c, // Just pass the compressor pointer
		absBasePath:       absBasePath,
		relContentPathKey: relContentPathKey,
		ctx:               ctx,
		format:            p.Format,
		level:             p.Level,
		toCompress:        toCompress,
		timestampUTC:      timestampUTC,
		metrics:           m,
		dryRun:            p.DryRun,
	}
	return t.execute()
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

	var m Metrics
	if p.Metrics {
		m = &CompressionMetrics{}
	} else {
		m = &NoopMetrics{}
	}

	t := &extractTask{
		PathCompressor:       c,
		ctx:                  ctx,
		absBasePath:          absBasePath,
		toExtract:            toExtract,
		absExtractTargetPath: absExtractTargetPath,
		overwriteBehavior:    p.OverwriteBehavior,
		modTimeWindow:        p.ModTimeWindow,
		timestampUTC:         timestampUTC,
		metrics:              m,
		dryRun:               p.DryRun,
	}
	return t.execute()
}
