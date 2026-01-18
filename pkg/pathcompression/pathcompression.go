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
	"bufio"
	"context"
	"io"
	"sync"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/pathcompressionmetrics"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

type PathCompressor struct {
	ioWriterPool *sync.Pool
	ioBufferPool *sync.Pool
	numWorkers   int
}

// NewPathCompressor creates a new PathCompressor with the given configuration.
func NewPathCompressor(bufferSizeKB int, numWorkers int) *PathCompressor {
	bufferSize := bufferSizeKB * 1024
	return &PathCompressor{
		numWorkers: numWorkers,
		ioWriterPool: &sync.Pool{
			New: func() interface{} {
				return bufio.NewWriterSize(io.Discard, bufferSize)
			},
		},
		ioBufferPool: &sync.Pool{
			New: func() interface{} {
				b := make([]byte, bufferSize)
				return &b
			},
		},
	}
}

// Compress processes the specific list of paths provided by the engine.
func (c *PathCompressor) Compress(ctx context.Context, absTargetBasePath, relContentPathKey string, toCompress []metafile.MetafileInfo, p *Plan, timestampUTC time.Time) error {

	// Check for cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if len(toCompress) == 0 {
		if p.DryRun {
			plog.Debug("[DRY RUN] No backups need compressing")
		} else {
			plog.Debug("No backups need compressing")
		}
		return nil
	}

	var m pathcompressionmetrics.Metrics
	if p.Metrics {
		m = &pathcompressionmetrics.CompressionMetrics{}
	} else {
		// Use the No-op implementation if metrics are disabled.
		m = &pathcompressionmetrics.NoopMetrics{}
	}

	t := &task{
		PathCompressor:    c, // Just pass the compressor pointer
		absTargetBasePath: absTargetBasePath,
		relContentPathKey: relContentPathKey,
		ctx:               ctx,
		format:            p.Format,
		toCompress:        toCompress,
		timestampUTC:      timestampUTC,
		metrics:           m,
		dryRun:            p.DryRun,
		compressTasksChan: make(chan metafile.MetafileInfo, c.numWorkers*2),
	}
	return t.execute()
}
