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
	"os"
	"sync"

	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/pathcompressionmetrics"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

type PathCompressionManager struct {
	ioWriterPool   *sync.Pool
	ioBufferPool   *sync.Pool
	metricsEnabled bool
	numWorkers     int
	contentSubDir  string
	dryRun         bool
}

// CompressionManager defines the interface for a component that applies a compression policy to backups.
type CompressionManager interface {
	Compress(ctx context.Context, absPaths []string, format Format) error
}

// Statically assert that *PathCompressionManager implements the CompressionManager interface.
var _ CompressionManager = (*PathCompressionManager)(nil)

type Config struct {
	MetricsEnabled bool
	ContentSubDir  string
	DryRun         bool
	BufferSizeKB   int
	NumWorkers     int
}

// NewPathCompressionManager creates a new PathCompressionManager with the given configuration.
func NewPathCompressionManager(cfg Config) *PathCompressionManager {
	bufferSize := cfg.BufferSizeKB * 1024

	return &PathCompressionManager{
		metricsEnabled: cfg.MetricsEnabled,
		numWorkers:     cfg.NumWorkers,
		contentSubDir:  cfg.ContentSubDir,
		dryRun:         cfg.DryRun,
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
func (c *PathCompressionManager) Compress(ctx context.Context, absBackupPaths []string, format Format) error {

	eligibleBackups := c.IdentifyEligibleBackups(ctx, absBackupPaths)
	if len(eligibleBackups) == 0 {
		if c.dryRun {
			plog.Debug("[DRY RUN] No backups need compressing")
		} else {
			plog.Debug("No backups need compressing")
		}
		return nil
	}

	var m pathcompressionmetrics.Metrics
	if c.metricsEnabled {
		m = &pathcompressionmetrics.CompressionMetrics{}
	} else {
		// Use the No-op implementation if metrics are disabled.
		m = &pathcompressionmetrics.NoopMetrics{}
	}

	j := &job{
		PathCompressionManager: c, // Just pass the manager pointer once
		ctx:                    ctx,
		format:                 format,
		metrics:                m,
		eligibleBackups:        eligibleBackups,
		compressTasksChan:      make(chan metafile.MetafileInfo, c.numWorkers*2),
	}
	return j.execute()
}

// IdentifyEligibleBackups scans the provided absolute backupPaths and returns a list of those
// that have not yet been compressed, based on their metadata.
func (c *PathCompressionManager) IdentifyEligibleBackups(ctx context.Context, absBackupPaths []string) []metafile.MetafileInfo {
	var eligible []metafile.MetafileInfo
	for _, absBackupPath := range absBackupPaths {
		select {
		case <-ctx.Done():
			return nil
		default:
			// Check if the backup directory still exists. It might have been removed by retention policy.
			if _, err := os.Stat(absBackupPath); os.IsNotExist(err) {
				continue
			}

			// Read metadata
			metadata, err := metafile.Read(absBackupPath)
			if err != nil {
				plog.Warn("Skipping compression check; cannot read metadata", "path", absBackupPath, "reason", err)
				continue
			}

			if !metadata.IsCompressed {
				// Store full path in RelPathKey.... FIX THIS!
				eligible = append(eligible, metafile.MetafileInfo{RelPathKey: absBackupPath, Metadata: metadata})
			}
		}
	}
	return eligible
}
