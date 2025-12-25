package pathcompressionmetrics

import (
	"fmt"
	"sync/atomic"
	"time"

	"pixelgardenlabs.io/pgl-backup/pkg/plog"
)

// Metrics defines the interface for collecting and reporting synchronization statistics.
type Metrics interface {
	AddArchivesCreated(n int64)
	AddArchivesFailed(n int64)
	AddOriginalBytes(n int64)
	AddCompressedBytes(n int64)
	AddEntriesProcessed(n int64)
	LogSummary(msg string)
	StartProgress(msg string, interval time.Duration)
	StopProgress()
}

// CompressionMetrics holds the atomic counters for tracking the compression operation's progress.
// It is the concrete implementation of the Metrics interface.
type CompressionMetrics struct {
	ArchivesCreated  atomic.Int64
	ArchivesFailed   atomic.Int64
	OriginalBytes    atomic.Int64
	CompressedBytes  atomic.Int64
	EntriesProcessed atomic.Int64

	stopChan chan struct{}
}

func (m *CompressionMetrics) AddArchivesCreated(n int64)  { m.ArchivesCreated.Add(n) }
func (m *CompressionMetrics) AddArchivesFailed(n int64)   { m.ArchivesFailed.Add(n) }
func (m *CompressionMetrics) AddOriginalBytes(n int64)    { m.OriginalBytes.Add(n) }
func (m *CompressionMetrics) AddCompressedBytes(n int64)  { m.CompressedBytes.Add(n) }
func (m *CompressionMetrics) AddEntriesProcessed(n int64) { m.EntriesProcessed.Add(n) }

func (m *CompressionMetrics) StartProgress(msg string, interval time.Duration) {
	m.stopChan = make(chan struct{})
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				m.LogSummary(msg)
			case <-m.stopChan:
				return
			}
		}
	}()
}

func (m *CompressionMetrics) StopProgress() {
	if m.stopChan != nil {
		close(m.stopChan)
	}
}

// LogSummary logs the current state of the metrics.
// This can be called by a background ticker or at the end of the run.
func (m *CompressionMetrics) LogSummary(msg string) {
	orig := m.OriginalBytes.Load()
	comp := m.CompressedBytes.Load()

	// Calculate compression ratio (avoid division by zero)
	var ratio float64
	if orig > 0 {
		ratio = float64(comp) / float64(orig) * 100.0
	}

	plog.Info(msg,
		"entries_processed", m.EntriesProcessed.Load(),
		"archives_created", m.ArchivesCreated.Load(),
		"archives_failed", m.ArchivesFailed.Load(),
		"original_bytes", fmt.Sprintf("%d", orig),
		"compressed_bytes", fmt.Sprintf("%d", comp),
		"ratio_pct", fmt.Sprintf("%.2f%%", ratio),
	)
}

// NoopMetrics is an implementation of the Metrics interface that performs no operations.
// It can be used to disable metrics collection without changing the calling code.
type NoopMetrics struct{}

func (m *NoopMetrics) AddArchivesCreated(n int64)                       {}
func (m *NoopMetrics) AddArchivesFailed(n int64)                        {}
func (m *NoopMetrics) AddOriginalBytes(n int64)                         {}
func (m *NoopMetrics) AddCompressedBytes(n int64)                       {}
func (m *NoopMetrics) AddEntriesProcessed(n int64)                      {}
func (m *NoopMetrics) LogSummary(msg string)                            {}
func (m *NoopMetrics) StartProgress(msg string, interval time.Duration) {}
func (m *NoopMetrics) StopProgress()                                    {}

// Statically assert that our types implement the interface.
var _ Metrics = (*CompressionMetrics)(nil)
var _ Metrics = (*NoopMetrics)(nil)
