package pathsync

import (
	"sync/atomic"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// Metrics defines the interface for collecting and reporting synchronization statistics.
type Metrics interface {
	AddFilesCopied(n int64)
	AddFilesDeleted(n int64)
	AddFilesExcluded(n int64)
	AddFilesUpToDate(n int64)
	AddBytesRead(n int64)
	AddBytesWritten(n int64)
	AddDirsCreated(n int64)
	AddDirsDeleted(n int64)
	AddDirsExcluded(n int64)
	AddEntriesProcessed(n int64)
	LogSummary(msg string)

	StartProgress(msg string, interval time.Duration)
	StopProgress()
}

// SyncMetrics holds the atomic counters for tracking the sync operation's progress.
// It is the concrete implementation of the Metrics interface.
type SyncMetrics struct {
	FilesCopied      atomic.Int64
	FilesDeleted     atomic.Int64
	FilesExcluded    atomic.Int64
	FilesUpToDate    atomic.Int64
	BytesRead        atomic.Int64
	BytesWritten     atomic.Int64
	DirsCreated      atomic.Int64
	DirsDeleted      atomic.Int64
	DirsExcluded     atomic.Int64
	EntriesProcessed atomic.Int64

	stopChan  chan struct{}
	startTime time.Time
}

func (m *SyncMetrics) AddFilesCopied(n int64)      { m.FilesCopied.Add(n) }
func (m *SyncMetrics) AddFilesDeleted(n int64)     { m.FilesDeleted.Add(n) }
func (m *SyncMetrics) AddFilesExcluded(n int64)    { m.FilesExcluded.Add(n) }
func (m *SyncMetrics) AddFilesUpToDate(n int64)    { m.FilesUpToDate.Add(n) }
func (m *SyncMetrics) AddBytesRead(n int64)        { m.BytesRead.Add(n) }
func (m *SyncMetrics) AddBytesWritten(n int64)     { m.BytesWritten.Add(n) }
func (m *SyncMetrics) AddDirsCreated(n int64)      { m.DirsCreated.Add(n) }
func (m *SyncMetrics) AddDirsDeleted(n int64)      { m.DirsDeleted.Add(n) }
func (m *SyncMetrics) AddDirsExcluded(n int64)     { m.DirsExcluded.Add(n) }
func (m *SyncMetrics) AddEntriesProcessed(n int64) { m.EntriesProcessed.Add(n) }

func (m *SyncMetrics) StartProgress(msg string, interval time.Duration) {
	m.startTime = time.Now()
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

func (m *SyncMetrics) StopProgress() {
	if m.stopChan != nil {
		close(m.stopChan)
	}
}

// Log prints a summary of the sync operation with a custom message.
// This can be called by a background ticker or at the end of the run.
func (m *SyncMetrics) LogSummary(msg string) {
	duration := time.Duration(0)
	if !m.startTime.IsZero() {
		duration = time.Since(m.startTime)
	}

	plog.Info(msg,
		"entries_processed", m.EntriesProcessed.Load(),
		"bytes_read", util.ByteCountIEC(m.BytesRead.Load()),
		"bytes_written", util.ByteCountIEC(m.BytesWritten.Load()),
		"files_copied", m.FilesCopied.Load(),
		"files_uptodate", m.FilesUpToDate.Load(),
		"files_deleted", m.FilesDeleted.Load(),
		"files_excluded", m.FilesExcluded.Load(),
		"dirs_created", m.DirsCreated.Load(),
		"dirs_deleted", m.DirsDeleted.Load(),
		"dirs_excluded", m.DirsExcluded.Load(),
		"duration", duration.Round(time.Millisecond),
	)
}

// NoopMetrics is an implementation of the Metrics interface that performs no operations.
// It can be used to disable metrics collection without changing the calling code.
type NoopMetrics struct{}

func (m *NoopMetrics) AddFilesCopied(n int64)                           {}
func (m *NoopMetrics) AddFilesDeleted(n int64)                          {}
func (m *NoopMetrics) AddFilesExcluded(n int64)                         {}
func (m *NoopMetrics) AddFilesUpToDate(n int64)                         {}
func (m *NoopMetrics) AddBytesRead(n int64)                             {}
func (m *NoopMetrics) AddBytesWritten(n int64)                          {}
func (m *NoopMetrics) AddDirsCreated(n int64)                           {}
func (m *NoopMetrics) AddDirsDeleted(n int64)                           {}
func (m *NoopMetrics) AddDirsExcluded(n int64)                          {}
func (m *NoopMetrics) AddEntriesProcessed(n int64)                      {}
func (m *NoopMetrics) LogSummary(msg string)                            {}
func (m *NoopMetrics) StartProgress(msg string, interval time.Duration) {}
func (m *NoopMetrics) StopProgress()                                    {}

// Statically assert that our types implement the interface.
var _ Metrics = (*SyncMetrics)(nil)
var _ Metrics = (*NoopMetrics)(nil)
