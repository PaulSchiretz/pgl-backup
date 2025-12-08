package metrics

import (
	"sync/atomic"

	"pixelgardenlabs.io/pgl-backup/pkg/plog"
)

// Metrics defines the interface for collecting and reporting synchronization statistics.
type Metrics interface {
	AddFilesCopied(n int64)
	AddFilesDeleted(n int64)
	AddFilesExcluded(n int64)
	AddFilesUpToDate(n int64)
	AddDirsCreated(n int64)
	AddDirsDeleted(n int64)
	AddDirsExcluded(n int64)
	Log()
}

// SyncMetrics holds the atomic counters for tracking the sync operation's progress.
// It is the concrete implementation of the Metrics interface.
type SyncMetrics struct {
	FilesCopied   atomic.Int64
	FilesDeleted  atomic.Int64
	FilesExcluded atomic.Int64
	FilesUpToDate atomic.Int64
	DirsCreated   atomic.Int64
	DirsDeleted   atomic.Int64
	DirsExcluded  atomic.Int64
}

func (m *SyncMetrics) AddFilesCopied(n int64)   { m.FilesCopied.Add(n) }
func (m *SyncMetrics) AddFilesDeleted(n int64)  { m.FilesDeleted.Add(n) }
func (m *SyncMetrics) AddFilesExcluded(n int64) { m.FilesExcluded.Add(n) }
func (m *SyncMetrics) AddFilesUpToDate(n int64) { m.FilesUpToDate.Add(n) }
func (m *SyncMetrics) AddDirsCreated(n int64)   { m.DirsCreated.Add(n) }
func (m *SyncMetrics) AddDirsDeleted(n int64)   { m.DirsDeleted.Add(n) }
func (m *SyncMetrics) AddDirsExcluded(n int64)  { m.DirsExcluded.Add(n) }

// Log prints a summary of the sync operation.
func (m *SyncMetrics) Log() {
	plog.Info("SUM",
		"filesCopied", m.FilesCopied.Load(),
		"filesUpToDate", m.FilesUpToDate.Load(),
		"filesDeleted", m.FilesDeleted.Load(),
		"filesExcluded", m.FilesExcluded.Load(),
		"dirsCreated", m.DirsCreated.Load(),
		"dirsDeleted", m.DirsDeleted.Load(),
		"dirsExcluded", m.DirsExcluded.Load(),
	)
}

// NoopMetrics is an implementation of the Metrics interface that performs no operations.
// It can be used to disable metrics collection without changing the calling code.
type NoopMetrics struct{}

func (m *NoopMetrics) AddFilesCopied(n int64)   {}
func (m *NoopMetrics) AddFilesDeleted(n int64)  {}
func (m *NoopMetrics) AddFilesExcluded(n int64) {}
func (m *NoopMetrics) AddFilesUpToDate(n int64) {}
func (m *NoopMetrics) AddDirsCreated(n int64)   {}
func (m *NoopMetrics) AddDirsDeleted(n int64)   {}
func (m *NoopMetrics) AddDirsExcluded(n int64)  {}
func (m *NoopMetrics) Log()                     {}

// Statically assert that our types implement the interface.
var _ Metrics = (*SyncMetrics)(nil)
var _ Metrics = (*NoopMetrics)(nil)
