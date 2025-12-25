package pathretentionmetrics

import (
	"sync/atomic"
	"time"

	"pixelgardenlabs.io/pgl-backup/pkg/plog"
)

// Metrics defines the interface for collecting and reporting retention statistics.
type Metrics interface {
	AddBackupsDeleted(n int64)
	AddBackupsFailed(n int64)
	LogSummary(msg string)
	StartProgress(msg string, interval time.Duration)
	StopProgress()
}

// RetentionMetrics holds the atomic counters for tracking the retention operation's progress.
type RetentionMetrics struct {
	BackupsDeleted atomic.Int64
	BackupsFailed  atomic.Int64

	stopChan chan struct{}
}

func (m *RetentionMetrics) AddBackupsDeleted(n int64) { m.BackupsDeleted.Add(n) }
func (m *RetentionMetrics) AddBackupsFailed(n int64)  { m.BackupsFailed.Add(n) }

func (m *RetentionMetrics) StartProgress(msg string, interval time.Duration) {
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

func (m *RetentionMetrics) StopProgress() {
	if m.stopChan != nil {
		close(m.stopChan)
	}
}

func (m *RetentionMetrics) LogSummary(msg string) {
	plog.Info(msg,
		"backups_deleted", m.BackupsDeleted.Load(),
		"backups_failed", m.BackupsFailed.Load(),
	)
}

// NoopMetrics is an implementation of the Metrics interface that performs no operations.
type NoopMetrics struct{}

func (m *NoopMetrics) AddBackupsDeleted(n int64)                        {}
func (m *NoopMetrics) AddBackupsFailed(n int64)                         {}
func (m *NoopMetrics) LogSummary(msg string)                            {}
func (m *NoopMetrics) StartProgress(msg string, interval time.Duration) {}
func (m *NoopMetrics) StopProgress()                                    {}

var _ Metrics = (*RetentionMetrics)(nil)
var _ Metrics = (*NoopMetrics)(nil)
