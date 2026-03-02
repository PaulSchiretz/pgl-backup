package patharchive

import (
	"sync/atomic"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

// Metrics defines the interface for collecting and reporting move statistics.
type Metrics interface {
	AddBackupsMoved(n int64)
	LogSummary(msg string)
	StartProgress(msg string, interval time.Duration)
	StopProgress()
}

// ArchiveMetrics holds the atomic counters for tracking the move operation's progress.
type ArchiveMetrics struct {
	BackupsMoved atomic.Int64
	stopChan     chan struct{}
	startTime    time.Time
}

func (m *ArchiveMetrics) AddBackupsMoved(n int64) { m.BackupsMoved.Add(n) }

func (m *ArchiveMetrics) StartProgress(msg string, interval time.Duration) {
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

func (m *ArchiveMetrics) StopProgress() {
	if m.stopChan != nil {
		close(m.stopChan)
	}
}

func (m *ArchiveMetrics) LogSummary(msg string) {
	duration := time.Duration(0)
	if !m.startTime.IsZero() {
		duration = time.Since(m.startTime)
	}

	plog.Info(msg,
		"backups_moved", m.BackupsMoved.Load(),
		"duration", duration.Round(time.Millisecond),
	)
}

// NoopMetrics is an implementation of the Metrics interface that performs no operations.
type NoopMetrics struct{}

func (m *NoopMetrics) AddBackupsMoved(n int64)                          {}
func (m *NoopMetrics) LogSummary(msg string)                            {}
func (m *NoopMetrics) StartProgress(msg string, interval time.Duration) {}
func (m *NoopMetrics) StopProgress()                                    {}

var _ Metrics = (*ArchiveMetrics)(nil)
var _ Metrics = (*NoopMetrics)(nil)
