package patharchivemetrics

import (
	"sync/atomic"
	"time"

	"pixelgardenlabs.io/pgl-backup/pkg/plog"
)

// Metrics defines the interface for collecting and reporting archive statistics.
type Metrics interface {
	AddArchivesCreated(n int64)
	LogSummary(msg string)
	StartProgress(msg string, interval time.Duration)
	StopProgress()
}

// ArchiveMetrics holds the atomic counters for tracking the archive operation's progress.
type ArchiveMetrics struct {
	ArchivesCreated atomic.Int64
	stopChan        chan struct{}
}

func (m *ArchiveMetrics) AddArchivesCreated(n int64) { m.ArchivesCreated.Add(n) }

func (m *ArchiveMetrics) StartProgress(msg string, interval time.Duration) {
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
	plog.Info(msg,
		"archives_created", m.ArchivesCreated.Load(),
	)
}

// NoopMetrics is an implementation of the Metrics interface that performs no operations.
type NoopMetrics struct{}

func (m *NoopMetrics) AddArchivesCreated(n int64)                       {}
func (m *NoopMetrics) LogSummary(msg string)                            {}
func (m *NoopMetrics) StartProgress(msg string, interval time.Duration) {}
func (m *NoopMetrics) StopProgress()                                    {}

var _ Metrics = (*ArchiveMetrics)(nil)
var _ Metrics = (*NoopMetrics)(nil)
