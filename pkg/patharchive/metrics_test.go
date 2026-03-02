package patharchive_test

import (
	"bytes"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/patharchive"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

func TestArchiveMetrics_Adders(t *testing.T) {
	t.Run("correctly increments counters", func(t *testing.T) {
		m := &patharchive.ArchiveMetrics{}
		m.AddBackupsMoved(5)
		if got := m.BackupsMoved.Load(); got != 5 {
			t.Errorf("expected BackupsMoved to be 5, got %d", got)
		}
	})
}

func TestArchiveMetrics_Log(t *testing.T) {
	t.Run("logs the correct summary values", func(t *testing.T) {
		var logBuf bytes.Buffer
		plog.SetOutput(&logBuf)
		t.Cleanup(func() { plog.SetOutput(os.Stderr) })

		m := &patharchive.ArchiveMetrics{}
		m.AddBackupsMoved(10)
		m.StartProgress("Test", time.Hour) // Initialize startTime
		m.StopProgress()                   // Stop immediately to avoid leaks
		m.LogSummary("Test Archive Summary")

		output := logBuf.String()
		if !strings.Contains(output, "msg=\"Test Archive Summary\"") {
			t.Errorf("expected log output to contain 'msg=\"Test Archive Summary\"', but it didn't. Got: %s", output)
		}
		if !strings.Contains(output, "backups_moved=10") {
			t.Errorf("expected log output to contain 'backups_moved=10', but it didn't. Got: %s", output)
		}
		if !strings.Contains(output, "duration=") {
			t.Errorf("expected log output to contain 'duration=', but it didn't. Got: %s", output)
		}
	})
}

func TestNoopMetrics(t *testing.T) {
	t.Run("all methods execute without panicking", func(t *testing.T) {
		m := &patharchive.NoopMetrics{}
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("NoopMetrics method panicked: %v", r)
			}
		}()
		m.AddBackupsMoved(1)
		m.LogSummary("noop test")
		m.StartProgress("noop", 0)
		m.StopProgress()
	})
}
