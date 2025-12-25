package pathretentionmetrics

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"pixelgardenlabs.io/pgl-backup/pkg/plog"
)

func TestRetentionMetrics_Adders(t *testing.T) {
	t.Run("correctly increments all counters", func(t *testing.T) {
		m := &RetentionMetrics{}

		m.AddBackupsDeleted(5)
		m.AddBackupsFailed(2)

		if got := m.BackupsDeleted.Load(); got != 5 {
			t.Errorf("expected BackupsDeleted to be 5, got %d", got)
		}
		if got := m.BackupsFailed.Load(); got != 2 {
			t.Errorf("expected BackupsFailed to be 2, got %d", got)
		}
	})
}

func TestRetentionMetrics_Log(t *testing.T) {
	t.Run("logs the correct summary values", func(t *testing.T) {
		// --- Setup: Redirect plog output to capture log output ---
		var logBuf bytes.Buffer
		plog.SetOutput(&logBuf)
		t.Cleanup(func() { plog.SetOutput(os.Stderr) }) // Restore original output after test.

		// --- Act ---
		m := &RetentionMetrics{}
		m.AddBackupsDeleted(10)
		m.AddBackupsFailed(3)
		m.LogSummary("Test Retention Summary")

		// --- Assert ---
		output := logBuf.String()

		if !strings.Contains(output, "msg=\"Test Retention Summary\"") {
			t.Errorf("expected log output to contain 'msg=\"Test Retention Summary\"', but it didn't. Got: %s", output)
		}
		if !strings.Contains(output, "backups_deleted=10") {
			t.Errorf("expected log output to contain 'backups_deleted=10', but it didn't. Got: %s", output)
		}
		if !strings.Contains(output, "backups_failed=3") {
			t.Errorf("expected log output to contain 'backups_failed=3', but it didn't. Got: %s", output)
		}
	})
}

func TestNoopMetrics(t *testing.T) {
	t.Run("all methods execute without panicking", func(t *testing.T) {
		m := &NoopMetrics{}

		defer func() {
			if r := recover(); r != nil {
				t.Errorf("NoopMetrics method panicked: %v", r)
			}
		}()

		m.AddBackupsDeleted(1)
		m.AddBackupsFailed(1)
		m.LogSummary("noop test")
		m.StartProgress("noop", 0)
		m.StopProgress()
	})
}
