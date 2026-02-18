package pathsync_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"bytes"

	"github.com/paulschiretz/pgl-backup/pkg/pathsync"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

func TestSyncMetrics_Adders(t *testing.T) {
	t.Run("correctly increments all counters", func(t *testing.T) {
		m := &pathsync.SyncMetrics{}

		m.AddFilesCopied(5)
		m.AddFilesDeleted(3)
		m.AddFilesExcluded(2)
		m.AddFilesUpToDate(10)
		m.AddBytesWritten(1024)
		m.AddDirsCreated(4)
		m.AddDirsDeleted(1)
		m.AddDirsExcluded(6)

		if got := m.FilesCopied.Load(); got != 5 {
			t.Errorf("expected FilesCopied to be 5, got %d", got)
		}
		if got := m.FilesDeleted.Load(); got != 3 {
			t.Errorf("expected FilesDeleted to be 3, got %d", got)
		}
		if got := m.FilesExcluded.Load(); got != 2 {
			t.Errorf("expected FilesExcluded to be 2, got %d", got)
		}
		if got := m.FilesUpToDate.Load(); got != 10 {
			t.Errorf("expected FilesUpToDate to be 10, got %d", got)
		}
		if got := m.BytesWritten.Load(); got != 1024 {
			t.Errorf("expected BytesWritten to be 1024, got %d", got)
		}
		if got := m.DirsCreated.Load(); got != 4 {
			t.Errorf("expected DirsCreated to be 4, got %d", got)
		}
		if got := m.DirsDeleted.Load(); got != 1 {
			t.Errorf("expected DirsDeleted to be 1, got %d", got)
		}
		if got := m.DirsExcluded.Load(); got != 6 {
			t.Errorf("expected DirsExcluded to be 6, got %d", got)
		}
	})
}

func TestSyncMetrics_Log(t *testing.T) {
	t.Run("logs the correct summary values", func(t *testing.T) {
		// --- Setup: Redirect plog output to capture log output ---
		var logBuf bytes.Buffer
		plog.SetOutput(&logBuf)
		t.Cleanup(func() { plog.SetOutput(os.Stderr) }) // Restore original output after test.

		// --- Act ---
		m := &pathsync.SyncMetrics{}
		m.AddFilesCopied(10)
		m.AddFilesUpToDate(20)
		m.AddBytesWritten(500)
		m.StartProgress("Test", time.Hour) // Initialize startTime
		m.StopProgress()                   // Stop immediately to avoid leaks
		m.LogSummary("Test Summary")

		// --- Assert ---
		output := logBuf.String()

		// --- Assert ---
		if !strings.Contains(output, "msg=\"Test Summary\"") {
			t.Errorf("expected log output to contain 'msg=\"Test Summary\"', but it didn't. Got: %s", output)
		}
		if !strings.Contains(output, "files_copied=10") {
			t.Errorf("expected log output to contain 'files_copied=10', but it didn't. Got: %s", output)
		}
		if !strings.Contains(output, "bytes_written=\"500 B\"") {
			t.Errorf("expected log output to contain 'bytes_written=\"500 B\"', but it didn't. Got: %s", output)
		}
		if !strings.Contains(output, "files_uptodate=20") {
			t.Errorf("expected log output to contain 'files_uptodate=20', but it didn't. Got: %s", output)
		}
		// Check a zero value to ensure it's also logged correctly
		if !strings.Contains(output, "files_deleted=0") {
			t.Errorf("expected log output to contain 'files_deleted=0', but it didn't. Got: %s", output)
		}
		if !strings.Contains(output, "duration=") {
			t.Errorf("expected log output to contain 'duration=', but it didn't. Got: %s", output)
		}
	})
}

func TestNoopMetrics(t *testing.T) {
	t.Run("all methods execute without panicking", func(t *testing.T) {
		// The purpose of this test is to simply call all methods on NoopMetrics
		// to ensure they are present and do not cause a runtime panic.
		// There are no values to assert, as the implementation is empty.
		m := &pathsync.NoopMetrics{}

		// Use a defer function with recover to explicitly check for panics.
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("NoopMetrics method panicked: %v", r)
			}
		}()

		m.AddFilesCopied(1)
		m.AddFilesDeleted(1)
		m.AddFilesExcluded(1)
		m.AddFilesUpToDate(1)
		m.AddBytesWritten(1)
		m.AddDirsCreated(1)
		m.AddDirsDeleted(1)
		m.AddDirsExcluded(1)
		m.LogSummary("noop test")
	})
}
