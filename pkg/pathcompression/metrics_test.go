package pathcompression_test

import (
	"bytes"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/pathcompression"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

func TestCompressionMetrics_Adders(t *testing.T) {
	t.Run("correctly increments all counters", func(t *testing.T) {
		m := &pathcompression.CompressionMetrics{}

		m.AddArchivesCreated(5)
		m.AddArchivesExtracted(3)
		m.AddArchivesFailed(2)
		m.AddBytesRead(1000)
		m.AddBytesWritten(500)
		m.AddEntriesProcessed(50)

		if got := m.ArchivesCreated.Load(); got != 5 {
			t.Errorf("expected ArchivesCreated to be 5, got %d", got)
		}
		if got := m.ArchivesExtracted.Load(); got != 3 {
			t.Errorf("expected ArchivesExtracted to be 3, got %d", got)
		}
		if got := m.ArchivesFailed.Load(); got != 2 {
			t.Errorf("expected ArchivesFailed to be 2, got %d", got)
		}
		if got := m.BytesRead.Load(); got != 1000 {
			t.Errorf("expected BytesRead to be 1000, got %d", got)
		}
		if got := m.BytesWritten.Load(); got != 500 {
			t.Errorf("expected BytesWritten to be 500, got %d", got)
		}
		if got := m.EntriesProcessed.Load(); got != 50 {
			t.Errorf("expected EntriesProcessed to be 50, got %d", got)
		}
	})
}

func TestCompressionMetrics_Log(t *testing.T) {
	t.Run("logs the correct summary values and ratio", func(t *testing.T) {
		// --- Setup: Redirect plog output to capture log output ---
		var logBuf bytes.Buffer
		plog.SetOutput(&logBuf)
		t.Cleanup(func() { plog.SetOutput(os.Stderr) }) // Restore original output after test.

		// --- Act ---
		m := &pathcompression.CompressionMetrics{}
		m.AddArchivesCreated(10)
		m.AddArchivesExtracted(5)
		m.AddBytesRead(200)
		m.AddBytesWritten(100)             // 50% ratio
		m.StartProgress("Test", time.Hour) // Initialize startTime
		m.StopProgress()                   // Stop immediately to avoid leaks
		m.LogSummary("Test Compression Summary")

		// --- Assert ---
		output := logBuf.String()

		if !strings.Contains(output, "msg=\"Test Compression Summary\"") {
			t.Errorf("expected log output to contain 'msg=\"Test Compression Summary\"', but it didn't. Got: %s", output)
		}
		if !strings.Contains(output, "archives_created=10") {
			t.Errorf("expected log output to contain 'archives_created=10', but it didn't. Got: %s", output)
		}
		if !strings.Contains(output, "archives_extracted=5") {
			t.Errorf("expected log output to contain 'archives_extracted=5', but it didn't. Got: %s", output)
		}
		if !strings.Contains(output, "bytes_read=\"200 B\"") {
			t.Errorf("expected log output to contain 'bytes_read=\"200 B\"', but it didn't. Got: %s", output)
		}
		if !strings.Contains(output, "bytes_written=\"100 B\"") {
			t.Errorf("expected log output to contain 'bytes_written=\"100 B\"', but it didn't. Got: %s", output)
		}
		// 100 / 200 * 100.0 = 50.00%
		if !strings.Contains(output, "io_ratio_pct=50.00%") {
			t.Errorf("expected log output to contain 'io_ratio_pct=50.00%%', but it didn't. Got: %s", output)
		}
		if !strings.Contains(output, "duration=") {
			t.Errorf("expected log output to contain 'duration=', but it didn't. Got: %s", output)
		}
	})

	t.Run("handles zero original bytes (division by zero check)", func(t *testing.T) {
		var logBuf bytes.Buffer
		plog.SetOutput(&logBuf)
		t.Cleanup(func() { plog.SetOutput(os.Stderr) })

		m := &pathcompression.CompressionMetrics{}
		// No bytes added
		m.LogSummary("Zero Check")

		output := logBuf.String()
		if !strings.Contains(output, "io_ratio_pct=0.00%") {
			t.Errorf("expected log output to contain 'io_ratio_pct=0.00%%' for zero bytes, but it didn't. Got: %s", output)
		}
	})
}

func TestNoopMetrics(t *testing.T) {
	t.Run("all methods execute without panicking", func(t *testing.T) {
		m := &pathcompression.NoopMetrics{}

		defer func() {
			if r := recover(); r != nil {
				t.Errorf("NoopMetrics method panicked: %v", r)
			}
		}()

		m.AddArchivesCreated(1)
		m.AddArchivesExtracted(1)
		m.AddArchivesFailed(1)
		m.AddBytesRead(1)
		m.AddBytesWritten(1)
		m.AddEntriesProcessed(1)
		m.LogSummary("noop test")
		m.StartProgress("noop", 0)
		m.StopProgress()
	})
}
