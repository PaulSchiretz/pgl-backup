package pathcompressionmetrics

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

func TestCompressionMetrics_Adders(t *testing.T) {
	t.Run("correctly increments all counters", func(t *testing.T) {
		m := &CompressionMetrics{}

		m.AddArchivesCreated(5)
		m.AddArchivesFailed(2)
		m.AddOriginalBytes(1000)
		m.AddCompressedBytes(500)
		m.AddEntriesProcessed(50)

		if got := m.ArchivesCreated.Load(); got != 5 {
			t.Errorf("expected ArchivesCreated to be 5, got %d", got)
		}
		if got := m.ArchivesFailed.Load(); got != 2 {
			t.Errorf("expected ArchivesFailed to be 2, got %d", got)
		}
		if got := m.OriginalBytes.Load(); got != 1000 {
			t.Errorf("expected OriginalBytes to be 1000, got %d", got)
		}
		if got := m.CompressedBytes.Load(); got != 500 {
			t.Errorf("expected CompressedBytes to be 500, got %d", got)
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
		m := &CompressionMetrics{}
		m.AddArchivesCreated(10)
		m.AddOriginalBytes(200)
		m.AddCompressedBytes(100) // 50% ratio
		m.LogSummary("Test Compression Summary")

		// --- Assert ---
		output := logBuf.String()

		if !strings.Contains(output, "msg=\"Test Compression Summary\"") {
			t.Errorf("expected log output to contain 'msg=\"Test Compression Summary\"', but it didn't. Got: %s", output)
		}
		if !strings.Contains(output, "archives_created=10") {
			t.Errorf("expected log output to contain 'archives_created=10', but it didn't. Got: %s", output)
		}
		if !strings.Contains(output, "original_bytes=200") {
			t.Errorf("expected log output to contain 'original_bytes=200', but it didn't. Got: %s", output)
		}
		if !strings.Contains(output, "compressed_bytes=100") {
			t.Errorf("expected log output to contain 'compressed_bytes=100', but it didn't. Got: %s", output)
		}
		// 100 / 200 * 100.0 = 50.00%
		if !strings.Contains(output, "ratio_pct=50.00%") {
			t.Errorf("expected log output to contain 'ratio_pct=50.00%%', but it didn't. Got: %s", output)
		}
	})

	t.Run("handles zero original bytes (division by zero check)", func(t *testing.T) {
		var logBuf bytes.Buffer
		plog.SetOutput(&logBuf)
		t.Cleanup(func() { plog.SetOutput(os.Stderr) })

		m := &CompressionMetrics{}
		// No bytes added
		m.LogSummary("Zero Check")

		output := logBuf.String()
		if !strings.Contains(output, "ratio_pct=0.00%") {
			t.Errorf("expected log output to contain 'ratio_pct=0.00%%' for zero bytes, but it didn't. Got: %s", output)
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

		m.AddArchivesCreated(1)
		m.AddArchivesFailed(1)
		m.AddOriginalBytes(1)
		m.AddCompressedBytes(1)
		m.AddEntriesProcessed(1)
		m.LogSummary("noop test")
		m.StartProgress("noop", 0)
		m.StopProgress()
	})
}
