package patharchive

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/config"
	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

// createTestMetafile creates a directory and a metafile inside it for testing.
func createTestMetafile(t *testing.T, dirPath string, timestampUTC time.Time) {
	t.Helper()
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		t.Fatalf("failed to create test backup dir: %v", err)
	}

	metadata := metafile.MetafileContent{
		Version:      "test",
		TimestampUTC: timestampUTC,
		Source:       "/src",
	}

	if err := metafile.Write(dirPath, metadata); err != nil {
		t.Fatalf("failed to write metafile for test backup: %v", err)
	}
}

func TestShouldArchive(t *testing.T) {
	testCases := []struct {
		name                      string
		interval                  time.Duration
		currentBackupTimestampUTC time.Time
		currentTimestampUTC       time.Time
		location                  *time.Location
		shouldArchive             bool
	}{
		{
			name:                      "Hourly - Same Hour",
			interval:                  time.Hour,
			currentBackupTimestampUTC: time.Date(2023, 10, 26, 14, 10, 0, 0, time.UTC),
			currentTimestampUTC:       time.Date(2023, 10, 26, 14, 55, 0, 0, time.UTC),
			shouldArchive:             false,
		},
		{
			name:                      "Hourly - Next Hour",
			interval:                  time.Hour,
			currentBackupTimestampUTC: time.Date(2023, 10, 26, 14, 55, 0, 0, time.UTC),
			currentTimestampUTC:       time.Date(2023, 10, 26, 15, 05, 0, 0, time.UTC),
			shouldArchive:             true,
		},
		{
			name:                      "Daily - Same Day",
			interval:                  24 * time.Hour,
			currentBackupTimestampUTC: time.Date(2023, 10, 26, 10, 0, 0, 0, time.UTC),
			currentTimestampUTC:       time.Date(2023, 10, 26, 14, 0, 0, 0, time.UTC),
			shouldArchive:             false,
		},
		{
			name:                      "Daily - Crosses Midnight",
			interval:                  24 * time.Hour,
			currentBackupTimestampUTC: time.Date(2023, 10, 26, 14, 0, 0, 0, time.UTC),
			currentTimestampUTC:       time.Date(2023, 10, 26, 14, 0, 0, 0, time.UTC).Add(26 * time.Hour),
			shouldArchive:             true,
		},
		{
			name:     "Weekly - No Archive (Day 3)",
			interval: 7 * 24 * time.Hour,
			// Unix Epoch (1970-01-01) was a Thursday.
			// Oct 23, 2023 is a Monday.
			// Oct 25, 2023 is a Wednesday.
			// Both fall within the same 7-day bucket (Thursday to Wednesday).
			currentBackupTimestampUTC: time.Date(2023, 10, 23, 12, 0, 0, 0, time.UTC), // Day D
			currentTimestampUTC:       time.Date(2023, 10, 25, 12, 0, 0, 0, time.UTC), // Day D + 2
			shouldArchive:             false,
		},
		{
			name:     "Weekly - Archive (Day 7 Boundary)",
			interval: 7 * 24 * time.Hour,
			// Unix Epoch (1970-01-01) was a Thursday.
			// Oct 23, 2023 is a Monday.
			// Oct 30, 2023 is the following Monday.
			// They are in different 7-day buckets because the boundary (Thursday) was crossed on Oct 26.
			currentBackupTimestampUTC: time.Date(2023, 10, 23, 12, 0, 0, 0, time.UTC), // Day D (Bucket B)
			currentTimestampUTC:       time.Date(2023, 10, 30, 12, 0, 0, 0, time.UTC), // Day D + 7 (Bucket B + 1)
			shouldArchive:             true,
		},
		{
			name:     "Daily - DST Spring Forward (23h day)",
			interval: 24 * time.Hour,
			// March 12, 2023 was DST start in NY.
			// 2023-03-11 12:00 EST -> 2023-03-12 12:00 EDT is 23 hours.
			// We want to ensure this counts as a full day change.
			currentBackupTimestampUTC: time.Date(2023, 3, 11, 17, 0, 0, 0, time.UTC), // 12:00 EST
			currentTimestampUTC:       time.Date(2023, 3, 12, 16, 0, 0, 0, time.UTC), // 12:00 EDT (23h later)
			location:                  mustLoadLocation("America/New_York"),
			shouldArchive:             true,
		},
		{
			name:     "Daily - DST Fall Back (25h day)",
			interval: 24 * time.Hour,
			// Nov 5, 2023 was DST end in NY.
			// 2023-11-04 12:00 EDT -> 2023-11-05 12:00 EST is 25 hours.
			// We want to ensure this counts as a full day change.
			currentBackupTimestampUTC: time.Date(2023, 11, 4, 16, 0, 0, 0, time.UTC), // 12:00 EDT
			currentTimestampUTC:       time.Date(2023, 11, 5, 17, 0, 0, 0, time.UTC), // 12:00 EST (25h later)
			location:                  mustLoadLocation("America/New_York"),
			shouldArchive:             true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			loc := tc.location
			if loc == nil {
				loc = time.UTC
			}
			// Create a run struct to pass to shouldArchive
			run := &archiveRun{
				currentBackupTimestampUTC: tc.currentBackupTimestampUTC,
				currentTimestampUTC:       tc.currentTimestampUTC,
				interval:                  tc.interval,
				location:                  loc,
			}
			result := run.shouldArchive()

			if tc.shouldArchive != result {
				t.Errorf("expected shouldArchive to be %v, but got %v", tc.shouldArchive, result)
			}
		})
	}
}

func mustLoadLocation(name string) *time.Location {
	loc, err := time.LoadLocation(name)
	if err != nil {
		// Fallback for systems without zoneinfo (e.g. Windows sometimes)
		return time.UTC
	}
	return loc
}

func TestEpochBucketingVerification(t *testing.T) {
	// This test verifies the assumptions made about epoch buckets in the main test cases.
	// Unix Epoch 1970-01-01 was a Thursday.
	// Therefore, 7-day buckets align with Thursday-to-Wednesday cycles.

	// Oct 23, 2023 (Monday) -> Bucket A
	t1 := time.Date(2023, 10, 23, 12, 0, 0, 0, time.UTC)
	// Oct 25, 2023 (Wednesday) -> Bucket A
	t2 := time.Date(2023, 10, 25, 12, 0, 0, 0, time.UTC)
	// Oct 26, 2023 (Thursday) -> Bucket B (New Bucket)
	t3 := time.Date(2023, 10, 26, 12, 0, 0, 0, time.UTC)

	b1 := calculateEpochBucket(t1, 7)
	b2 := calculateEpochBucket(t2, 7)
	b3 := calculateEpochBucket(t3, 7)

	if b1 != b2 {
		t.Errorf("Expected Mon Oct 23 and Wed Oct 25 to be in same bucket, got %d and %d", b1, b2)
	}
	if b2 == b3 {
		t.Errorf("Expected Wed Oct 25 and Thu Oct 26 to be in different buckets, got %d and %d", b2, b3)
	}
}

func TestCalculateEpochDays(t *testing.T) {
	loc := time.UTC

	// Case 1: Leap Year (1972)
	// Feb 28, 1972
	t1 := time.Date(1972, 2, 28, 12, 0, 0, 0, loc)
	d1 := calculateEpochDays(t1, loc)

	// Feb 29, 1972 (Leap Day)
	t2 := time.Date(1972, 2, 29, 12, 0, 0, 0, loc)
	d2 := calculateEpochDays(t2, loc)

	// Mar 1, 1972
	t3 := time.Date(1972, 3, 1, 12, 0, 0, 0, loc)
	d3 := calculateEpochDays(t3, loc)

	if d2 != d1+1 {
		t.Errorf("Leap Year: Expected Feb 29 to be 1 day after Feb 28, got diff %d", d2-d1)
	}
	if d3 != d2+1 {
		t.Errorf("Leap Year: Expected Mar 1 to be 1 day after Feb 29, got diff %d", d3-d2)
	}

	// Case 2: Non-Leap Year (1973)
	// Feb 28, 1973
	t4 := time.Date(1973, 2, 28, 12, 0, 0, 0, loc)
	d4 := calculateEpochDays(t4, loc)

	// Mar 1, 1973
	t5 := time.Date(1973, 3, 1, 12, 0, 0, 0, loc)
	d5 := calculateEpochDays(t5, loc)

	if d5 != d4+1 {
		t.Errorf("Non-Leap Year: Expected Mar 1 to be 1 day after Feb 28, got diff %d", d5-d4)
	}
}

// calculateEpochBucket is a helper to verify test assumptions about bucketing.
// It mirrors the logic in patharchive.go.
func calculateEpochBucket(t time.Time, intervalDays int64) int64 {
	days := calculateEpochDays(t, t.Location())
	return days / intervalDays
}

func TestArchive(t *testing.T) {
	t.Run("Happy Path - Archives when threshold is crossed", func(t *testing.T) {
		// Arrange
		tempDir := t.TempDir()
		cfg := config.NewDefault()
		cfg.Paths.TargetBase = tempDir
		cfg.Archive.Incremental.IntervalMode = config.ManualInterval
		cfg.Archive.Incremental.IntervalSeconds = 86400 // 24h

		archiver := NewPathArchiver(cfg)

		currentBackupTimestampUTC := time.Now().UTC().Add(-25 * time.Hour)
		currentTimestampUTC := time.Now().UTC()
		currentBackupPath := filepath.Join(tempDir, "current")
		archivesDir := filepath.Join(tempDir, cfg.Paths.IncrementalSubDirs.Archive)
		createTestMetafile(t, currentBackupPath, currentBackupTimestampUTC)

		// Act
		archivePath, err := archiver.Archive(context.Background(), archivesDir, currentBackupPath, currentTimestampUTC, cfg.Archive.Incremental, cfg.Retention.Incremental)
		if err != nil {
			t.Fatalf("Archive failed: %v", err)
		}

		// Assert
		if _, err := os.Stat(currentBackupPath); !os.IsNotExist(err) {
			t.Error("expected old current directory to be renamed, but it still exists")
		}

		archiveTimestamp := config.FormatTimestampWithOffset(currentBackupTimestampUTC)
		archiveDirName := cfg.Naming.Prefix + archiveTimestamp
		expectedArchivePath := filepath.Join(archivesDir, archiveDirName)
		if archivePath != expectedArchivePath {
			t.Errorf("expected archive path %s, but got %s", expectedArchivePath, archivePath)
		}
		if _, err := os.Stat(expectedArchivePath); os.IsNotExist(err) {
			t.Errorf("expected archive directory %s to exist, but it does not", expectedArchivePath)
		}
	})

	t.Run("No-Op - Does not archive when threshold is not crossed", func(t *testing.T) {
		// Arrange
		tempDir := t.TempDir()
		cfg := config.NewDefault()
		cfg.Paths.TargetBase = tempDir
		cfg.Archive.Incremental.IntervalSeconds = 86400 // 24h

		archiver := NewPathArchiver(cfg)

		currentBackupTimestampUTC := time.Now().UTC().Add(-2 * time.Hour)
		currentTimestampUTC := time.Now().UTC()
		currentBackupPath := filepath.Join(tempDir, "current")
		archivesDir := filepath.Join(tempDir, cfg.Paths.IncrementalSubDirs.Archive)
		createTestMetafile(t, currentBackupPath, currentBackupTimestampUTC)

		// Act
		_, err := archiver.Archive(context.Background(), archivesDir, currentBackupPath, currentTimestampUTC, cfg.Archive.Incremental, cfg.Retention.Incremental)
		if err != nil && err != ErrNothingToArchive {
			t.Fatalf("Archive failed unexpectedly: %v", err)
		}

		// Assert
		if _, err := os.Stat(currentBackupPath); os.IsNotExist(err) {
			t.Error("expected current directory to exist, but it was renamed")
		}
	})

	t.Run("Error - Destination already exists", func(t *testing.T) {
		// Arrange
		tempDir := t.TempDir()
		cfg := config.NewDefault()
		cfg.Paths.TargetBase = tempDir
		cfg.Archive.Incremental.IntervalMode = config.ManualInterval
		cfg.Archive.Incremental.IntervalSeconds = 86400 // 24h

		archiver := NewPathArchiver(cfg)

		currentBackupTimestampUTC := time.Now().UTC().Add(-25 * time.Hour)
		currentTimestampUTC := time.Now().UTC()
		currentBackupPath := filepath.Join(tempDir, "current")
		archivesDir := filepath.Join(tempDir, cfg.Paths.IncrementalSubDirs.Archive)
		createTestMetafile(t, currentBackupPath, currentBackupTimestampUTC)

		// Manually create the destination to cause a conflict
		archiveTimestamp := config.FormatTimestampWithOffset(currentBackupTimestampUTC)
		archiveDirName := cfg.Naming.Prefix + archiveTimestamp
		conflictPath := filepath.Join(archivesDir, archiveDirName)
		os.MkdirAll(conflictPath, 0755)

		// Act
		_, err := archiver.Archive(context.Background(), archivesDir, currentBackupPath, currentTimestampUTC, cfg.Archive.Incremental, cfg.Retention.Incremental)

		// Assert
		if err == nil {
			t.Fatal("expected an error when destination exists, but got nil")
		}
		expectedErr := fmt.Sprintf("archive destination %s already exists", conflictPath)
		if !os.IsExist(err) && err.Error() != expectedErr {
			t.Errorf("expected error message '%s', but got '%s'", expectedErr, err.Error())
		}
	})

	t.Run("Disabled - Interval 0 does not create archives dir", func(t *testing.T) {
		// Arrange
		tempDir := t.TempDir()
		cfg := config.NewDefault()
		cfg.Paths.TargetBase = tempDir
		// Explicitly disable archiving by setting interval to 0 in manual mode
		cfg.Archive.Incremental.IntervalMode = config.ManualInterval
		cfg.Archive.Incremental.IntervalSeconds = 0

		archiver := NewPathArchiver(cfg)

		currentBackupTimestampUTC := time.Now().UTC().Add(-25 * time.Hour)
		currentTimestampUTC := time.Now().UTC()
		currentBackupPath := filepath.Join(tempDir, "current")
		archivesDir := filepath.Join(tempDir, cfg.Paths.IncrementalSubDirs.Archive)
		createTestMetafile(t, currentBackupPath, currentBackupTimestampUTC)

		// Act
		_, err := archiver.Archive(context.Background(), archivesDir, currentBackupPath, currentTimestampUTC, cfg.Archive.Incremental, cfg.Retention.Incremental)
		if err != nil && err != ErrNothingToArchive {
			t.Fatalf("Archive failed unexpectedly: %v", err)
		}

		// Assert
		if _, err := os.Stat(archivesDir); !os.IsNotExist(err) {
			t.Errorf("expected archives directory %s NOT to exist when interval is 0, but it does", archivesDir)
		}
	})

	t.Run("Dry Run - Does not rename directory", func(t *testing.T) {
		// Arrange
		tempDir := t.TempDir()
		cfg := config.NewDefault()
		cfg.Paths.TargetBase = tempDir
		cfg.Archive.Incremental.IntervalMode = config.ManualInterval
		cfg.Archive.Incremental.IntervalSeconds = 86400 // 24h
		cfg.DryRun = true                               // Enable Dry Run

		archiver := NewPathArchiver(cfg)

		currentBackupTimestampUTC := time.Now().UTC().Add(-25 * time.Hour)
		currentTimestampUTC := time.Now().UTC()
		currentBackupPath := filepath.Join(tempDir, "current")
		archivesDir := filepath.Join(tempDir, cfg.Paths.IncrementalSubDirs.Archive)
		createTestMetafile(t, currentBackupPath, currentBackupTimestampUTC)

		// Act
		_, err := archiver.Archive(context.Background(), archivesDir, currentBackupPath, currentTimestampUTC, cfg.Archive.Incremental, cfg.Retention.Incremental)
		if err != nil {
			t.Fatalf("Archive failed unexpectedly in dry run: %v", err)
		}

		// Assert
		if _, err := os.Stat(currentBackupPath); os.IsNotExist(err) {
			t.Error("expected current directory to exist in dry run, but it was renamed/deleted")
		}
		if _, err := os.Stat(archivesDir); !os.IsNotExist(err) {
			t.Errorf("expected archives directory %s NOT to exist in dry run, but it does", archivesDir)
		}
	})
}

func TestDetermineInterval(t *testing.T) {
	t.Run("Auto Mode", func(t *testing.T) {
		testCases := []struct {
			name            string
			retentionPolicy config.RetentionPolicyConfig
			expected        time.Duration
		}{
			{"Hourly retention", config.RetentionPolicyConfig{Enabled: true, Hours: 1}, 1 * time.Hour},
			{"Daily retention", config.RetentionPolicyConfig{Enabled: true, Days: 1}, 24 * time.Hour},
			{"Weekly retention", config.RetentionPolicyConfig{Enabled: true, Weeks: 1}, 7 * 24 * time.Hour},
			{"Monthly retention", config.RetentionPolicyConfig{Enabled: true, Months: 1}, 30 * 24 * time.Hour},
			{"Yearly retention", config.RetentionPolicyConfig{Enabled: true, Years: 1}, 365 * 24 * time.Hour},
			{"No retention (but enabled)", config.RetentionPolicyConfig{Enabled: true}, 24 * time.Hour}, // Fallback
			{"Mixed retention (hourly wins)", config.RetentionPolicyConfig{Enabled: true, Hours: 1, Days: 7}, 1 * time.Hour},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Arrange
				cfg := config.NewDefault()
				cfg.Archive.Incremental.IntervalMode = config.AutoInterval
				cfg.Retention.Incremental = tc.retentionPolicy

				// Act
				archiver := NewPathArchiver(cfg)
				interval := archiver.determineInterval(cfg.Archive.Incremental, cfg.Retention.Incremental)

				// Assert
				if interval != tc.expected {
					t.Errorf("expected interval %v, but got %v", tc.expected, interval)
				}
			})
		}
	})

	t.Run("Manual Mode", func(t *testing.T) {
		t.Run("Returns configured value", func(t *testing.T) {
			// Arrange
			cfg := config.NewDefault()
			cfg.Archive.Incremental.IntervalMode = config.ManualInterval
			cfg.Archive.Incremental.IntervalSeconds = 43200 // 12h

			// Act
			archiver := NewPathArchiver(cfg)
			interval := archiver.determineInterval(cfg.Archive.Incremental, cfg.Retention.Incremental)

			// Assert
			if interval != 12*time.Hour {
				t.Errorf("expected interval to be 12h, but got %v", interval)
			}
		})

		t.Run("Logs warning on mismatch", func(t *testing.T) {
			// Arrange
			var logBuf bytes.Buffer
			plog.SetOutput(&logBuf)
			t.Cleanup(func() { plog.SetOutput(os.Stderr) })

			cfg := config.NewDefault()
			cfg.Archive.Incremental.IntervalMode = config.ManualInterval
			cfg.Archive.Incremental.IntervalSeconds = 172800 // 48h
			cfg.Retention.Incremental.Days = 7               // Daily retention is enabled

			// Act
			archiver := NewPathArchiver(cfg)
			archiver.determineInterval(cfg.Archive.Incremental, cfg.Retention.Incremental)

			// Assert
			logOutput := logBuf.String()
			if !strings.Contains(logOutput, "Configuration Mismatch") {
				t.Errorf("expected a 'Configuration Mismatch' warning in logs, but got none in: %q", logOutput)
			}
			if !strings.Contains(logOutput, "mismatched_periods=Daily") {
				t.Errorf("expected mismatched_periods to contain 'Daily', but got: %q", logOutput)
			}
		})

		t.Run("Does not log warning on match", func(t *testing.T) {
			// Arrange
			var logBuf bytes.Buffer
			plog.SetOutput(&logBuf)
			t.Cleanup(func() { plog.SetOutput(os.Stderr) })

			cfg := config.NewDefault()
			cfg.Archive.Incremental.IntervalMode = config.ManualInterval
			cfg.Archive.Incremental.IntervalSeconds = 43200 // 12h
			cfg.Retention.Incremental.Days = 7              // Daily retention is enabled

			// Act
			archiver := NewPathArchiver(cfg)
			archiver.determineInterval(cfg.Archive.Incremental, cfg.Retention.Incremental)

			// Assert
			logOutput := logBuf.String()
			if strings.Contains(logOutput, "Configuration Mismatch") {
				t.Errorf("expected no mismatch warnings, but got: %q", logOutput)
			}
		})
	})
}
