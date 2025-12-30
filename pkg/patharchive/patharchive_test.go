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
		Mode:         "incremental",
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
			name:                      "Weekly - No Archive (Day 3)",
			interval:                  7 * 24 * time.Hour,
			currentBackupTimestampUTC: time.Date(2023, 10, 23, 12, 0, 0, 0, time.UTC), // Day D
			currentTimestampUTC:       time.Date(2023, 10, 26, 12, 0, 0, 0, time.UTC), // Day D + 3
			shouldArchive:             false,
		},
		{
			name:                      "Weekly - Archive (Day 7 Boundary)",
			interval:                  7 * 24 * time.Hour,
			currentBackupTimestampUTC: time.Date(2023, 10, 23, 12, 0, 0, 0, time.UTC), // Day D (Bucket B)
			currentTimestampUTC:       time.Date(2023, 10, 30, 12, 0, 0, 0, time.UTC), // Day D + 7 (Bucket B + 1)
			shouldArchive:             true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a run struct to pass to shouldArchive
			run := &archiveRun{
				currentBackupTimestampUTC: tc.currentBackupTimestampUTC,
				currentTimestampUTC:       tc.currentTimestampUTC,
				interval:                  tc.interval,
			}
			result := run.shouldArchive()

			if tc.shouldArchive != result {
				t.Errorf("expected shouldArchive to be %v, but got %v", tc.shouldArchive, result)
			}
		})
	}
}

func TestArchive(t *testing.T) {
	t.Run("Happy Path - Archives when threshold is crossed", func(t *testing.T) {
		// Arrange
		tempDir := t.TempDir()
		cfg := config.NewDefault()
		cfg.Paths.TargetBase = tempDir
		cfg.Archive.Incremental.Mode = config.ManualInterval
		cfg.Archive.Incremental.IntervalSeconds = 86400 // 24h

		archiver := NewPathArchiver(cfg)

		currentBackupTimestampUTC := time.Now().UTC().Add(-25 * time.Hour)
		currentTimestampUTC := time.Now().UTC()
		currentBackupPath := filepath.Join(tempDir, "current")
		archivesDir := filepath.Join(tempDir, cfg.Paths.ArchivesSubDir)
		createTestMetafile(t, currentBackupPath, currentBackupTimestampUTC)

		// Act
		archivePath, err := archiver.Archive(context.Background(), archivesDir, currentBackupPath, currentTimestampUTC)
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
		archivesDir := filepath.Join(tempDir, cfg.Paths.ArchivesSubDir)
		createTestMetafile(t, currentBackupPath, currentBackupTimestampUTC)

		// Act
		_, err := archiver.Archive(context.Background(), archivesDir, currentBackupPath, currentTimestampUTC)
		if err != nil {
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
		cfg.Archive.Incremental.Mode = config.ManualInterval
		cfg.Archive.Incremental.IntervalSeconds = 86400 // 24h

		archiver := NewPathArchiver(cfg)

		currentBackupTimestampUTC := time.Now().UTC().Add(-25 * time.Hour)
		currentTimestampUTC := time.Now().UTC()
		currentBackupPath := filepath.Join(tempDir, "current")
		archivesDir := filepath.Join(tempDir, cfg.Paths.ArchivesSubDir)
		createTestMetafile(t, currentBackupPath, currentBackupTimestampUTC)

		// Manually create the destination to cause a conflict
		archiveTimestamp := config.FormatTimestampWithOffset(currentBackupTimestampUTC)
		archiveDirName := cfg.Naming.Prefix + archiveTimestamp
		conflictPath := filepath.Join(archivesDir, archiveDirName)
		os.MkdirAll(conflictPath, 0755)

		// Act
		_, err := archiver.Archive(context.Background(), archivesDir, currentBackupPath, currentTimestampUTC)

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
		cfg.Archive.Incremental.Mode = config.ManualInterval
		cfg.Archive.Incremental.IntervalSeconds = 0

		archiver := NewPathArchiver(cfg)

		currentBackupTimestampUTC := time.Now().UTC().Add(-25 * time.Hour)
		currentTimestampUTC := time.Now().UTC()
		currentBackupPath := filepath.Join(tempDir, "current")
		archivesDir := filepath.Join(tempDir, cfg.Paths.ArchivesSubDir)
		createTestMetafile(t, currentBackupPath, currentBackupTimestampUTC)

		// Act
		_, err := archiver.Archive(context.Background(), archivesDir, currentBackupPath, currentTimestampUTC)
		if err != nil {
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
		cfg.Archive.Incremental.Mode = config.ManualInterval
		cfg.Archive.Incremental.IntervalSeconds = 86400 // 24h
		cfg.DryRun = true                               // Enable Dry Run

		archiver := NewPathArchiver(cfg)

		currentBackupTimestampUTC := time.Now().UTC().Add(-25 * time.Hour)
		currentTimestampUTC := time.Now().UTC()
		currentBackupPath := filepath.Join(tempDir, "current")
		archivesDir := filepath.Join(tempDir, cfg.Paths.ArchivesSubDir)
		createTestMetafile(t, currentBackupPath, currentBackupTimestampUTC)

		// Act
		_, err := archiver.Archive(context.Background(), archivesDir, currentBackupPath, currentTimestampUTC)
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
				cfg.Archive.Incremental.Mode = config.AutoInterval
				cfg.Retention.Incremental = tc.retentionPolicy

				// Act
				archiver := NewPathArchiver(cfg)
				interval := archiver.determineInterval()

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
			cfg.Archive.Incremental.Mode = config.ManualInterval
			cfg.Archive.Incremental.IntervalSeconds = 43200 // 12h

			// Act
			archiver := NewPathArchiver(cfg)
			interval := archiver.determineInterval()

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
			cfg.Archive.Incremental.Mode = config.ManualInterval
			cfg.Archive.Incremental.IntervalSeconds = 172800 // 48h
			cfg.Retention.Incremental.Days = 7               // Daily retention is enabled

			// Act
			archiver := NewPathArchiver(cfg)
			archiver.determineInterval()

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
			cfg.Archive.Incremental.Mode = config.ManualInterval
			cfg.Archive.Incremental.IntervalSeconds = 43200 // 12h
			cfg.Retention.Incremental.Days = 7              // Daily retention is enabled

			// Act
			archiver := NewPathArchiver(cfg)
			archiver.determineInterval()

			// Assert
			logOutput := logBuf.String()
			if strings.Contains(logOutput, "Configuration Mismatch") {
				t.Errorf("expected no mismatch warnings, but got: %q", logOutput)
			}
		})
	})
}
