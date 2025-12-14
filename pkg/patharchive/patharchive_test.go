package patharchive

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"pixelgardenlabs.io/pgl-backup/pkg/config"
	"pixelgardenlabs.io/pgl-backup/pkg/plog"
)

// runMetadata is a local copy for testing purposes, mirroring retention.RunMetadata
type runMetadata struct {
	Version    string    `json:"version"`
	BackupTime time.Time `json:"backupTime"`
	Mode       string    `json:"mode"`
	Source     string    `json:"source"`
}

// Helper to create a temporary directory structure for a backup.
func createTestBackup(t *testing.T, baseDir, name string, backupTime time.Time) {
	t.Helper()
	backupPath := filepath.Join(baseDir, name)
	if err := os.MkdirAll(backupPath, 0755); err != nil {
		t.Fatalf("failed to create test backup dir: %v", err)
	}

	metaFilePath := filepath.Join(backupPath, config.MetaFileName)
	metaData := runMetadata{
		Version:    "test",
		BackupTime: backupTime,
		Mode:       "incremental",
		Source:     "/src",
	}
	jsonData, err := json.MarshalIndent(metaData, "", "  ")
	if err != nil {
		t.Fatalf("could not marshal meta data: %v", err)
	}
	if err := os.WriteFile(metaFilePath, jsonData, 0644); err != nil {
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
			cfg := config.NewDefault()
			cfg.IncrementalArchivePolicy.Interval = tc.interval
			archiver := NewPathArchiver(cfg)

			// Create a runState struct to pass to shouldArchive
			runState := &archiveRunState{
				currentBackupTimestampUTC: tc.currentBackupTimestampUTC,
				currentTimestampUTC:       tc.currentTimestampUTC,
				interval:                  tc.interval,
			}
			result := archiver.shouldArchive(runState)

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
		cfg.Paths.ArchivesSubDir = "archives"
		cfg.IncrementalArchivePolicy.Interval = 24 * time.Hour

		archiver := NewPathArchiver(cfg)

		lastBackupTime := time.Now().UTC().Add(-25 * time.Hour)
		currentBackupTime := time.Now().UTC()
		currentBackupDir := filepath.Join(tempDir, "current")
		createTestBackup(t, tempDir, "current", lastBackupTime)

		// Act
		err := archiver.Archive(context.Background(), currentBackupDir, lastBackupTime, currentBackupTime)
		if err != nil {
			t.Fatalf("Archive failed: %v", err)
		}

		// Assert
		if _, err := os.Stat(currentBackupDir); !os.IsNotExist(err) {
			t.Error("expected old current directory to be renamed, but it still exists")
		}

		archiveTimestamp := config.FormatTimestampWithOffset(lastBackupTime)
		archiveDirName := cfg.Naming.Prefix + archiveTimestamp
		expectedArchivePath := filepath.Join(tempDir, cfg.Paths.ArchivesSubDir, archiveDirName)
		if _, err := os.Stat(expectedArchivePath); os.IsNotExist(err) {
			t.Errorf("expected archive directory %s to exist, but it does not", expectedArchivePath)
		}
	})

	t.Run("No-Op - Does not archive when threshold is not crossed", func(t *testing.T) {
		// Arrange
		tempDir := t.TempDir()
		cfg := config.NewDefault()
		cfg.Paths.TargetBase = tempDir
		cfg.IncrementalArchivePolicy.Interval = 24 * time.Hour

		archiver := NewPathArchiver(cfg)

		lastBackupTime := time.Now().UTC().Add(-2 * time.Hour)
		currentBackupTime := time.Now().UTC()
		currentBackupDir := filepath.Join(tempDir, "current")
		createTestBackup(t, tempDir, "current", lastBackupTime)

		// Act
		err := archiver.Archive(context.Background(), currentBackupDir, lastBackupTime, currentBackupTime)
		if err != nil {
			t.Fatalf("Archive failed unexpectedly: %v", err)
		}

		// Assert
		if _, err := os.Stat(currentBackupDir); os.IsNotExist(err) {
			t.Error("expected current directory to exist, but it was renamed")
		}
	})

	t.Run("Error - Destination already exists", func(t *testing.T) {
		// Arrange
		tempDir := t.TempDir()
		cfg := config.NewDefault()
		cfg.Paths.TargetBase = tempDir
		cfg.Paths.ArchivesSubDir = "archives"
		cfg.IncrementalArchivePolicy.Interval = 24 * time.Hour

		archiver := NewPathArchiver(cfg)

		lastBackupTime := time.Now().UTC().Add(-25 * time.Hour)
		currentBackupTime := time.Now().UTC()
		currentBackupDir := filepath.Join(tempDir, "current")
		createTestBackup(t, tempDir, "current", lastBackupTime)

		// Manually create the destination to cause a conflict
		archiveTimestamp := config.FormatTimestampWithOffset(lastBackupTime)
		archiveDirName := cfg.Naming.Prefix + archiveTimestamp
		conflictPath := filepath.Join(tempDir, cfg.Paths.ArchivesSubDir, archiveDirName)
		os.MkdirAll(conflictPath, 0755)

		// Act
		err := archiver.Archive(context.Background(), currentBackupDir, lastBackupTime, currentBackupTime)

		// Assert
		if err == nil {
			t.Fatal("expected an error when destination exists, but got nil")
		}
		expectedErr := fmt.Sprintf("archive destination %s already exists", conflictPath)
		if !os.IsExist(err) && err.Error() != expectedErr {
			t.Errorf("expected error message '%s', but got '%s'", expectedErr, err.Error())
		}
	})
}

func TestPrepareRun(t *testing.T) {
	t.Run("Auto Mode", func(t *testing.T) {
		testCases := []struct {
			name            string
			retentionPolicy config.BackupRetentionPolicyConfig
			expected        time.Duration
		}{
			{"Hourly retention", config.BackupRetentionPolicyConfig{Enabled: true, Hours: 1}, 1 * time.Hour},
			{"Daily retention", config.BackupRetentionPolicyConfig{Enabled: true, Days: 1}, 24 * time.Hour},
			{"Weekly retention", config.BackupRetentionPolicyConfig{Enabled: true, Weeks: 1}, 7 * 24 * time.Hour},
			{"Monthly retention", config.BackupRetentionPolicyConfig{Enabled: true, Months: 1}, 30 * 24 * time.Hour},
			{"Yearly retention", config.BackupRetentionPolicyConfig{Enabled: true, Years: 1}, 365 * 24 * time.Hour},
			{"No retention (but enabled)", config.BackupRetentionPolicyConfig{Enabled: true}, 24 * time.Hour}, // Fallback
			{"Mixed retention (hourly wins)", config.BackupRetentionPolicyConfig{Enabled: true, Hours: 1, Days: 7}, 1 * time.Hour},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Arrange
				cfg := config.NewDefault()
				cfg.IncrementalArchivePolicy.Mode = config.AutoInterval
				cfg.IncrementalRetentionPolicy = tc.retentionPolicy
				archiver := NewPathArchiver(cfg)

				// Act
				runState := &archiveRunState{}
				archiver.prepareRun(context.Background(), runState)

				// Assert
				if runState.interval != tc.expected {
					t.Errorf("expected interval %v, but got %v", tc.expected, runState.interval)
				}
			})
		}
	})

	t.Run("Manual Mode", func(t *testing.T) {
		t.Run("Returns configured value", func(t *testing.T) {
			// Arrange
			cfg := config.NewDefault()
			cfg.IncrementalArchivePolicy.Mode = config.ManualInterval
			cfg.IncrementalArchivePolicy.Interval = 12 * time.Hour
			archiver := NewPathArchiver(cfg)

			// Act
			runState := &archiveRunState{
				interval: cfg.IncrementalArchivePolicy.Interval,
			}
			archiver.prepareRun(context.Background(), runState)

			// Assert
			if runState.interval != 12*time.Hour {
				t.Errorf("expected interval to be 12h, but got %v", runState.interval)
			}
		})

		t.Run("Logs warning on mismatch", func(t *testing.T) {
			// Arrange
			var logBuf bytes.Buffer
			plog.SetOutput(&logBuf)
			t.Cleanup(func() { plog.SetOutput(os.Stderr) })

			cfg := config.NewDefault()
			cfg.IncrementalArchivePolicy.Mode = config.ManualInterval
			cfg.IncrementalArchivePolicy.Interval = 48 * time.Hour // Slower than daily
			cfg.IncrementalRetentionPolicy.Days = 7                // Daily retention is enabled

			archiver := NewPathArchiver(cfg)

			// Act
			runState := &archiveRunState{
				interval: cfg.IncrementalArchivePolicy.Interval,
			}
			archiver.prepareRun(context.Background(), runState)

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
			cfg.IncrementalArchivePolicy.Mode = config.ManualInterval
			cfg.IncrementalArchivePolicy.Interval = 12 * time.Hour // Faster than daily
			cfg.IncrementalRetentionPolicy.Days = 7                // Daily retention is enabled

			archiver := NewPathArchiver(cfg)

			// Act
			runState := &archiveRunState{}
			archiver.prepareRun(context.Background(), runState)

			// Assert
			logOutput := logBuf.String()
			if strings.Contains(logOutput, "Configuration Mismatch") {
				t.Errorf("expected no mismatch warnings, but got: %q", logOutput)
			}
		})
	})
}
