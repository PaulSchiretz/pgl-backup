package patharchive_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/patharchive"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

func TestShouldArchive(t *testing.T) {
	// Define a fixed "Now" for consistent testing.
	// Note: The implementation uses time.Local for >= 24h intervals logic.
	now := time.Date(2023, 10, 27, 14, 0, 0, 0, time.UTC)

	tests := []struct {
		name            string
		disabled        bool
		intervalMode    patharchive.IntervalMode
		intervalSeconds int
		constraints     patharchive.IntervalModeConstraints
		lastBackupAge   time.Duration
		setupEmptyPath  bool
		expectShould    bool
		expectErr       error
	}{
		{
			name:            "Manual - 24h Interval - 25h Passed (Should Archive)",
			intervalMode:    patharchive.Manual,
			intervalSeconds: 86400,
			lastBackupAge:   25 * time.Hour,
			expectShould:    true,
		},
		{
			name:            "Manual - 24h Interval - 1h Passed (Should NOT Archive)",
			intervalMode:    patharchive.Manual,
			intervalSeconds: 86400,
			lastBackupAge:   1 * time.Hour,
			expectShould:    false,
		},
		{
			name:          "Auto - Hourly Retention - 2h Passed (Should Archive)",
			intervalMode:  patharchive.Auto,
			constraints:   patharchive.IntervalModeConstraints{Hours: 1},
			lastBackupAge: 2 * time.Hour,
			expectShould:  true,
		},
		{
			name:          "Auto - Daily Retention - 1h Passed (Should NOT Archive)",
			intervalMode:  patharchive.Auto,
			constraints:   patharchive.IntervalModeConstraints{Days: 1},
			lastBackupAge: 1 * time.Hour,
			expectShould:  false,
		},
		{
			name:            "Disabled - Should NOT Archive even if interval passed",
			disabled:        true,
			intervalMode:    patharchive.Manual,
			intervalSeconds: 86400,
			lastBackupAge:   25 * time.Hour,
			expectShould:    false,
			expectErr:       patharchive.ErrDisabled,
		},
		{
			name:            "Invalid Input - Empty Path",
			intervalMode:    patharchive.Manual,
			intervalSeconds: 86400,
			lastBackupAge:   25 * time.Hour,
			setupEmptyPath:  true,
			expectShould:    false,
			expectErr:       patharchive.ErrNothingToArchive,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// 1. Setup
			lastBackupTime := now.Add(-tc.lastBackupAge)
			toArchive := metafile.MetafileInfo{
				RelPathKey: "current",
				Metadata:   metafile.MetafileContent{TimestampUTC: lastBackupTime},
			}
			if tc.setupEmptyPath {
				toArchive.RelPathKey = ""
			}

			archiver := patharchive.NewPathArchiver()
			plan := &patharchive.Plan{
				Enabled:         !tc.disabled,
				IntervalMode:    tc.intervalMode,
				IntervalSeconds: tc.intervalSeconds,
				Constraints:     tc.constraints,
			}

			// 2. Execute
			should, err := archiver.ShouldArchive(toArchive, plan, now)

			// 3. Verify
			if should != tc.expectShould {
				t.Errorf("expected should=%v, got %v", tc.expectShould, should)
			}

			if tc.expectErr != nil {
				if !errors.Is(err, tc.expectErr) {
					t.Errorf("expected error %v, got %v", tc.expectErr, err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestArchive(t *testing.T) {
	now := time.Now().UTC()

	tests := []struct {
		name           string
		dryRun         bool
		setupConflict  bool // If true, create the destination directory beforehand
		expectErrorStr string
		expectArchived bool // True if file system change is expected
		verify         func(t *testing.T, result metafile.MetafileInfo)
	}{
		{
			name:           "Happy Path - Should Archive",
			expectArchived: true,
			verify: func(t *testing.T, result metafile.MetafileInfo) {
				expectedPrefix := "archive/backup_"
				if !strings.HasPrefix(result.RelPathKey, expectedPrefix) {
					t.Errorf("ResultInfo path mismatch. Want prefix %q, got %q", expectedPrefix, result.RelPathKey)
				}
			},
		},
		{
			name:           "Dry Run - Should Log but NOT Move",
			dryRun:         true,
			expectArchived: false, // FS should not change
			verify: func(t *testing.T, result metafile.MetafileInfo) {
				expectedPrefix := "archive/backup_"
				if !strings.HasPrefix(result.RelPathKey, expectedPrefix) {
					t.Errorf("Dry Run ResultInfo path mismatch. Want prefix %q, got %q", expectedPrefix, result.RelPathKey)
				}
			},
		},
		{
			name:           "Destination Conflict (Should Error)",
			setupConflict:  true,
			expectErrorStr: "already exists",
			expectArchived: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// 1. Setup Filesystem
			tempDir := t.TempDir()
			targetBase := tempDir
			relCurrent := "current"
			relArchive := "archive"
			prefix := "backup_"

			absCurrent := filepath.Join(targetBase, relCurrent)
			if err := os.MkdirAll(absCurrent, 0755); err != nil {
				t.Fatalf("failed to create current dir: %v", err)
			}

			// Create Metafile
			lastBackupTime := now.Add(-25 * time.Hour) // Fixed time for consistent naming
			meta := metafile.MetafileContent{
				TimestampUTC: lastBackupTime,
			}
			if err := metafile.Write(absCurrent, &meta); err != nil {
				t.Fatalf("failed to write metafile: %v", err)
			}

			toArchive := metafile.MetafileInfo{
				RelPathKey: relCurrent,
				Metadata:   meta,
			}

			// Setup Conflict if needed
			if tc.setupConflict {
				// Calculate expected name
				ts := util.FormatTimestampWithOffset(lastBackupTime)
				conflictPath := filepath.Join(targetBase, relArchive, prefix+ts)
				if err := os.MkdirAll(conflictPath, 0755); err != nil {
					t.Fatalf("failed to create conflict dir: %v", err)
				}
			}

			// 2. Create Archiver and Plan
			archiver := patharchive.NewPathArchiver()
			plan := &patharchive.Plan{
				Enabled:         true,
				IntervalMode:    patharchive.Manual,
				IntervalSeconds: 86400,
				DryRun:          tc.dryRun,
				Metrics:         false,
			}

			// 3. Execute
			result, err := archiver.Archive(context.Background(), targetBase, relArchive, prefix, toArchive, plan, now.Add(1*time.Hour))

			// 4. Verify Error
			if tc.expectErrorStr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.expectErrorStr) {
					t.Errorf("expected error containing %q, got %v", tc.expectErrorStr, err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// 5. Verify Filesystem State
			_, errCurrent := os.Stat(absCurrent)
			currentExists := errCurrent == nil

			if tc.expectArchived {
				if currentExists {
					t.Error("expected 'current' directory to be moved, but it still exists")
				}
				// Verify archive exists
				ts := util.FormatTimestampWithOffset(lastBackupTime)
				expectedArchivePath := filepath.Join(targetBase, relArchive, prefix+ts)
				if _, err := os.Stat(expectedArchivePath); os.IsNotExist(err) {
					t.Errorf("expected archive at %s, but not found", expectedArchivePath)
				}
			} else {
				if !currentExists {
					t.Error("expected 'current' directory to remain, but it is gone")
				}
			}

			if tc.verify != nil {
				tc.verify(t, result)
			}
		})
	}
}
