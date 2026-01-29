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

func TestArchive(t *testing.T) {
	// Define a fixed "Now" for consistent testing.
	// Note: The implementation uses time.Local for >= 24h intervals logic.
	now := time.Now().UTC()

	tests := []struct {
		name            string
		disabled        bool
		intervalMode    patharchive.IntervalMode
		intervalSeconds int
		constraints     patharchive.IntervalModeConstraints
		lastBackupAge   time.Duration
		dryRun          bool
		setupConflict   bool // If true, create the destination directory beforehand
		setupEmptyPath  bool // If true, force the source path to be empty
		expectError     error
		expectErrorStr  string
		expectArchived  bool // True if file system change is expected
		verify          func(t *testing.T, plan *patharchive.Plan, result metafile.MetafileInfo)
	}{
		{
			name:            "Manual - 24h Interval - 25h Passed (Should Archive)",
			intervalMode:    patharchive.Manual,
			intervalSeconds: 86400,
			lastBackupAge:   25 * time.Hour,
			expectArchived:  true,
			verify: func(t *testing.T, plan *patharchive.Plan, result metafile.MetafileInfo) {
				expectedPrefix := "archive/backup_"
				if !strings.HasPrefix(result.RelPathKey, expectedPrefix) {
					t.Errorf("ResultInfo path mismatch. Want prefix %q, got %q", expectedPrefix, result.RelPathKey)
				}
			},
		},
		{
			name:            "Manual - 24h Interval - 1h Passed (Should NOT Archive)",
			intervalMode:    patharchive.Manual,
			intervalSeconds: 86400,
			lastBackupAge:   1 * time.Hour,
			expectError:     patharchive.ErrNothingToArchive,
			expectArchived:  false,
		},
		{
			name:           "Auto - Hourly Retention - 2h Passed (Should Archive)",
			intervalMode:   patharchive.Auto,
			constraints:    patharchive.IntervalModeConstraints{Hours: 1},
			lastBackupAge:  2 * time.Hour,
			expectArchived: true,
			verify: func(t *testing.T, plan *patharchive.Plan, result metafile.MetafileInfo) {
				expectedPrefix := "archive/backup_"
				if !strings.HasPrefix(result.RelPathKey, expectedPrefix) {
					t.Errorf("ResultInfo path mismatch. Want prefix %q, got %q", expectedPrefix, result.RelPathKey)
				}
			},
		},
		{
			name:           "Auto - Daily Retention - 1h Passed (Should NOT Archive)",
			intervalMode:   patharchive.Auto,
			constraints:    patharchive.IntervalModeConstraints{Days: 1},
			lastBackupAge:  1 * time.Hour,
			expectError:    patharchive.ErrNothingToArchive,
			expectArchived: false,
		},
		{
			name:            "Dry Run - 25h Passed (Should Log but NOT Move)",
			intervalMode:    patharchive.Manual,
			intervalSeconds: 86400,
			lastBackupAge:   25 * time.Hour,
			dryRun:          true,
			expectArchived:  false, // FS should not change
		},
		{
			name:            "Disabled - Should NOT Archive even if interval passed",
			disabled:        true,
			intervalMode:    patharchive.Manual,
			intervalSeconds: 86400,
			lastBackupAge:   25 * time.Hour,
			expectArchived:  false,
			expectError:     patharchive.ErrDisabled,
		},
		{
			name:            "Dry Run - Verify ResultInfo Populated",
			intervalMode:    patharchive.Manual,
			intervalSeconds: 86400,
			lastBackupAge:   25 * time.Hour,
			dryRun:          true,
			expectArchived:  false,
			verify: func(t *testing.T, plan *patharchive.Plan, result metafile.MetafileInfo) {
				expectedPrefix := "archive/backup_"
				if !strings.HasPrefix(result.RelPathKey, expectedPrefix) {
					t.Errorf("Dry Run ResultInfo path mismatch. Want prefix %q, got %q", expectedPrefix, result.RelPathKey)
				}
			},
		},
		{
			name:            "Invalid Input - Empty Path",
			intervalMode:    patharchive.Manual,
			intervalSeconds: 86400,
			lastBackupAge:   25 * time.Hour,
			setupEmptyPath:  true,
			expectError:     patharchive.ErrNothingToArchive,
			verify: func(t *testing.T, plan *patharchive.Plan, result metafile.MetafileInfo) {
				// No result info should be set
			},
		},
		{
			name:            "Destination Conflict (Should Error)",
			intervalMode:    patharchive.Manual,
			intervalSeconds: 86400,
			lastBackupAge:   25 * time.Hour,
			setupConflict:   true,
			expectErrorStr:  "already exists",
			expectArchived:  false,
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
			lastBackupTime := now.Add(-tc.lastBackupAge)
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
			// Override for invalid input test
			if tc.setupEmptyPath {
				toArchive.RelPathKey = ""
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
				Enabled:         !tc.disabled,
				IntervalMode:    tc.intervalMode,
				IntervalSeconds: tc.intervalSeconds,
				Constraints:     tc.constraints,
				DryRun:          tc.dryRun,
				Metrics:         false,
			}

			// 3. Execute
			result, err := archiver.Archive(context.Background(), targetBase, relArchive, prefix, toArchive, plan, now)

			// 4. Verify Error
			if tc.expectError != nil {
				if !errors.Is(err, tc.expectError) {
					t.Errorf("expected error %v, got %v", tc.expectError, err)
				}
			} else if tc.expectErrorStr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.expectErrorStr) {
					t.Errorf("expected error containing %q, got %v", tc.expectErrorStr, err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			} else {
				if !tc.disabled && result.RelPathKey == "" {
					t.Error("expected plan.ResultInfo to be set, but it was empty")
				}
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
				tc.verify(t, plan, result)
			}
		})
	}
}
