package pathretention_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/pathretention"
)

func TestPrune(t *testing.T) {
	// Fixed reference time: Friday, Oct 27, 2023 12:00:00 UTC
	now := time.Date(2023, 10, 27, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name          string
		plan          pathretention.Plan
		setupBackups  func(t *testing.T, baseDir string) []metafile.MetafileInfo
		expectDeleted []string
		expectKept    []string
		expectError   error
	}{
		{
			name: "Keep 1 Hourly - Deletes Older",
			plan: pathretention.Plan{
				Enabled: true,
				Hours:   1,
			},
			setupBackups: func(t *testing.T, baseDir string) []metafile.MetafileInfo {
				return []metafile.MetafileInfo{
					createTestBackup(t, baseDir, "newest", now.Add(-10*time.Minute)),
					createTestBackup(t, baseDir, "older", now.Add(-2*time.Hour)),
				}
			},
			expectKept:    []string{"newest"},
			expectDeleted: []string{"older"},
		},
		{
			name: "Promotion - Hourly to Daily",
			plan: pathretention.Plan{
				Enabled: true,
				Hours:   1,
				Days:    1,
			},
			setupBackups: func(t *testing.T, baseDir string) []metafile.MetafileInfo {
				return []metafile.MetafileInfo{
					// Fits Hourly (12:00 bucket)
					createTestBackup(t, baseDir, "hourly_slot", now),
					// Fits Daily (Yesterday)
					createTestBackup(t, baseDir, "daily_slot", now.Add(-24*time.Hour)),
				}
			},
			expectKept:    []string{"hourly_slot", "daily_slot"},
			expectDeleted: []string{},
		},
		{
			name: "Bucket Saturation - Keep Newest in Bucket",
			plan: pathretention.Plan{
				Enabled: true,
				Days:    1, // Keep 1 daily backup
			},
			setupBackups: func(t *testing.T, baseDir string) []metafile.MetafileInfo {
				return []metafile.MetafileInfo{
					createTestBackup(t, baseDir, "today_new", now),
					createTestBackup(t, baseDir, "today_old", now.Add(-1*time.Hour)),
				}
			},
			expectKept:    []string{"today_new"},
			expectDeleted: []string{"today_old"},
		},
		{
			name: "Dry Run - No Deletion",
			plan: pathretention.Plan{
				Enabled: true,
				Hours:   1,
				DryRun:  true,
			},
			setupBackups: func(t *testing.T, baseDir string) []metafile.MetafileInfo {
				return []metafile.MetafileInfo{
					createTestBackup(t, baseDir, "newest", now),
					createTestBackup(t, baseDir, "older", now.Add(-2*time.Hour)),
				}
			},
			expectKept:    []string{"newest", "older"}, // Physically kept
			expectDeleted: []string{},
		},
		{
			name: "Complex Cascade",
			plan: pathretention.Plan{
				Enabled: true,
				Hours:   1,
				Days:    1,
				Weeks:   1,
			},
			setupBackups: func(t *testing.T, baseDir string) []metafile.MetafileInfo {
				return []metafile.MetafileInfo{
					createTestBackup(t, baseDir, "b_hour", now),                      // 12:00 Today
					createTestBackup(t, baseDir, "b_day", now.Add(-24*time.Hour)),    // Yesterday
					createTestBackup(t, baseDir, "b_week", now.Add(-7*24*time.Hour)), // Last Week
					createTestBackup(t, baseDir, "b_old", now.Add(-14*24*time.Hour)), // 2 Weeks ago (Delete)
				}
			},
			expectKept:    []string{"b_hour", "b_day", "b_week"},
			expectDeleted: []string{"b_old"},
		},
		{
			name: "Unsorted Input - Sorts Correctly",
			plan: pathretention.Plan{
				Enabled: true,
				Hours:   1,
			},
			setupBackups: func(t *testing.T, baseDir string) []metafile.MetafileInfo {
				// Pass in Oldest first. Logic should sort Newest first internally.
				return []metafile.MetafileInfo{
					createTestBackup(t, baseDir, "older", now.Add(-2*time.Hour)),
					createTestBackup(t, baseDir, "newest", now.Add(-10*time.Minute)),
				}
			},
			expectKept:    []string{"newest"},
			expectDeleted: []string{"older"},
		},
		{
			name: "Delete All - Zero Retention",
			plan: pathretention.Plan{
				Enabled: true,
				Hours:   0,
				Days:    0,
				Weeks:   0,
				Months:  0,
				Years:   0,
			},
			setupBackups: func(t *testing.T, baseDir string) []metafile.MetafileInfo {
				return []metafile.MetafileInfo{
					createTestBackup(t, baseDir, "b1", now),
					createTestBackup(t, baseDir, "b2", now.Add(-1*time.Hour)),
				}
			},
			expectKept:    []string{},
			expectDeleted: []string{"b1", "b2"},
		},
		{
			name: "Long Term - Monthly and Yearly",
			plan: pathretention.Plan{
				Enabled: true,
				Months:  1,
				Years:   1,
			},
			setupBackups: func(t *testing.T, baseDir string) []metafile.MetafileInfo {
				return []metafile.MetafileInfo{
					createTestBackup(t, baseDir, "this_month", now),                  // Fits Monthly (Oct 2023)
					createTestBackup(t, baseDir, "last_year", now.AddDate(-1, 0, 0)), // Fits Yearly (Oct 2022)
					createTestBackup(t, baseDir, "old_year", now.AddDate(-2, 0, 0)),  // Fits nothing (Oct 2021)
				}
			},
			expectKept:    []string{"this_month", "last_year"},
			expectDeleted: []string{"old_year"},
		},
		{
			name: "Disabled Retention - Safety Check",
			plan: pathretention.Plan{
				Enabled: false,
				Hours:   0, // Even with 0 retention, nothing should be deleted if disabled
			},
			setupBackups: func(t *testing.T, baseDir string) []metafile.MetafileInfo {
				return []metafile.MetafileInfo{
					createTestBackup(t, baseDir, "b1", now),
					createTestBackup(t, baseDir, "b2", now.Add(-1*time.Hour)),
				}
			},
			expectKept:    []string{"b1", "b2"},
			expectDeleted: []string{},
			expectError:   pathretention.ErrDisabled,
		},
		{
			name: "Empty Input - Returns Specific Error",
			plan: pathretention.Plan{Enabled: true},
			setupBackups: func(t *testing.T, baseDir string) []metafile.MetafileInfo {
				return []metafile.MetafileInfo{}
			},
			expectKept:  []string{},
			expectError: pathretention.ErrNothingToPrune,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			targetBase := t.TempDir()
			toPrune := tc.setupBackups(t, targetBase)

			retainer := pathretention.NewPathRetainer(2) // 2 workers

			err := retainer.Prune(context.Background(), targetBase, toPrune, &tc.plan, now)
			if tc.expectError != nil {
				if !errors.Is(err, tc.expectError) {
					t.Errorf("Expected error %v, got %v", tc.expectError, err)
				}
			} else {
				if err != nil {
					t.Fatalf("Prune failed: %v", err)
				}
			}

			// Verify Kept
			for _, name := range tc.expectKept {
				p := filepath.Join(targetBase, name)
				if _, err := os.Stat(p); os.IsNotExist(err) {
					t.Errorf("Expected %s to be kept, but it is missing", name)
				}
			}

			// Verify Deleted
			for _, name := range tc.expectDeleted {
				p := filepath.Join(targetBase, name)
				if _, err := os.Stat(p); !os.IsNotExist(err) {
					t.Errorf("Expected %s to be deleted, but it exists", name)
				}
			}
		})
	}
}

func createTestBackup(t *testing.T, baseDir, relPath string, timestamp time.Time) metafile.MetafileInfo {
	absPath := filepath.Join(baseDir, relPath)
	if err := os.MkdirAll(absPath, 0755); err != nil {
		t.Fatalf("failed to create backup dir: %v", err)
	}

	return metafile.MetafileInfo{
		RelPathKey: relPath,
		Metadata: metafile.MetafileContent{
			TimestampUTC: timestamp,
		},
	}
}
