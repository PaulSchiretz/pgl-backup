package engine

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"pixelgardenlabs.io/pgl-backup/pkg/config"
	"pixelgardenlabs.io/pgl-backup/pkg/plog"
)

// mockSyncer is a mock implementation of pathsync.Syncer for testing.
type mockSyncer struct {
	// Store the arguments that Sync was called with.
	calledWith struct {
		src, dst string
	}
}

// Sync records the src and dst paths it was called with and returns nil.
func (m *mockSyncer) Sync(ctx context.Context, src, dst string, mirror bool, excludeFiles, excludeDirs []string) error {
	m.calledWith.src = src
	m.calledWith.dst = dst
	// To make the test more realistic, we need to simulate the real syncer's
	// behavior of creating the destination directory.
	if err := os.MkdirAll(dst, 0755); err != nil {
		return err
	}
	return nil
}

// Helper to create a dummy engine for testing.
func newTestEngine(cfg config.Config) *Engine {
	e := New(cfg, "test-version")
	return e
}

// Helper to create a temporary directory structure for a backup.
func createTestBackup(t *testing.T, baseDir, name string, backupTime time.Time) {
	t.Helper()
	backupPath := filepath.Join(baseDir, name)
	if err := os.MkdirAll(backupPath, 0755); err != nil {
		t.Fatalf("failed to create test backup dir: %v", err)
	}
	err := writeBackupMetafile(backupPath, "test", "incremental", "/src", backupTime, false)
	if err != nil {
		t.Fatalf("failed to write metafile for test backup: %v", err)
	}
}

func TestShouldRollover(t *testing.T) {
	// Define a fixed location for consistent timezone testing
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("failed to load location: %v", err)
	}

	// Test cases for shouldRollover
	testCases := []struct {
		name              string
		interval          time.Duration
		lastBackup        time.Time
		currentBackup     time.Time
		shouldRollover    bool
		location          *time.Location
		expectPanic       bool // For invalid intervals
		effectiveInterval time.Duration
	}{
		// Sub-Daily Tests (UTC based)
		{
			name:           "Hourly - Same Hour",
			interval:       time.Hour,
			lastBackup:     time.Date(2023, 10, 26, 14, 10, 0, 0, time.UTC),
			currentBackup:  time.Date(2023, 10, 26, 14, 55, 0, 0, time.UTC),
			shouldRollover: false,
		},
		{
			name:           "Hourly - Next Hour",
			interval:       time.Hour,
			lastBackup:     time.Date(2023, 10, 26, 14, 55, 0, 0, time.UTC),
			currentBackup:  time.Date(2023, 10, 26, 15, 05, 0, 0, time.UTC),
			shouldRollover: true,
		},
		// Daily Tests (Local Timezone based)
		{
			name:           "Daily - Same Day",
			interval:       24 * time.Hour,
			lastBackup:     time.Date(2023, 10, 26, 10, 0, 0, 0, location),
			currentBackup:  time.Date(2023, 10, 26, 23, 0, 0, 0, location),
			shouldRollover: false,
		},
		{
			name:           "Daily - Crosses Midnight",
			interval:       24 * time.Hour,
			lastBackup:     time.Date(2023, 10, 26, 23, 55, 0, 0, location),
			currentBackup:  time.Date(2023, 10, 27, 0, 5, 0, 0, location),
			shouldRollover: true,
		},
		{
			name:           "Weekly - No Rollover (Day 3)",
			interval:       7 * 24 * time.Hour,
			lastBackup:     time.Date(2023, 10, 23, 12, 0, 0, 0, location), // Day D
			currentBackup:  time.Date(2023, 10, 26, 12, 0, 0, 0, location), // Day D + 3
			shouldRollover: false,                                          // 3 days is less than 7 days
		},
		{
			name:           "Weekly - Rollover (Day 7 Boundary)",
			interval:       7 * 24 * time.Hour,
			lastBackup:     time.Date(2023, 10, 23, 12, 0, 0, 0, location), // Day D (Bucket B)
			currentBackup:  time.Date(2023, 10, 30, 12, 0, 0, 0, location), // Day D + 7 (Bucket B + 1)
			shouldRollover: true,                                           // Exactly 7 calendar days have elapsed, crossing the bucket
		},
		// Daylight Saving Time Change Test
		{
			name:           "Daily - Across DST Fallback",
			interval:       24 * time.Hour,
			lastBackup:     time.Date(2023, 11, 4, 12, 0, 0, 0, location), // Before DST change
			currentBackup:  time.Date(2023, 11, 5, 12, 0, 0, 0, location), // After DST change (day becomes 25 hours long)
			shouldRollover: true,
		},
		{
			name:           "Daily - Across DST Spring Forward",
			interval:       24 * time.Hour,
			lastBackup:     time.Date(2023, 3, 11, 12, 0, 0, 0, location), // Before DST change
			currentBackup:  time.Date(2023, 3, 12, 12, 0, 0, 0, location), // After DST change (day is 23 hours long)
			shouldRollover: true,
		},
		// Zero/Negative Interval Test
		{
			name:              "Zero Interval - Defaults to 24h",
			interval:          0,
			effectiveInterval: 24 * time.Hour,
			lastBackup:        time.Date(2023, 10, 26, 23, 55, 0, 0, location),
			currentBackup:     time.Date(2023, 10, 27, 0, 5, 0, 0, location),
			shouldRollover:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			cfg := config.NewDefault()
			cfg.RolloverInterval = tc.interval

			// Override effective interval for tests that check default behavior
			if tc.effectiveInterval > 0 {
				cfg.RolloverInterval = tc.effectiveInterval
			}

			e := newTestEngine(cfg)
			e.currentTimestampUTC = tc.currentBackup

			// Act
			result := e.shouldRollover(tc.lastBackup)

			// Assert
			if tc.shouldRollover != result {
				t.Errorf("expected shouldRollover to be %v, but got %v", tc.shouldRollover, result)
			}
		})
	}
}

func TestDetermineBackupsToKeep(t *testing.T) {
	// Arrange: Create a series of backups over time
	now := time.Now()
	allBackups := []backupInfo{
		{Time: now.Add(-1 * time.Hour), Name: "backup_hourly_1"},        // Kept as hourly
		{Time: now.Add(-2 * time.Hour), Name: "backup_hourly_2"},        // Kept as hourly
		{Time: now.Add(-25 * time.Hour), Name: "backup_daily_1"},        // Kept as daily
		{Time: now.Add(-49 * time.Hour), Name: "backup_daily_2"},        // Kept as daily
		{Time: now.Add(-8 * 24 * time.Hour), Name: "backup_weekly_1"},   // Kept as weekly
		{Time: now.Add(-15 * 24 * time.Hour), Name: "backup_weekly_2"},  // Kept as weekly
		{Time: now.Add(-35 * 24 * time.Hour), Name: "backup_monthly_1"}, // Kept as monthly
		{Time: now.Add(-65 * 24 * time.Hour), Name: "backup_monthly_2"}, // Kept as monthly
		{Time: now.Add(-100 * 24 * time.Hour), Name: "backup_old_1"},    // To be deleted
		{Time: now.Add(-200 * 24 * time.Hour), Name: "backup_old_2"},    // To be deleted
	}

	policy := config.BackupRetentionPolicyConfig{
		Hours:  2,
		Days:   2,
		Weeks:  2,
		Months: 2,
	}

	e := newTestEngine(config.NewDefault())

	// Act
	kept := e.determineBackupsToKeep(allBackups, policy)

	// Assert
	if len(kept) != 8 {
		t.Errorf("expected to keep 8 backups, but got %d", len(kept))
	}

	expectedToKeep := []string{
		"backup_hourly_1", "backup_hourly_2",
		"backup_daily_1", "backup_daily_2",
		"backup_weekly_1", "backup_weekly_2",
		"backup_monthly_1", "backup_monthly_2",
	}
	for _, name := range expectedToKeep {
		if !kept[name] {
			t.Errorf("expected backup %s to be kept, but it was not", name)
		}
	}

	expectedToDelete := []string{"backup_old_1", "backup_old_2"}
	for _, name := range expectedToDelete {
		if kept[name] {
			t.Errorf("expected backup %s to be deleted, but it was kept", name)
		}
	}
}

func TestPerformSync_PreserveSourceDirectoryName(t *testing.T) {
	// Suppress log output for this test to keep the test output clean.
	var buf bytes.Buffer
	plog.SetOutput(&buf)
	t.Cleanup(func() { plog.SetOutput(os.Stderr) })

	srcDir := t.TempDir()
	targetBase := t.TempDir()

	testCases := []struct {
		name                        string
		preserveSourceDirectoryName bool
		expectedDst                 string
	}{
		{
			name:                        "PreserveSourceDirectoryName is true",
			preserveSourceDirectoryName: true,
			expectedDst:                 filepath.Join(targetBase, "current", filepath.Base(srcDir)),
		},
		{
			name:                        "PreserveSourceDirectoryName is false",
			preserveSourceDirectoryName: false,
			expectedDst:                 filepath.Join(targetBase, "current"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			cfg := config.NewDefault()
			cfg.Mode = config.IncrementalMode
			cfg.Paths.Source = srcDir
			cfg.Paths.TargetBase = targetBase
			cfg.Naming.IncrementalModeSuffix = "current"
			cfg.Paths.PreserveSourceDirectoryName = tc.preserveSourceDirectoryName

			e := newTestEngine(cfg)
			e.currentTarget = filepath.Join(targetBase, "current") // Set by prepareDestination

			// Inject the mock syncer
			mock := &mockSyncer{}
			e.syncer = mock

			// Act
			err := e.performSync(context.Background())
			if err != nil {
				t.Fatalf("performSync failed: %v", err)
			}

			if mock.calledWith.dst != tc.expectedDst {
				t.Errorf("expected destination path to be %q, but got %q", tc.expectedDst, mock.calledWith.dst)
			}
		})
	}
}

func TestDetermineBackupsToKeep_Promotion(t *testing.T) {
	// This test ensures that a single backup is "promoted" to fill the highest-priority slot.
	// For example, the most recent backup should fill an hourly, daily, weekly, or monthly slot.

	// Arrange
	now := time.Now()
	// This backup set is designed to fill every available retention slot,
	// leaving one backup that is truly redundant and should be deleted.
	allBackups := []backupInfo{
		{Time: now.Add(-1 * time.Hour), Name: "kept_hourly"},         // Fills the hourly slot
		{Time: now.Add(-25 * time.Hour), Name: "kept_daily"},         // Fills the daily slot
		{Time: now.Add(-8 * 24 * time.Hour), Name: "kept_weekly"},    // Fills the weekly slot
		{Time: now.Add(-35 * 24 * time.Hour), Name: "kept_monthly"},  // Fills the monthly slot
		{Time: now.Add(-70 * 24 * time.Hour), Name: "to_be_deleted"}, // Older than all slots, should be deleted
	}

	policy := config.BackupRetentionPolicyConfig{
		Hours:  1,
		Days:   1,
		Weeks:  1,
		Months: 1,
	}

	cfg := config.NewDefault()
	// Set a rollover that is fast enough to not trigger "slow-fill" warnings
	cfg.RolloverInterval = 1 * time.Hour
	e := newTestEngine(cfg)

	// Act
	kept := e.determineBackupsToKeep(allBackups, policy)

	// Assert
	// The logic should keep 4 backups, each filling one slot, and delete the 5th.
	if len(kept) != 4 {
		t.Errorf("expected to keep 4 backups, but got %d", len(kept))
	}
	if !kept["kept_hourly"] {
		t.Error("expected 'kept_hourly' to be kept, but it was not")
	}
	if !kept["kept_daily"] {
		t.Error("expected 'kept_daily' to be kept, but it was not")
	}
	if !kept["kept_weekly"] {
		t.Error("expected 'kept_weekly' to be kept, but it was not")
	}
	if !kept["kept_monthly"] {
		t.Error("expected 'kept_monthly' to be kept, but it was not")
	}
	if kept["to_be_deleted"] {
		t.Error("expected 'to_be_deleted' to be deleted, but it was kept")
	}
}

func TestPerformRollover(t *testing.T) {
	// Arrange
	tempDir := t.TempDir()
	cfg := config.NewDefault()
	cfg.Paths.TargetBase = tempDir
	cfg.Naming.Prefix = "backup_"
	cfg.Naming.IncrementalModeSuffix = "current"
	cfg.RolloverInterval = 24 * time.Hour

	e := newTestEngine(cfg)
	e.currentTimestampUTC = time.Now().UTC() // Today

	// Create a "current" backup from yesterday
	lastBackupTime := e.currentTimestampUTC.Add(-25 * time.Hour)
	currentBackupDirName := "backup_current"
	createTestBackup(t, tempDir, currentBackupDirName, lastBackupTime)

	// Act
	err := e.performRollover(context.Background())
	if err != nil {
		t.Fatalf("performRollover failed: %v", err)
	}

	// Assert
	// 1. The old "current" directory should be gone.
	oldPath := filepath.Join(tempDir, currentBackupDirName)
	_, err = os.Stat(oldPath)
	if !os.IsNotExist(err) {
		t.Errorf("expected old current directory to be renamed, but it still exists or another error occurred: %v", err)
	}

	// 2. A new archived directory should exist.
	archiveTimestamp := lastBackupTime.Format(cfg.Naming.TimeFormat)
	archiveDirName := "backup_" + archiveTimestamp
	newPath := filepath.Join(tempDir, archiveDirName)
	_, err = os.Stat(newPath)
	if err != nil {
		t.Errorf("expected new archive directory to exist, but it does not: %v", err)
	}
}

func TestPerformRollover_NoRollover(t *testing.T) {
	// Arrange
	tempDir := t.TempDir()
	cfg := config.NewDefault()
	cfg.Paths.TargetBase = tempDir
	cfg.Naming.Prefix = "backup_"
	cfg.Naming.IncrementalModeSuffix = "current"
	cfg.RolloverInterval = 24 * time.Hour

	// To test this reliably, we need to control the timestamps precisely.
	// Let's set the "current" run to be on a specific day.
	currentRunTime := time.Date(2023, 10, 27, 14, 0, 0, 0, time.Local)

	e := newTestEngine(cfg)
	e.currentTimestampUTC = currentRunTime

	// Create a "current" backup from a few hours ago (same day)
	// This is the timestamp that will be written to the metafile.
	lastBackupTimeInMeta := currentRunTime.Add(-2 * time.Hour)
	currentBackupDirName := "backup_current"
	createTestBackup(t, tempDir, currentBackupDirName, lastBackupTimeInMeta)

	// Act
	err := e.performRollover(context.Background())
	if err != nil {
		t.Fatalf("performRollover failed: %v", err)
	}

	// Assert
	// The "current" directory should still exist, unchanged.
	currentPath := filepath.Join(tempDir, currentBackupDirName)
	_, err = os.Stat(currentPath)
	if err != nil {
		t.Errorf("current directory should not have been renamed, but it's missing or an error occurred: %v", err)
	}
}

func TestApplyRetentionPolicy(t *testing.T) {
	// Arrange
	tempDir := t.TempDir()
	cfg := config.NewDefault()
	cfg.Paths.TargetBase = tempDir
	cfg.Naming.Prefix = "backup_"
	cfg.RetentionPolicy = config.BackupRetentionPolicyConfig{
		Hours:  0,
		Days:   1, // Keep one daily backup
		Weeks:  0,
		Months: 0,
	}

	e := newTestEngine(cfg)
	now := time.Now()

	// Create backups to be kept and deleted
	createTestBackup(t, tempDir, "backup_kept", now.Add(-1*24*time.Hour))
	createTestBackup(t, tempDir, "backup_to_delete", now.Add(-5*24*time.Hour))

	// Create a non-backup directory that should be ignored
	if err := os.Mkdir(filepath.Join(tempDir, "not_a_backup"), 0755); err != nil {
		t.Fatalf("failed to create non-backup dir: %v", err)
	}

	// Act
	err := e.applyRetentionPolicy(context.Background())
	if err != nil {
		t.Fatalf("applyRetentionPolicy failed: %v", err)
	}

	// Assert
	// Kept backup should exist
	_, err = os.Stat(filepath.Join(tempDir, "backup_kept"))
	if err != nil {
		t.Errorf("expected kept backup to exist, but it does not: %v", err)
	}

	// Deleted backup should not exist
	_, err = os.Stat(filepath.Join(tempDir, "backup_to_delete"))
	if !os.IsNotExist(err) {
		t.Errorf("expected old backup to be deleted, but it still exists or another error occurred: %v", err)
	}

	// Ignored directory should exist
	_, err = os.Stat(filepath.Join(tempDir, "not_a_backup"))
	if err != nil {
		t.Errorf("expected non-backup directory to be ignored, but it was deleted or an error occurred: %v", err)
	}
}

func TestPrepareDestination(t *testing.T) {
	t.Run("Snapshot Mode", func(t *testing.T) {
		// Arrange
		tempDir := t.TempDir()
		cfg := config.NewDefault()
		cfg.Mode = config.SnapshotMode
		cfg.Paths.TargetBase = tempDir
		cfg.Naming.Prefix = "snap_"
		cfg.Naming.TimeFormat = "2006-01-02"

		e := newTestEngine(cfg)
		e.currentTimestampUTC = time.Date(2023, 10, 27, 0, 0, 0, 0, time.UTC)

		// Act
		err := e.prepareDestination(context.Background())
		if err != nil {
			t.Fatalf("prepareDestination failed: %v", err)
		}

		// Assert
		expectedPath := filepath.Join(tempDir, "snap_2023-10-27")
		if expectedPath != e.currentTarget {
			t.Errorf("expected target path %q, but got %q", expectedPath, e.currentTarget)
		}
	})

	t.Run("Incremental Mode", func(t *testing.T) {
		// Arrange
		tempDir := t.TempDir()
		cfg := config.NewDefault()
		cfg.Mode = config.IncrementalMode
		cfg.Paths.TargetBase = tempDir
		cfg.Naming.Prefix = "incr_"
		cfg.Naming.IncrementalModeSuffix = "latest"

		e := newTestEngine(cfg)
		e.currentTimestampUTC = time.Now().UTC()

		// Act
		err := e.prepareDestination(context.Background())
		if err != nil {
			t.Fatalf("prepareDestination failed: %v", err)
		}

		// Assert
		expectedPath := filepath.Join(tempDir, "incr_latest")
		if expectedPath != e.currentTarget {
			t.Errorf("expected target path %q, but got %q", expectedPath, e.currentTarget)
		}
	})
}

func TestFetchSortedBackups(t *testing.T) {
	// Arrange
	tempDir := t.TempDir()
	cfg := config.NewDefault()
	cfg.Naming.Prefix = "backup_"
	e := newTestEngine(cfg)

	now := time.Now()
	backup1Time := now.Add(-10 * time.Hour)
	backup2Time := now.Add(-5 * time.Hour) // Newest
	backup3Time := now.Add(-20 * time.Hour)

	createTestBackup(t, tempDir, "backup_1", backup1Time)
	createTestBackup(t, tempDir, "backup_2_newest", backup2Time)
	createTestBackup(t, tempDir, "backup_3", backup3Time)

	// Create a directory without a metafile that should be ignored
	if err := os.Mkdir(filepath.Join(tempDir, "backup_no_meta"), 0755); err != nil {
		t.Fatalf("failed to create dir without metafile: %v", err)
	}

	// Act
	backups, err := e.fetchSortedBackups(context.Background(), tempDir, "non_existent_current_dir")
	if err != nil {
		t.Fatalf("fetchSortedBackups failed: %v", err)
	}

	// Assert
	if len(backups) != 3 {
		t.Fatalf("expected to find 3 valid backups, but got %d", len(backups))
	}

	// Check that they are sorted newest to oldest
	expectedOrder := []string{"backup_2_newest", "backup_1", "backup_3"}
	for i, name := range expectedOrder {
		if backups[i].Name != name {
			t.Errorf("expected backup at index %d to be %s, but got %s", i, name, backups[i].Name)
		}
	}

	expectedTimes := []time.Time{backup2Time, backup1Time, backup3Time}
	for i, ti := range expectedTimes {
		if !backups[i].Time.Equal(ti) {
			t.Errorf("expected backup time at index %d to be %v, but got %v", i, ti, backups[i].Time)
		}
	}
}
