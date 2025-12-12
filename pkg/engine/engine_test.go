package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"pixelgardenlabs.io/pgl-backup/pkg/config"
	"pixelgardenlabs.io/pgl-backup/pkg/plog"
)

// mockSyncer is a mock implementation of pathsync.Syncer for testing.
type mockSyncer struct {
	// Store the arguments that Sync was called with.
	calledWith struct {
		src, dst      string
		enableMetrics bool
	}
}

// Sync records the src and dst paths it was called with and returns nil.
func (m *mockSyncer) Sync(ctx context.Context, src, dst string, mirror bool, excludeFiles, excludeDirs []string, enableMetrics bool) error {
	m.calledWith.src = src
	m.calledWith.dst = dst
	m.calledWith.enableMetrics = enableMetrics
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
	// The helper now needs to know if it should create the backup in the archives subdir.
	if err := os.MkdirAll(backupPath, 0755); err != nil {
		t.Fatalf("failed to create test backup dir: %v", err)
	}
	err := writeBackupMetafile(backupPath, "test", "incremental", "/src", backupTime, false)
	if err != nil {
		t.Fatalf("failed to write metafile for test backup: %v", err)
	}
}

func TestInitializeBackupTarget(t *testing.T) {
	t.Run("Happy Path - Creates config file", func(t *testing.T) {
		// Arrange
		srcDir := t.TempDir()
		targetDir := t.TempDir()

		cfg := config.NewDefault()
		cfg.Paths.Source = srcDir
		cfg.Paths.TargetBase = targetDir

		e := newTestEngine(cfg)

		// Act
		err := e.InitializeBackupTarget(context.Background())
		if err != nil {
			t.Fatalf("InitializeBackupTarget failed: %v", err)
		}

		// Assert
		configPath := filepath.Join(targetDir, config.ConfigFileName)
		if _, err := os.Stat(configPath); os.IsNotExist(err) {
			t.Error("expected config file to be created, but it was not")
		}
	})

	t.Run("Failure - Preflight check fails", func(t *testing.T) {
		// Arrange
		targetDir := t.TempDir()

		cfg := config.NewDefault()
		cfg.Paths.Source = "/non/existent/path" // Invalid source to trigger preflight failure
		cfg.Paths.TargetBase = targetDir

		e := newTestEngine(cfg)

		// Act
		err := e.InitializeBackupTarget(context.Background())
		if err == nil {
			t.Fatal("expected an error from preflight check, but got nil")
		}

		// Assert
		configPath := filepath.Join(targetDir, config.ConfigFileName)
		if _, err := os.Stat(configPath); !os.IsNotExist(err) {
			t.Error("expected config file NOT to be created on failure, but it was")
		}
	})

	t.Run("Failure - Lock is already held", func(t *testing.T) {
		// Arrange
		srcDir := t.TempDir()
		targetDir := t.TempDir()

		// Acquire a lock on the target directory beforehand
		lock, err := os.Create(filepath.Join(targetDir, config.LockFileName))
		if err != nil {
			t.Fatalf("failed to create dummy lock file: %v", err)
		}
		defer lock.Close()

		cfg := config.NewDefault()
		cfg.Paths.Source = srcDir
		cfg.Paths.TargetBase = targetDir

		e := newTestEngine(cfg)
		e.InitializeBackupTarget(context.Background())
	})
}

func TestShouldRollover(t *testing.T) {
	// Test cases for shouldRollover
	testCases := []struct {
		name              string
		interval          time.Duration
		lastBackup        time.Time
		currentBackup     time.Time
		shouldRollover    bool
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
		// These tests use UTC but cross the midnight boundary, which shouldRollover
		// will evaluate using the system's local time.
		{
			name:       "Daily - Same Day",
			interval:   24 * time.Hour,
			lastBackup: time.Date(2023, 10, 26, 10, 0, 0, 0, time.UTC),
			// Set current time only a few hours after the last backup. This guarantees
			// they fall on the same local calendar day in any timezone.
			currentBackup:  time.Date(2023, 10, 26, 14, 0, 0, 0, time.UTC),
			shouldRollover: false,
		},
		{
			name:       "Daily - Crosses Midnight",
			interval:   24 * time.Hour,
			lastBackup: time.Date(2023, 10, 26, 14, 0, 0, 0, time.UTC),
			// We set the current time to be 26 hours after the last backup. This guarantees
			// that no matter what the local timezone is (from UTC-12 to UTC+14), the two
			// timestamps will fall on different calendar days.
			currentBackup:  time.Date(2023, 10, 26, 14, 0, 0, 0, time.UTC).Add(26 * time.Hour),
			shouldRollover: true,
		},
		{
			name:           "Weekly - No Rollover (Day 3)",
			interval:       7 * 24 * time.Hour,
			lastBackup:     time.Date(2023, 10, 23, 12, 0, 0, 0, time.UTC), // Day D
			currentBackup:  time.Date(2023, 10, 26, 12, 0, 0, 0, time.UTC), // Day D + 3
			shouldRollover: false,                                          // 3 days is less than 7 days
		},
		{
			name:           "Weekly - Rollover (Day 7 Boundary)",
			interval:       7 * 24 * time.Hour,
			lastBackup:     time.Date(2023, 10, 23, 12, 0, 0, 0, time.UTC), // Day D (Bucket B)
			currentBackup:  time.Date(2023, 10, 30, 12, 0, 0, 0, time.UTC), // Day D + 7 (Bucket B + 1)
			shouldRollover: true,                                           // Exactly 7 calendar days have elapsed, crossing the bucket
		},
		// Daylight Saving Time Change Test
		{
			name:           "Daily - Across DST Fallback",
			interval:       24 * time.Hour,
			lastBackup:     time.Date(2023, 11, 4, 12, 0, 0, 0, time.UTC), // Before DST change
			currentBackup:  time.Date(2023, 11, 5, 12, 0, 0, 0, time.UTC), // After DST change
			shouldRollover: true,
		},
		{
			name:           "Daily - Across DST Spring Forward",
			interval:       24 * time.Hour,
			lastBackup:     time.Date(2023, 3, 11, 12, 0, 0, 0, time.UTC), // Before DST change
			currentBackup:  time.Date(2023, 3, 12, 12, 0, 0, 0, time.UTC), // After DST change
			shouldRollover: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			cfg := config.NewDefault()
			cfg.IncrementalRolloverPolicy.Interval = tc.interval

			e := newTestEngine(cfg)
			currentRun := &runState{timestampUTC: tc.currentBackup}

			// Act
			result := e.shouldRollover(tc.lastBackup, currentRun)

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
		{Time: now.Add(-1 * time.Hour), Name: "backup_hourly_1"},        // Kept as hourly (Hour 1)
		{Time: now.Add(-2 * time.Hour), Name: "backup_hourly_2"},        // Kept as hourly (Hour 2)
		{Time: now.Add(-25 * time.Hour), Name: "backup_daily_1"},        // Kept as daily (Day 1)
		{Time: now.Add(-50 * time.Hour), Name: "backup_daily_2"},        // Kept as daily (Day 2)
		{Time: now.Add(-8 * 24 * time.Hour), Name: "backup_weekly_1"},   // Kept as weekly (Week 1)
		{Time: now.Add(-16 * 24 * time.Hour), Name: "backup_weekly_2"},  // Kept as weekly (Week 2)
		{Time: now.Add(-35 * 24 * time.Hour), Name: "backup_monthly_1"}, // Kept as monthly (Month 1)
		{Time: now.Add(-70 * 24 * time.Hour), Name: "backup_monthly_2"}, // Kept as monthly (Month 2)
		{Time: now.Add(-400 * 24 * time.Hour), Name: "backup_yearly_1"}, // Kept as yearly (Year 1)
		{Time: now.Add(-800 * 24 * time.Hour), Name: "backup_yearly_2"}, // Kept as yearly (Year 2)
		{Time: now.Add(-1200 * 24 * time.Hour), Name: "backup_old_1"},   // To be deleted
		{Time: now.Add(-1600 * 24 * time.Hour), Name: "backup_old_2"},   // To be deleted
	}

	policy := config.BackupRetentionPolicyConfig{
		Enabled: true,
		Hours:   2,
		Days:    2,
		Weeks:   2,
		Months:  2,
		Years:   2,
	}

	e := newTestEngine(config.NewDefault())

	// Act
	kept := e.determineBackupsToKeep(allBackups, policy, "test_policy")

	// Assert
	if len(kept) != 10 {
		t.Errorf("expected to keep 10 backups, but got %d", len(kept))
	}

	expectedToKeep := []string{
		"backup_hourly_1", "backup_hourly_2",
		"backup_daily_1", "backup_daily_2",
		"backup_weekly_1", "backup_weekly_2",
		"backup_monthly_1", "backup_monthly_2",
		"backup_yearly_1", "backup_yearly_2",
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

// TestHelperProcess isn't a real test. It's a helper process that the exec-based
// tests can run. It's a standard pattern for testing code that uses os/exec.
func TestHelperProcess(t *testing.T) {
	// Check if the special environment variable is set. If not, this is a normal
	// test run, so we do nothing.
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}

	// The arguments passed to the command are available in os.Args.
	// The command structure is: `... -test.run=TestHelperProcess -- <shell> <shell_arg> <command>`
	// The actual hook command we want to check is at index 5.
	if len(os.Args) < 6 {
		os.Exit(1)
	}
	command := os.Args[5]

	switch command {
	case "hook_success":
		fmt.Fprintln(os.Stdout, "success output")
		os.Exit(0)
	case "hook_fail":
		fmt.Fprintln(os.Stderr, "failure output")
		os.Exit(1)
	case "hook_sleep":
		time.Sleep(2 * time.Second)
		os.Exit(0)
	default:
		os.Exit(1)
	}
}

func TestRunHooks(t *testing.T) {
	// This helper function sets up the command execution to call our TestHelperProcess.
	// It's a crucial part of mocking os/exec.
	mockExecCommand := func(ctx context.Context, command string, args ...string) *exec.Cmd {
		cs := []string{"-test.run=TestHelperProcess", "--", command}
		cs = append(cs, args...)
		cmd := exec.CommandContext(ctx, os.Args[0], cs...)
		// This is the crucial fix: The real `createHookCommand` sets SysProcAttr
		// on the command it creates. Our mock must preserve this setting when it
		// creates its own command that calls the test helper process.
		originalCmd := exec.CommandContext(ctx, command, args...)
		cmd.SysProcAttr = originalCmd.SysProcAttr
		cmd.Env = []string{"GO_WANT_HELPER_PROCESS=1"}
		return cmd
	}

	// Suppress log output for this test to keep the test output clean.
	var logBuf bytes.Buffer
	plog.SetOutput(&logBuf)
	t.Cleanup(func() { plog.SetOutput(os.Stderr) })

	t.Run("No Hooks", func(t *testing.T) {
		cfg := config.NewDefault()
		e := newTestEngine(cfg)
		err := e.runHooks(context.Background(), []string{}, "test")
		if err != nil {
			t.Errorf("expected no error for empty hooks, but got: %v", err)
		}
	})

	t.Run("Successful Hook", func(t *testing.T) {
		cfg := config.NewDefault()
		e := newTestEngine(cfg)
		e.hookCommandExecutor = mockExecCommand // Inject our mock executor

		err := e.runHooks(context.Background(), []string{"hook_success"}, "test")
		if err != nil {
			t.Errorf("expected no error for successful hook, but got: %v", err)
		}
	})

	t.Run("Failing Hook", func(t *testing.T) {
		cfg := config.NewDefault()
		e := newTestEngine(cfg)
		e.hookCommandExecutor = mockExecCommand // Inject our mock executor

		err := e.runHooks(context.Background(), []string{"hook_fail"}, "test")
		if err == nil {
			t.Fatal("expected an error for failing hook, but got nil")
		}
		if !strings.Contains(err.Error(), "failed: exit status 1") {
			t.Errorf("expected error to contain 'exit status 1', but got: %v", err)
		}
	})

	t.Run("Dry Run", func(t *testing.T) {
		logBuf.Reset()
		cfg := config.NewDefault()
		cfg.DryRun = true
		e := newTestEngine(cfg)
		e.hookCommandExecutor = mockExecCommand

		err := e.runHooks(context.Background(), []string{"hook_success"}, "test")
		if err != nil {
			t.Errorf("expected no error in dry run, but got: %v", err)
		}

		logOutput := logBuf.String()
		if !strings.Contains(logOutput, "[DRY RUN] Would execute command") {
			t.Errorf("expected dry run log message, but got: %q", logOutput)
		}
	})

	t.Run("Context Cancellation", func(t *testing.T) {
		cfg := config.NewDefault()
		e := newTestEngine(cfg)
		e.hookCommandExecutor = mockExecCommand

		ctx, cancel := context.WithCancel(context.Background())
		// Cancel the context almost immediately
		time.AfterFunc(50*time.Millisecond, cancel)

		// Use a hook that sleeps, so it will be interrupted by the cancellation.
		err := e.runHooks(ctx, []string{"hook_sleep"}, "test")

		if err == nil {
			t.Fatal("expected a context cancellation error, but got nil")
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected error to wrap context.Canceled, but got: %v", err)
		}
	})
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
		srcDir                      string // Optional: Override the default srcDir for specific cases
	}{
		{
			name:                        "PreserveSourceDirectoryName is true",
			preserveSourceDirectoryName: true,
			expectedDst:                 filepath.Join(targetBase, "test_current", filepath.Base(srcDir)),
		},
		{
			name:                        "PreserveSourceDirectoryName is false",
			preserveSourceDirectoryName: false,
			expectedDst:                 filepath.Join(targetBase, "test_current"),
		},
		{
			name:                        "PreserveSourceDirectoryName is true - Windows Drive Root",
			preserveSourceDirectoryName: true,
			expectedDst:                 filepath.Join(targetBase, "test_current", "C"),
			// This test case will only run on Windows, otherwise it will be skipped.
			// On non-Windows, "C:" is not a root, so filepath.Base("C:") would be "C:".
			// The logic should handle this gracefully.
			srcDir: "C:\\",
		},
		{
			name:                        "PreserveSourceDirectoryName is true - Unix Root",
			preserveSourceDirectoryName: true,
			expectedDst:                 filepath.Join(targetBase, "test_current"), // Should not append anything for "/"
			srcDir:                      "/",
		},
		{
			name:                        "PreserveSourceDirectoryName is true - Relative Path",
			preserveSourceDirectoryName: true,
			expectedDst:                 filepath.Join(targetBase, "test_current", "my_relative_dir"),
			srcDir:                      "./my_relative_dir",
		},
		// Add more cases as needed, e.g., paths ending with a separator.
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			cfg := config.NewDefault()
			cfg.Mode = config.IncrementalMode

			// Use the test-case specific srcDir if provided, otherwise use the default.
			testSrcDir := srcDir // Default temp dir
			if tc.srcDir != "" {
				testSrcDir = tc.srcDir
			}

			// Skip platform-specific tests on the wrong OS.
			if (runtime.GOOS != "windows" && strings.HasPrefix(testSrcDir, "C:\\")) ||
				(runtime.GOOS == "windows" && testSrcDir == "/") {
				t.Skipf("Skipping platform-specific test case %q on %s", tc.name, runtime.GOOS)
			}

			cfg.Paths.Source = testSrcDir
			cfg.Paths.TargetBase = targetBase
			cfg.Paths.IncrementalSubDir = "test_current"
			cfg.Paths.PreserveSourceDirectoryName = tc.preserveSourceDirectoryName

			e := newTestEngine(cfg)
			currentRun := &runState{target: filepath.Join(targetBase, cfg.Paths.IncrementalSubDir), timestampUTC: time.Now()}

			// Inject the mock syncer
			mock := &mockSyncer{}
			e.syncer = mock

			// Act
			err := e.performSync(context.Background(), currentRun)
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
		{Time: now.Add(-1 * time.Hour), Name: "kept_hourly"},          // Fills the hourly slot
		{Time: now.Add(-25 * time.Hour), Name: "kept_daily"},          // Fills the daily slot
		{Time: now.Add(-8 * 24 * time.Hour), Name: "kept_weekly"},     // Fills the weekly slot
		{Time: now.Add(-35 * 24 * time.Hour), Name: "kept_monthly"},   // Fills the monthly slot
		{Time: now.Add(-400 * 24 * time.Hour), Name: "kept_yearly"},   // Fills the yearly slot
		{Time: now.Add(-800 * 24 * time.Hour), Name: "to_be_deleted"}, // Older than all slots, should be deleted
	}

	policy := config.BackupRetentionPolicyConfig{
		Hours:  1,
		Days:   1,
		Weeks:  1,
		Months: 1,
		Years:  1,
	}

	cfg := config.NewDefault()
	// Set a rollover that is fast enough to not trigger "slow-fill" warnings
	cfg.IncrementalRolloverPolicy.Interval = 1 * time.Hour
	e := newTestEngine(cfg)

	// Act
	kept := e.determineBackupsToKeep(allBackups, policy, "test_policy")

	// Assert
	// The logic should keep 5 backups, each filling one slot, and delete the 6th.
	if len(kept) != 5 {
		t.Errorf("expected to keep 5 backups, but got %d", len(kept))
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
	if !kept["kept_yearly"] {
		t.Error("expected 'kept_yearly' to be kept, but it was not")
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
	cfg.Paths.ArchivesSubDir = "Archives" // Explicitly set for test clarity
	cfg.IncrementalRolloverPolicy.Interval = 24 * time.Hour

	e := newTestEngine(cfg)
	currentRun := &runState{timestampUTC: time.Now().UTC().Add(25 * time.Hour)}

	// Create a "current" backup from yesterday
	lastBackupTime := currentRun.timestampUTC.Add(-25 * time.Hour)
	currentBackupDirName := cfg.Paths.IncrementalSubDir
	createTestBackup(t, tempDir, currentBackupDirName, lastBackupTime)

	// Act
	err := e.performRollover(context.Background(), currentRun)
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
	archiveTimestamp := config.FormatTimestampWithOffset(lastBackupTime)
	archiveDirName := "PGL_Backup_" + archiveTimestamp
	archivesSubDir := filepath.Join(tempDir, cfg.Paths.ArchivesSubDir)
	newPath := filepath.Join(archivesSubDir, archiveDirName)
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
	cfg.Paths.IncrementalSubDir = "test_current"
	cfg.IncrementalRolloverPolicy.Interval = 24 * time.Hour

	// To test this reliably, we need to control the timestamps precisely.
	// Let's set the "current" run to be on a specific day.
	currentRunTime := time.Date(2023, 10, 27, 14, 0, 0, 0, time.Local)

	e := newTestEngine(cfg)
	currentRun := &runState{timestampUTC: currentRunTime}

	// Create a "current" backup from a few hours ago (same day)
	// This is the timestamp that will be written to the metafile.
	lastBackupTimeInMeta := currentRunTime.Add(-2 * time.Hour)
	currentBackupDirName := cfg.Paths.IncrementalSubDir
	createTestBackup(t, tempDir, currentBackupDirName, lastBackupTimeInMeta)

	// Act
	err := e.performRollover(context.Background(), currentRun)
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
	cfg.Paths.ArchivesSubDir = "archives"
	cfg.Naming.Prefix = "backup_"
	cfg.IncrementalRetentionPolicy = config.BackupRetentionPolicyConfig{
		Enabled: true,
		Hours:   0,
		Days:    1, // Keep one daily backup
		Weeks:   0,
		Months:  0,
	}

	e := newTestEngine(cfg)
	now := time.Now()

	// The retention policy operates on the archives subdirectory.
	archivesDir := filepath.Join(tempDir, cfg.Paths.ArchivesSubDir)
	os.MkdirAll(archivesDir, 0755)

	// Create backups to be kept and deleted
	createTestBackup(t, archivesDir, "backup_kept", now.Add(-1*24*time.Hour))
	createTestBackup(t, archivesDir, "backup_to_delete", now.Add(-5*24*time.Hour))

	// Create a non-backup directory that should be ignored
	if err := os.Mkdir(filepath.Join(archivesDir, "not_a_backup"), 0755); err != nil {
		t.Fatalf("failed to create non-backup dir: %v", err)
	}

	// Act
	err := e.applyRetentionFor(context.Background(), "test", archivesDir, cfg.IncrementalRetentionPolicy, "")
	if err != nil {
		t.Fatalf("applyRetentionPolicy failed: %v", err)
	}

	// Assert
	// Kept backup should exist
	_, err = os.Stat(filepath.Join(archivesDir, "backup_kept"))
	if err != nil {
		t.Errorf("expected kept backup to exist, but it does not: %v", err)
	}

	// Deleted backup should not exist
	_, err = os.Stat(filepath.Join(archivesDir, "backup_to_delete"))
	if !os.IsNotExist(err) {
		t.Errorf("expected old backup to be deleted, but it still exists or another error occurred: %v", err)
	}

	// Ignored directory should exist
	_, err = os.Stat(filepath.Join(archivesDir, "not_a_backup"))
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
		cfg.Paths.SnapshotsSubDir = "snapshots"
		cfg.Naming.Prefix = "snap_"

		e := newTestEngine(cfg)
		testTime := time.Date(2023, 10, 27, 14, 30, 0, 0, time.UTC)
		currentRun := &runState{timestampUTC: testTime}

		// Act
		err := e.prepareDestination(context.Background(), currentRun)
		if err != nil {
			t.Fatalf("prepareDestination failed: %v", err)
		}

		// Assert
		snapshotsDir := filepath.Join(tempDir, cfg.Paths.SnapshotsSubDir)
		expectedPath := filepath.Join(snapshotsDir, "snap_"+config.FormatTimestampWithOffset(testTime))
		if expectedPath != currentRun.target {
			t.Errorf("expected target path %q, but got %q", expectedPath, currentRun.target)
		}
	})

	t.Run("Incremental Mode", func(t *testing.T) {
		// Arrange
		tempDir := t.TempDir()
		cfg := config.NewDefault()
		cfg.Mode = config.IncrementalMode
		cfg.Paths.TargetBase = tempDir
		cfg.Paths.IncrementalSubDir = "latest"

		e := newTestEngine(cfg)
		currentRun := &runState{timestampUTC: time.Now().UTC()}

		// Act
		err := e.prepareDestination(context.Background(), currentRun)
		if err != nil {
			t.Fatalf("prepareDestination failed: %v", err)
		}

		// Assert
		expectedPath := filepath.Join(tempDir, cfg.Paths.IncrementalSubDir)
		if expectedPath != currentRun.target {
			t.Errorf("expected target path %q, but got %q", expectedPath, currentRun.target)
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
