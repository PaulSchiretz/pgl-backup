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
	"pixelgardenlabs.io/pgl-backup/pkg/metafile"
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

// mockArchiver is a mock implementation of patharchive.Archiver for testing.
type mockArchiver struct {
	archiveCalled bool
	err           error
}

func (m *mockArchiver) Archive(ctx context.Context, archivesDir, currentBackupPath string, currentBackupUTC time.Time) error {
	m.archiveCalled = true
	return m.err
}

// mockRetentionManager is a mock implementation of pathretention.RetentionManager for testing.
type mockRetentionManager struct {
	applyCalled bool
	err         error
}

func (m *mockRetentionManager) Apply(ctx context.Context, policyTitle, dirPath string, retentionPolicy config.RetentionPolicyConfig, excludedDir string) error {
	m.applyCalled = true
	return m.err
}

// Helper to create a dummy engine for testing.
func newTestEngine(cfg config.Config) *Engine {
	e := New(cfg, "test-version")
	return e //nolint:all // This is a test helper, not production code.
}

// Helper to create a temporary directory structure for a backup.
func createTestBackup(t *testing.T, e *Engine, baseDir, name string, timestampUTC time.Time) {
	t.Helper()
	backupPath := filepath.Join(baseDir, name)
	// The helper now needs to know if it should create the backup in the archives subdir.
	if err := os.MkdirAll(backupPath, 0755); err != nil {
		t.Fatalf("failed to create test backup dir: %v", err)
	}

	// Create a runState that mirrors what writeBackupMetafile expects.
	runState := &engineRunState{
		//target:              backupPath,
		currentTimestampUTC: timestampUTC,
		source:              "/src", // A dummy source path for the metafile content.
		mode:                config.IncrementalMode,
	}

	if err := metafile.Write(backupPath, metafile.MetafileContent{Version: e.version, TimestampUTC: runState.currentTimestampUTC, Mode: runState.mode.String(), Source: runState.source}); err != nil {
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
		// Check that the error is of the specific type *exec.ExitError.
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Errorf("expected error to be of type *exec.ExitError, but got %T", err)
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
		// When a command is killed due to context cancellation, the error returned by cmd.Run()
		// will wrap context.Canceled. We use errors.Is to check for this.
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
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
			expectedDst:                 filepath.Join(targetBase, "test_current", "PGL_Backup_Content", filepath.Base(srcDir)),
		},
		{
			name:                        "PreserveSourceDirectoryName is false",
			preserveSourceDirectoryName: false,
			expectedDst:                 filepath.Join(targetBase, "test_current", "PGL_Backup_Content"),
		},
		{
			name:                        "PreserveSourceDirectoryName is true - Windows Drive Root",
			preserveSourceDirectoryName: true,
			expectedDst:                 filepath.Join(targetBase, "test_current", "PGL_Backup_Content", "C"),
			// This test case will only run on Windows, otherwise it will be skipped.
			// On non-Windows, "C:" is not a root, so filepath.Base("C:") would be "C:".
			// The logic should handle this gracefully.
			srcDir: "C:\\",
		},
		{
			name:                        "PreserveSourceDirectoryName is true - Unix Root",
			preserveSourceDirectoryName: true,
			expectedDst:                 filepath.Join(targetBase, "test_current", "PGL_Backup_Content"), // Should not append anything for "/"
			srcDir:                      "/",
		},
		{
			name:                        "PreserveSourceDirectoryName is true - Relative Path",
			preserveSourceDirectoryName: true,
			expectedDst:                 filepath.Join(targetBase, "test_current", "PGL_Backup_Content", "my_relative_dir"),
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
			currentRun := &engineRunState{
				target:              filepath.Join(targetBase, cfg.Paths.IncrementalSubDir),
				currentTimestampUTC: time.Now().UTC(),
				source:              cfg.Paths.Source,
			}

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

func TestPrepareDestination_IncrementalMode(t *testing.T) {
	// Arrange
	tempDir := t.TempDir()
	cfg := config.NewDefault()
	cfg.Mode = config.IncrementalMode
	cfg.Paths.TargetBase = tempDir
	cfg.Paths.IncrementalSubDir = "latest"

	e := newTestEngine(cfg)
	currentRun := &engineRunState{
		currentTimestampUTC: time.Now().UTC(),
		source:              cfg.Paths.Source,
		mode:                cfg.Mode,
	}

	// Create a dummy "current" backup to trigger the archiver
	createTestBackup(t, e, tempDir, cfg.Paths.IncrementalSubDir, time.Now().Add(-25*time.Hour))

	// Inject a mock archiver
	mock := &mockArchiver{}
	e.archiver = mock

	// Act
	err := e.prepareRun(context.Background(), currentRun)
	if err != nil {
		t.Fatalf("prepareDestination failed: %v", err)
	}

	// Assert
	// 1. The archiver should have been called because a "current" backup existed.
	if !mock.archiveCalled {
		t.Error("expected archiver.Archive to be called, but it was not")
	}

	// 2. The target for the new sync should be the incremental directory.
	expectedPath := filepath.Join(tempDir, cfg.Paths.IncrementalSubDir)
	if expectedPath != currentRun.target {
		t.Errorf("expected target path %q, but got %q", expectedPath, currentRun.target)
	}
}

func TestExecuteBackup_Retention(t *testing.T) {
	// Arrange
	tempDir := t.TempDir()
	cfg := config.NewDefault()
	cfg.Paths.Source = t.TempDir() // Valid source
	cfg.Paths.TargetBase = tempDir
	cfg.Retention.Incremental.Enabled = true // Enable retention
	cfg.Retention.Snapshot.Enabled = true    // Enable retention

	e := newTestEngine(cfg)

	// Inject mocks for all components to isolate the test
	mockSyncer := &mockSyncer{}
	mockArchiver := &mockArchiver{}
	mockRetention := &mockRetentionManager{}
	e.syncer = mockSyncer
	e.archiver = mockArchiver
	e.retentionManager = mockRetention

	// Act
	// We don't care about the error, only that the retention manager was called.
	// We also ignore the lock file error for this test.
	_ = os.Remove(filepath.Join(tempDir, config.LockFileName))
	_ = e.ExecuteBackup(context.Background())

	// Assert
	if !mockRetention.applyCalled {
		t.Error("expected retentionManager.Apply to be called, but it was not")
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
		currentRun := &engineRunState{
			currentTimestampUTC: testTime,
			source:              cfg.Paths.Source,
			mode:                cfg.Mode,
		}

		// Act
		err := e.prepareRun(context.Background(), currentRun)
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
		currentRun := &engineRunState{
			currentTimestampUTC: time.Now().UTC(),
			source:              cfg.Paths.Source,
			mode:                cfg.Mode,
		}

		// Act
		err := e.prepareRun(context.Background(), currentRun)
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
