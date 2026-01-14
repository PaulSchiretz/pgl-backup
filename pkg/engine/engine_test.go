package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/config"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

// mockSyncer is a mock implementation of pathsync.Syncer for testing.
type mockSyncer struct {
	// Store the arguments that Sync was called with.
	calledWith struct {
		source, target string
		enableMetrics  bool
	}
}

// Sync records the src and dst paths it was called with and returns nil.
func (m *mockSyncer) Sync(ctx context.Context, source, target string, preserveSourceDirName, mirror bool, excludeFiles, excludeDirs []string, enableMetrics bool) error {
	m.calledWith.source = source
	m.calledWith.target = target
	m.calledWith.enableMetrics = enableMetrics
	// To make the test more realistic, we need to simulate the real syncer's
	// behavior of creating the destination directory.
	if err := os.MkdirAll(target, 0755); err != nil {
		return err
	}
	return nil
}

// mockArchiver is a mock implementation of patharchive.Archiver for testing.
type mockArchiver struct {
	archiveCalled bool
	err           error
}

func (m *mockArchiver) Archive(ctx context.Context, archivesDir, currentBackupPath string, currentBackupUTC time.Time, archivePolicy config.ArchivePolicyConfig, retentionPolicy config.RetentionPolicyConfig) (string, error) {
	m.archiveCalled = true
	return "", m.err
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

// mockCompressionManager is a mock implementation of pathcompression.CompressionManager for testing.
type mockCompressionManager struct {
	compressCalled bool
	err            error
}

func (m *mockCompressionManager) Compress(ctx context.Context, backups []string, policy config.CompressionPolicyConfig) error {
	m.compressCalled = true
	return m.err
}

// Helper to create a dummy engine for testing.
func newTestEngine(cfg config.Config) *Engine {
	e := New(cfg, "test-version")
	return e //nolint:all // This is a test helper, not production code.
}

func TestInitializeBackupTarget(t *testing.T) {
	t.Run("Happy Path - Creates config file", func(t *testing.T) {
		// Arrange
		srcDir := t.TempDir()
		targetDir := t.TempDir()

		cfg := config.NewDefault("test-version")
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

		cfg := config.NewDefault("test-version")
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

// TestExecutePrune_OnNonExistentTarget verifies that ExecutePrune fails and does not create the target
func TestExecutePrune_OnNonExistentTarget(t *testing.T) {
	// Arrange
	tempDir := t.TempDir()
	targetDir := filepath.Join(tempDir, "non_existent_target")

	cfg := config.NewDefault("test-version")
	cfg.Paths.TargetBase = targetDir
	cfg.Paths.Source = t.TempDir() // Valid source

	e := newTestEngine(cfg)

	// Act
	err := e.ExecutePrune(context.Background())

	// Assert
	if err == nil {
		t.Error("expected ExecutePrune to fail when target does not exist, but it succeeded")
	}

	if _, err := os.Stat(targetDir); !os.IsNotExist(err) {
		t.Error("expected target directory NOT to be created, but it was")
	}
}

func TestPerformCompression(t *testing.T) {
	t.Run("Empty List", func(t *testing.T) {
		// Arrange
		cfg := config.NewDefault("test-version")
		e := newTestEngine(cfg)
		mock := &mockCompressionManager{}
		e.compressionManager = mock

		// Act
		runState := &engineRunState{
			doCompression:            true,
			compressionPolicy:        config.CompressionPolicyConfig{Enabled: true, Format: config.TarZstFormat},
			absBackupPathsToCompress: []string{},
		}
		// The new performCompression takes the runState.
		err := e.performCompression(context.Background(), runState)

		// Assert
		if err != nil {
			t.Errorf("expected no error, but got: %v", err)
		}
		if mock.compressCalled {
			t.Error("expected compressionManager.Compress NOT to be called for empty list, but it was")
		}
	})

	t.Run("Non-Empty List", func(t *testing.T) {
		// Arrange
		cfg := config.NewDefault("test-version")
		e := newTestEngine(cfg)
		mock := &mockCompressionManager{}
		e.compressionManager = mock

		backups := []string{"/path/to/backup1", "/path/to/backup2"}

		// Act
		runState := &engineRunState{
			doCompression:            true,
			compressionPolicy:        config.CompressionPolicyConfig{Enabled: true, Format: config.TarZstFormat},
			absBackupPathsToCompress: backups,
		}
		err := e.performCompression(context.Background(), runState)

		// Assert
		if err != nil {
			t.Errorf("expected no error, but got: %v", err)
		}
		if !mock.compressCalled {
			t.Error("expected compressionManager.Compress to be called, but it was not")
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
		cfg := config.NewDefault("test-version")
		e := newTestEngine(cfg)
		err := e.runHooks(context.Background(), []string{}, "test")
		if err != nil {
			t.Errorf("expected no error for empty hooks, but got: %v", err)
		}
	})

	t.Run("Successful Hook", func(t *testing.T) {
		cfg := config.NewDefault("test-version")
		e := newTestEngine(cfg)
		e.hookCommandExecutor = mockExecCommand // Inject our mock executor

		err := e.runHooks(context.Background(), []string{"hook_success"}, "test")
		if err != nil {
			t.Errorf("expected no error for successful hook, but got: %v", err)
		}
	})

	t.Run("Failing Hook", func(t *testing.T) {
		cfg := config.NewDefault("test-version")
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
		cfg := config.NewDefault("test-version")
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
		cfg := config.NewDefault("test-version")
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

func TestExecuteBackup_Retention(t *testing.T) {
	// Arrange
	tempDir := t.TempDir()
	cfg := config.NewDefault("test-version")
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

func TestInitBackupRun(t *testing.T) {
	t.Run("Snapshot Mode", func(t *testing.T) {
		// Arrange
		tempDir := t.TempDir()
		cfg := config.NewDefault("test-version")
		cfg.Paths.TargetBase = tempDir
		cfg.Paths.SnapshotSubDirs.Archive = "snapshots"
		cfg.Naming.Prefix = "snap_"
		cfg.Retention.Snapshot.Enabled = true
		cfg.Compression.Snapshot.Enabled = true

		e := newTestEngine(cfg)

		// Act
		runState, err := e.initBackupRun(config.SnapshotMode)
		if err != nil {
			t.Fatalf("initBackupRun failed: %v", err)
		}

		// Assert
		// Check run state properties
		if runState.mode != config.SnapshotMode {
			t.Errorf("expected mode to be Snapshot, got %v", runState.mode)
		}

		if !runState.doSync {
			t.Error("expected doSync to be true for snapshot mode")
		}
		if !runState.doRetention {
			t.Error("expected doRetention to be true for snapshot mode")
		}
		if !runState.doCompression {
			t.Error("expected doCompression to be true for snapshot mode")
		}
		if runState.doArchiving {
			t.Error("expected doArchiving to be false for snapshot mode")
		}

		actualSyncTargetPath := filepath.Join(tempDir, runState.relSyncTargetPath)

		// Check target path
		snapshotsDir := filepath.Join(tempDir, cfg.Paths.SnapshotSubDirs.Archive)
		if !strings.HasPrefix(actualSyncTargetPath, snapshotsDir) {
			t.Errorf("expected target path to be in snapshots dir %q, but got %q", snapshotsDir, actualSyncTargetPath)
		}
		backupName := filepath.Base(actualSyncTargetPath)
		if !strings.HasPrefix(backupName, cfg.Naming.Prefix) {
			t.Errorf("expected backup name to have prefix %q, but got %q", cfg.Naming.Prefix, backupName)
		}
	})

	t.Run("Incremental Mode", func(t *testing.T) {
		// Arrange
		tempDir := t.TempDir()
		cfg := config.NewDefault("test-version")
		cfg.Paths.TargetBase = tempDir
		cfg.Paths.IncrementalSubDirs.Current = "latest"
		cfg.Retention.Incremental.Enabled = true
		cfg.Compression.Incremental.Enabled = true

		e := newTestEngine(cfg)

		// Act
		runState, err := e.initBackupRun(config.IncrementalMode)
		if err != nil {
			t.Fatalf("initBackupRun failed: %v", err)
		}

		// Assert
		// Check run state properties
		if runState.mode != config.IncrementalMode {
			t.Errorf("expected mode to be Incremental, got %v", runState.mode)
		}

		if !runState.doSync {
			t.Error("expected doSync to be true for incremental mode")
		}

		if !runState.doRetention {
			t.Error("expected doRetention to be true for incremental mode")
		}
		if !runState.doCompression {
			t.Error("expected doCompression to be true for incremental mode")
		}
		if !runState.doArchiving {
			t.Error("expected doArchiving to be true for incremental mode")
		}

		// Check target path
		actualSyncTargetPath := filepath.Join(tempDir, runState.relSyncTargetPath)
		expectedPath := filepath.Join(tempDir, cfg.Paths.IncrementalSubDirs.Current)
		if expectedPath != actualSyncTargetPath {
			t.Errorf("expected target path %q, but got %q", expectedPath, actualSyncTargetPath)
		}
	})
}
