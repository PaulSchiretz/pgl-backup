package engine_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/engine"
	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/patharchive"
	"github.com/paulschiretz/pgl-backup/pkg/pathcompression"
	"github.com/paulschiretz/pgl-backup/pkg/pathretention"
	"github.com/paulschiretz/pgl-backup/pkg/pathsync"
	"github.com/paulschiretz/pgl-backup/pkg/planner"
	"github.com/paulschiretz/pgl-backup/pkg/preflight"
)

// --- Mocks ---

type mockValidator struct {
	err error
}

func (m *mockValidator) Run(ctx context.Context, absSourcePath, absTargetBasePath string, p *preflight.Plan, timestampUTC time.Time) error {
	return m.err
}

type mockSyncer struct {
	err        error
	resultInfo metafile.MetafileInfo
}

func (m *mockSyncer) Sync(ctx context.Context, absSourcePath, absTargetBasePath, relCurrentPathKey, relContentPathKey string, p *pathsync.Plan, timestampUTC time.Time) error {
	if m.err != nil {
		return m.err
	}
	// Simulate setting result info
	p.ResultInfo = m.resultInfo
	return nil
}

type mockArchiver struct {
	err error
}

func (m *mockArchiver) Archive(ctx context.Context, absTargetBasePath, relArchivePathKey, backupDirPrefix string, toArchive metafile.MetafileInfo, p *patharchive.Plan, timestampUTC time.Time) error {
	if m.err != nil {
		return m.err
	}
	// Simulate result info
	p.ResultInfo = metafile.MetafileInfo{RelPathKey: "archived_path"}
	return nil
}

type mockRetainer struct {
	err error
}

func (m *mockRetainer) Prune(ctx context.Context, absTargetBasePath string, toPrune []metafile.MetafileInfo, p *pathretention.Plan, timestampUTC time.Time) error {
	return m.err
}

type mockCompressor struct {
	err error
}

func (m *mockCompressor) Compress(ctx context.Context, absTargetBasePath, relContentPathKey string, toCompress []metafile.MetafileInfo, p *pathcompression.Plan, timestampUTC time.Time) error {
	return m.err
}

// TestHelperProcess isn't a real test. It's a helper process that the exec-based
// tests can run. It's a standard pattern for testing code that uses os/exec.
func TestHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	// The arguments passed to the command are available in os.Args.
	// The command structure is: `... -test.run=TestHelperProcess -- <command_line>`
	args := os.Args
	for i, arg := range args {
		if arg == "--" {
			args = args[i+1:]
			break
		}
	}
	if len(args) > 0 && strings.Contains(args[0], "fail") {
		os.Exit(1)
	}
	os.Exit(0)
}

// --- Tests ---

func TestExecuteBackup(t *testing.T) {
	const (
		relCurrent = "current"
		relArchive = "archive"
		relContent = "content"
		prefix     = "backup_"
	)

	tests := []struct {
		name string
		mode planner.Mode

		// Plan configuration
		archiveEnabled     bool
		syncEnabled        bool
		retentionEnabled   bool
		compressionEnabled bool
		failFast           bool
		preBackupHooks     []string
		postBackupHooks    []string
		dryRun             bool

		// Mock behaviors
		preflightErr error
		syncErr      error
		archiveErr   error
		retentionErr error
		compressErr  error

		// Filesystem setup
		setupFS func(t *testing.T, targetDir string)

		// Expectations
		expectError   bool
		errorContains string
		expectedHooks []string // Substrings to match against executed hooks
	}{
		{
			name:               "Incremental Happy Path",
			mode:               planner.Incremental,
			archiveEnabled:     true,
			syncEnabled:        true,
			retentionEnabled:   true,
			compressionEnabled: true,
			setupFS: func(t *testing.T, targetDir string) {
				// Create 'current' backup for archiving
				currentPath := filepath.Join(targetDir, relCurrent)
				if err := os.MkdirAll(currentPath, 0755); err != nil {
					t.Fatal(err)
				}
				meta := metafile.MetafileContent{TimestampUTC: time.Now()}
				if err := metafile.Write(currentPath, meta); err != nil {
					t.Fatal(err)
				}
			},
			expectError: false,
		},
		{
			name:               "Snapshot Happy Path",
			mode:               planner.Snapshot,
			archiveEnabled:     true,
			syncEnabled:        true,
			retentionEnabled:   true,
			compressionEnabled: true,
			expectError:        false,
		},
		{
			name:          "Preflight Failure",
			mode:          planner.Incremental,
			preflightErr:  errors.New("preflight failed"),
			expectError:   true,
			errorContains: "preflight failed",
		},
		{
			name:          "Sync Failure",
			mode:          planner.Incremental,
			syncEnabled:   true,
			syncErr:       errors.New("sync failed"),
			expectError:   true,
			errorContains: "error during sync",
		},
		{
			name:           "Incremental Archive Failure (FailFast=True)",
			mode:           planner.Incremental,
			archiveEnabled: true,
			failFast:       true,
			archiveErr:     errors.New("archive failed"),
			setupFS: func(t *testing.T, targetDir string) {
				// Create 'current' backup so fetchBackup succeeds
				currentPath := filepath.Join(targetDir, relCurrent)
				os.MkdirAll(currentPath, 0755)
				metafile.Write(currentPath, metafile.MetafileContent{})
			},
			expectError:   true,
			errorContains: "Error during archive",
		},
		{
			name:           "Incremental Archive Failure (FailFast=False)",
			mode:           planner.Incremental,
			archiveEnabled: true,
			failFast:       false,
			archiveErr:     errors.New("archive failed"),
			setupFS: func(t *testing.T, targetDir string) {
				currentPath := filepath.Join(targetDir, relCurrent)
				os.MkdirAll(currentPath, 0755)
				metafile.Write(currentPath, metafile.MetafileContent{})
			},
			expectError: false, // Should continue
		},
		{
			name:             "Retention Failure (FailFast=True)",
			mode:             planner.Incremental,
			retentionEnabled: true,
			failFast:         true,
			retentionErr:     errors.New("retention failed"),
			expectError:      true,
			errorContains:    "Error during prune",
		},
		{
			name:             "Retention Failure (FailFast=False)",
			mode:             planner.Incremental,
			retentionEnabled: true,
			failFast:         false,
			retentionErr:     errors.New("retention failed"),
			expectError:      false, // Should continue
		},
		{
			name:               "Compression Failure (FailFast=True)",
			mode:               planner.Incremental,
			compressionEnabled: true,
			failFast:           true,
			compressErr:        errors.New("compression failed"),
			expectError:        true,
			errorContains:      "Error during compress",
		},
		{
			name:               "Compression Failure (FailFast=False)",
			mode:               planner.Incremental,
			compressionEnabled: true,
			failFast:           false,
			compressErr:        errors.New("compression failed"),
			expectError:        false, // Should continue
		},
		{
			name:            "Hooks Execution Success",
			mode:            planner.Incremental,
			preBackupHooks:  []string{"echo pre"},
			postBackupHooks: []string{"echo post"},
			expectedHooks:   []string{"echo pre", "echo post"},
			expectError:     false,
		},
		{
			name:           "Pre-Backup Hook Failure",
			mode:           planner.Incremental,
			preBackupHooks: []string{"fail_hook"},
			expectedHooks:  []string{"fail_hook"},
			expectError:    true,
			errorContains:  "pre-backup hook failed",
		},
		{
			name:            "Post-Backup Hook Failure (Non-Fatal)",
			mode:            planner.Incremental,
			postBackupHooks: []string{"fail_hook"},
			expectedHooks:   []string{"fail_hook"},
			expectError:     false, // Post-backup hooks shouldn't fail the run
		},
		{
			name:            "Dry Run Hooks",
			mode:            planner.Incremental,
			dryRun:          true,
			preBackupHooks:  []string{"echo pre"},
			postBackupHooks: []string{"echo post"},
			expectedHooks:   []string{}, // Hooks are NOT executed in dry run (just logged)
			expectError:     false,
		},
		{
			name:            "Post-Backup Hooks Run After Sync Failure",
			mode:            planner.Incremental,
			syncEnabled:     true,
			syncErr:         errors.New("sync failed"),
			postBackupHooks: []string{"echo post"},
			expectedHooks:   []string{"echo post"},
			expectError:     true,
			errorContains:   "error during sync",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srcDir := t.TempDir()
			targetDir := t.TempDir()

			if tc.setupFS != nil {
				tc.setupFS(t, targetDir)
			}

			// Construct Plan
			plan := &planner.BackupPlan{
				Mode:     tc.mode,
				DryRun:   tc.dryRun,
				FailFast: tc.failFast,
				Paths: planner.PathKeys{
					RelCurrentPathKey: relCurrent,
					RelArchivePathKey: relArchive,
					RelContentPathKey: relContent,
					BackupDirPrefix:   prefix,
				},
				Preflight: &preflight.Plan{},
				Sync: &pathsync.Plan{
					Enabled: tc.syncEnabled,
				},
				Archive: &patharchive.Plan{
					Enabled: tc.archiveEnabled,
				},
				Retention: &pathretention.Plan{
					Enabled: tc.retentionEnabled,
				},
				Compression: &pathcompression.Plan{
					Enabled: tc.compressionEnabled,
				},
				PreBackupHooks:  tc.preBackupHooks,
				PostBackupHooks: tc.postBackupHooks,
			}

			// Mocks
			v := &mockValidator{err: tc.preflightErr}
			s := &mockSyncer{err: tc.syncErr, resultInfo: metafile.MetafileInfo{RelPathKey: "synced_path"}}
			a := &mockArchiver{err: tc.archiveErr}
			r := &mockRetainer{err: tc.retentionErr}
			c := &mockCompressor{err: tc.compressErr}

			runner := engine.NewRunner(v, s, a, r, c)

			// Mock Hook Executor
			var executedHooks []string
			mockExecutor := func(ctx context.Context, name string, arg ...string) *exec.Cmd {
				cmdLine := name
				if len(arg) > 0 {
					cmdLine += " " + strings.Join(arg, " ")
				}
				executedHooks = append(executedHooks, cmdLine)

				cs := []string{"-test.run=TestHelperProcess", "--", cmdLine}
				cmd := exec.CommandContext(ctx, os.Args[0], cs...)
				cmd.Env = []string{"GO_WANT_HELPER_PROCESS=1"}
				return cmd
			}
			runner.SetHookCommandExecutor(mockExecutor)

			err := runner.ExecuteBackup(context.Background(), srcDir, targetDir, plan)

			if tc.expectError {
				if err == nil {
					t.Fatal("expected error, but got nil")
				}
				if tc.errorContains != "" && !strings.Contains(err.Error(), tc.errorContains) {
					t.Errorf("expected error to contain %q, but got: %v", tc.errorContains, err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}

			// Verify hooks
			if len(tc.expectedHooks) > 0 {
				// Check if expected hooks are present in executedHooks
				// Note: This is a simple containment check.
				for _, expected := range tc.expectedHooks {
					found := false
					for _, executed := range executedHooks {
						if strings.Contains(executed, expected) {
							found = true
							break
						}
					}
					if !found {
						t.Errorf("expected hook execution containing %q, but got %v", expected, executedHooks)
					}
				}
			} else if len(executedHooks) > 0 && tc.dryRun {
				t.Errorf("expected no hooks executed in dry run, but got %v", executedHooks)
			}
		})
	}
}

func TestExecutePrune(t *testing.T) {
	const (
		relArchiveInc  = "archive_inc"
		relArchiveSnap = "archive_snap"
		prefix         = "backup_"
	)

	tests := []struct {
		name string

		// Plan
		retentionIncEnabled  bool
		retentionSnapEnabled bool

		// Mocks
		preflightErr error
		retentionErr error

		// Expectations
		expectError   bool
		errorContains string
	}{
		{
			name:                 "Happy Path - Both Enabled",
			retentionIncEnabled:  true,
			retentionSnapEnabled: true,
			expectError:          false,
		},
		{
			name:          "Preflight Failure",
			preflightErr:  errors.New("preflight failed"),
			expectError:   true,
			errorContains: "preflight failed",
		},
		{
			name:                "Retention Failure",
			retentionIncEnabled: true,
			retentionErr:        errors.New("retention failed"),
			expectError:         true,
			errorContains:       "fatal error during prune",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			targetDir := t.TempDir()

			// Ensure archive directories exist for fetchBackups
			os.MkdirAll(filepath.Join(targetDir, relArchiveInc), 0755)
			os.MkdirAll(filepath.Join(targetDir, relArchiveSnap), 0755)

			plan := &planner.PrunePlan{
				Preflight: &preflight.Plan{},
				PathsIncremental: planner.PathKeys{
					RelArchivePathKey: relArchiveInc,
					BackupDirPrefix:   prefix,
				},
				PathsSnapshot: planner.PathKeys{
					RelArchivePathKey: relArchiveSnap,
					BackupDirPrefix:   prefix,
				},
				RetentionIncremental: &pathretention.Plan{
					Enabled: tc.retentionIncEnabled,
				},
				RetentionSnapshot: &pathretention.Plan{
					Enabled: tc.retentionSnapEnabled,
				},
			}

			v := &mockValidator{err: tc.preflightErr}
			s := &mockSyncer{}
			a := &mockArchiver{}
			r := &mockRetainer{err: tc.retentionErr}
			c := &mockCompressor{}

			runner := engine.NewRunner(v, s, a, r, c)

			err := runner.ExecutePrune(context.Background(), targetDir, plan)

			if tc.expectError {
				if err == nil {
					t.Fatal("expected error, but got nil")
				}
				if tc.errorContains != "" && !strings.Contains(err.Error(), tc.errorContains) {
					t.Errorf("expected error to contain %q, but got: %v", tc.errorContains, err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
		})
	}
}
