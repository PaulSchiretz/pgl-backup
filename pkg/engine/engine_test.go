package engine_test

import (
	"bytes"
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
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/preflight"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// --- Mocks ---

type mockValidator struct {
	err error
}

func (m *mockValidator) Run(ctx context.Context, absSourcePath, absTargetPath string, p *preflight.Plan, timestampUTC time.Time) error {
	return m.err
}

type mockSyncer struct {
	err               error
	resultInfo        metafile.MetafileInfo
	restoreRelPathKey string
}

func (m *mockSyncer) Sync(ctx context.Context, absBasePath, absSourcePath, relCurrentPathKey, relContentPathKey string, p *pathsync.Plan, timestampUTC time.Time) (metafile.MetafileInfo, error) {
	if m.err != nil {
		return metafile.MetafileInfo{}, m.err
	}
	// Simulate real syncer behavior: ensure the target directory exists.
	// This is required because Runner now writes the metafile to this directory.
	targetPath := filepath.Join(absBasePath, relCurrentPathKey)
	os.MkdirAll(targetPath, 0755)

	// Return the configured result info
	return m.resultInfo, nil
}

func (m *mockSyncer) Restore(ctx context.Context, absBasePath string, relContentPathKey string, toRestore metafile.MetafileInfo, absRestoreTargetPath string, p *pathsync.Plan) error {
	m.restoreRelPathKey = toRestore.RelPathKey
	return m.err
}

type mockArchiver struct {
	err               error
	resultPath        string
	returnEmptyResult bool
}

func (m *mockArchiver) Archive(ctx context.Context, absBasePath, relArchivePathKey, backupNamePrefix string, toArchive metafile.MetafileInfo, p *patharchive.Plan, timestampUTC time.Time) (metafile.MetafileInfo, error) {
	if m.err != nil {
		return metafile.MetafileInfo{}, m.err
	}
	if m.returnEmptyResult {
		return metafile.MetafileInfo{}, nil
	}
	// Simulate result info
	path := "archived_path"
	if m.resultPath != "" {
		path = m.resultPath
	}
	return metafile.MetafileInfo{RelPathKey: path}, nil
}

type mockRetainer struct {
	err     error
	toPrune []metafile.MetafileInfo
}

func (m *mockRetainer) Prune(ctx context.Context, absBasePath string, toPrune []metafile.MetafileInfo, p *pathretention.Plan, timestampUTC time.Time) error {
	m.toPrune = toPrune
	return m.err
}

type mockCompressor struct {
	err               error
	extractRelPathKey string
}

func (m *mockCompressor) Compress(ctx context.Context, absBasePath, relContentPathKey string, toCompress metafile.MetafileInfo, p *pathcompression.CompressPlan, timestampUTC time.Time) error {
	return m.err
}

func (m *mockCompressor) Extract(ctx context.Context, absBasePath string, toExtract metafile.MetafileInfo, absExtractTargetPath string, p *pathcompression.ExtractPlan, timestampUTC time.Time) error {
	m.extractRelPathKey = toExtract.RelPathKey
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
		preflightErr       error
		syncErr            error
		archiveErr         error
		retentionErr       error
		compressErr        error
		archiveReturnEmpty bool

		// Filesystem setup
		setupFS func(t *testing.T, baseDir string)

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
			setupFS: func(t *testing.T, baseDir string) {
				// Create 'current' backup for archiving
				currentPath := filepath.Join(baseDir, relCurrent)
				if err := os.MkdirAll(currentPath, 0755); err != nil {
					t.Fatal(err)
				}
				meta := metafile.MetafileContent{TimestampUTC: time.Now()}
				if err := metafile.Write(currentPath, &meta); err != nil {
					t.Fatal(err)
				}
			},
			expectError: false,
		},
		{
			name:               "Incremental Happy Path with Archive and Compression",
			mode:               planner.Incremental,
			archiveEnabled:     true,
			syncEnabled:        true,
			retentionEnabled:   true,
			compressionEnabled: true,
			setupFS: func(t *testing.T, baseDir string) {
				currentPath := filepath.Join(baseDir, relCurrent)
				if err := os.MkdirAll(currentPath, 0755); err != nil {
					t.Fatal(err)
				}
				meta := metafile.MetafileContent{TimestampUTC: time.Now()}
				if err := metafile.Write(currentPath, &meta); err != nil {
					t.Fatal(err)
				}
			},
			// We need the archiver to return a path so compression triggers
			archiveReturnEmpty: false,
			expectError:        false,
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
			setupFS: func(t *testing.T, baseDir string) {
				// Create 'current' backup so fetchBackup succeeds
				currentPath := filepath.Join(baseDir, relCurrent)
				os.MkdirAll(currentPath, 0755)
				metafile.Write(currentPath, &metafile.MetafileContent{})
			},
			expectError:   true,
			errorContains: "error during archive",
		},
		{
			name:           "Incremental Archive Failure (FailFast=False)",
			mode:           planner.Incremental,
			archiveEnabled: true,
			failFast:       false,
			archiveErr:     errors.New("archive failed"),
			setupFS: func(t *testing.T, baseDir string) {
				currentPath := filepath.Join(baseDir, relCurrent)
				os.MkdirAll(currentPath, 0755)
				metafile.Write(currentPath, &metafile.MetafileContent{})
			},
			expectError: false, // Should continue
		},
		{
			name:           "Archive Nothing To Archive (Ignored)",
			mode:           planner.Incremental,
			archiveEnabled: true,
			archiveErr:     patharchive.ErrNothingToArchive,
			setupFS: func(t *testing.T, baseDir string) {
				// Create 'current' backup so fetchBackup succeeds
				currentPath := filepath.Join(baseDir, relCurrent)
				os.MkdirAll(currentPath, 0755)
				metafile.Write(currentPath, &metafile.MetafileContent{})
			},
			expectError: false,
		},
		{
			name:               "Incremental Archive Success Empty Result (FailFast=True)",
			mode:               planner.Incremental,
			archiveEnabled:     true,
			failFast:           true,
			archiveReturnEmpty: true,
			setupFS: func(t *testing.T, baseDir string) {
				currentPath := filepath.Join(baseDir, relCurrent)
				os.MkdirAll(currentPath, 0755)
				metafile.Write(currentPath, &metafile.MetafileContent{})
			},
			expectError:   true,
			errorContains: "archive succeeded but ResultInfo is empty",
		},
		{
			name:             "Retention Failure (FailFast=True)",
			mode:             planner.Incremental,
			retentionEnabled: true,
			failFast:         true,
			retentionErr:     errors.New("retention failed"),
			expectError:      true,
			errorContains:    "error during prune",
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
			archiveEnabled:     true,
			compressionEnabled: true,
			failFast:           true,
			compressErr:        errors.New("compression failed"),
			setupFS: func(t *testing.T, baseDir string) {
				currentPath := filepath.Join(baseDir, relCurrent)
				os.MkdirAll(currentPath, 0755)
				metafile.Write(currentPath, &metafile.MetafileContent{})
			},
			expectError:   true,
			errorContains: "error during compress",
		},
		{
			name:               "Compression Failure (FailFast=False)",
			mode:               planner.Incremental,
			archiveEnabled:     true,
			compressionEnabled: true,
			failFast:           false,
			compressErr:        errors.New("compression failed"),
			setupFS: func(t *testing.T, baseDir string) {
				currentPath := filepath.Join(baseDir, relCurrent)
				os.MkdirAll(currentPath, 0755)
				metafile.Write(currentPath, &metafile.MetafileContent{})
			},
			expectError: false, // Should continue
		},
		{
			name:               "Compression Skipped if Archive Disabled",
			mode:               planner.Incremental,
			archiveEnabled:     false, // Key: Archive is off
			compressionEnabled: true,  // But compression is on
			// If compression were called, this error would be returned and fail the test.
			compressErr: errors.New("compression should not have been called"),
			expectError: false, // The run should succeed without calling compression.
		},
		{
			name:             "Retention Nothing To Prune (Ignored)",
			mode:             planner.Incremental,
			retentionEnabled: true,
			retentionErr:     pathretention.ErrNothingToPrune,
			expectError:      false,
		},
		{
			name:               "Compression Nothing To Compress (Ignored)",
			mode:               planner.Incremental,
			archiveEnabled:     true,
			compressionEnabled: true,
			compressErr:        pathcompression.ErrNothingToCompress,
			setupFS: func(t *testing.T, baseDir string) {
				currentPath := filepath.Join(baseDir, relCurrent)
				os.MkdirAll(currentPath, 0755)
				metafile.Write(currentPath, &metafile.MetafileContent{})
			},
			expectError: false,
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
		{
			name:          "Metafile Write Failure",
			mode:          planner.Incremental,
			syncEnabled:   true,
			expectError:   true,
			errorContains: "failed to write metafile",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srcDir := t.TempDir()
			baseDir := t.TempDir()

			if tc.setupFS != nil {
				tc.setupFS(t, baseDir)
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
					BackupNamePrefix:  prefix,
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
				Compression: &pathcompression.CompressPlan{
					Enabled: tc.compressionEnabled,
				},
				PreBackupHooks:  tc.preBackupHooks,
				PostBackupHooks: tc.postBackupHooks,
			}

			// Mocks
			v := &mockValidator{err: tc.preflightErr}
			s := &mockSyncer{err: tc.syncErr, resultInfo: metafile.MetafileInfo{RelPathKey: relCurrent}}
			a := &mockArchiver{err: tc.archiveErr, returnEmptyResult: tc.archiveReturnEmpty}

			if tc.name == "Metafile Write Failure" {
				// Return a path that doesn't exist so metafile.Write fails
				s.resultInfo = metafile.MetafileInfo{RelPathKey: "non_existent_dir"}
			}

			// If we want to test compression triggering, the mock archiver needs to return a path
			if tc.name == "Incremental Happy Path with Archive and Compression" {
				a.resultPath = "archive/backup_123"
			}

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

			err := runner.ExecuteBackup(context.Background(), baseDir, srcDir, plan)

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

func TestExecuteList(t *testing.T) {
	tests := []struct {
		name           string
		setupFS        func(t *testing.T, baseDir string)
		expectError    bool
		expectedLogMsg []string // Substrings to check in the log output
	}{
		{
			name: "List Happy Path",
			setupFS: func(t *testing.T, baseDir string) {
				// 1. Incremental Current
				incCurrent := filepath.Join(baseDir, "PGL_Backup_Incremental_Current")
				os.MkdirAll(incCurrent, 0755)
				metafile.Write(incCurrent, &metafile.MetafileContent{
					TimestampUTC: time.Now(),
					Mode:         "incremental",
					UUID:         "uuid-1",
				})

				// 2. Snapshot Archive (Compressed)
				snapArchive := filepath.Join(baseDir, "PGL_Backup_Snapshot_Archive")
				os.MkdirAll(snapArchive, 0755)
				b2 := filepath.Join(snapArchive, "PGL_Backup_2023-01-02")
				os.MkdirAll(b2, 0755)
				metafile.Write(b2, &metafile.MetafileContent{
					TimestampUTC:      time.Now().Add(-48 * time.Hour),
					Mode:              "snapshot",
					UUID:              "uuid-3",
					IsCompressed:      true,
					CompressionFormat: "zip",
				})
			},
			expectError: false,
			expectedLogMsg: []string{
				"Backup (",              // Check for the timestamp format start
				"compressionFormat=zip", // Check for compression format
			},
		},
		{
			name:        "List Empty Base",
			setupFS:     func(t *testing.T, baseDir string) {},
			expectError: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			baseDir := t.TempDir()
			if tc.setupFS != nil {
				tc.setupFS(t, baseDir)
			}

			// Capture logs
			var logBuf bytes.Buffer
			plog.SetOutput(&logBuf)
			defer plog.SetOutput(os.Stderr)

			plan := &planner.ListPlan{
				PathsIncremental: planner.PathKeys{
					RelCurrentPathKey: "PGL_Backup_Incremental_Current",
					RelArchivePathKey: "PGL_Backup_Incremental_Archive",
					BackupNamePrefix:  "PGL_Backup_",
				},
				PathsSnapshot: planner.PathKeys{
					RelCurrentPathKey: "PGL_Backup_Snapshot_Current",
					RelArchivePathKey: "PGL_Backup_Snapshot_Archive",
					BackupNamePrefix:  "PGL_Backup_",
				},
				Preflight: &preflight.Plan{},
			}

			runner := engine.NewRunner(&mockValidator{}, &mockSyncer{}, &mockArchiver{}, &mockRetainer{}, &mockCompressor{})
			err := runner.ExecuteList(context.Background(), baseDir, plan)

			if (err != nil) != tc.expectError {
				t.Errorf("ExecuteList() error = %v, expectError %v", err, tc.expectError)
			}

			output := logBuf.String()
			for _, msg := range tc.expectedLogMsg {
				if !strings.Contains(output, msg) {
					t.Errorf("Expected log output to contain %q, but got:\n%s", msg, output)
				}
			}
		})
	}
}

func TestExecuteRestore(t *testing.T) {
	const (
		relCurrent = "current_dir"
		relArchive = "archive_dir"
		relContent = "content_dir"
	)

	tests := []struct {
		name          string
		backupName    string
		isCompressed  bool
		setupFS       func(t *testing.T, baseDir string)
		expectedPath  string // The relative path key expected to be passed to Syncer/Compressor
		expectExtract bool   // True if we expect Compressor.Extract, False for Syncer.Restore
		expectError   bool
	}{
		{
			name:         "Restore Current (Uncompressed)",
			backupName:   "current",
			isCompressed: false,
			setupFS: func(t *testing.T, baseDir string) {
				path := filepath.Join(baseDir, relCurrent)
				os.MkdirAll(path, 0755)
				metafile.Write(path, &metafile.MetafileContent{IsCompressed: false})
			},
			expectedPath:  relCurrent,
			expectExtract: false,
		},
		{
			name:         "Restore Archive (Compressed)",
			backupName:   "backup_123",
			isCompressed: true,
			setupFS: func(t *testing.T, baseDir string) {
				path := filepath.Join(baseDir, relArchive, "backup_123")
				os.MkdirAll(path, 0755)
				metafile.Write(path, &metafile.MetafileContent{IsCompressed: true})
			},
			expectedPath:  filepath.Join(relArchive, "backup_123"),
			expectExtract: true,
		},
		{
			name:         "Restore Archive (Uncompressed)",
			backupName:   "backup_456",
			isCompressed: false,
			setupFS: func(t *testing.T, baseDir string) {
				path := filepath.Join(baseDir, relArchive, "backup_456")
				os.MkdirAll(path, 0755)
				metafile.Write(path, &metafile.MetafileContent{IsCompressed: false})
			},
			expectedPath:  filepath.Join(relArchive, "backup_456"),
			expectExtract: false,
		},
		{
			name:        "Backup Not Found",
			backupName:  "missing",
			setupFS:     func(t *testing.T, baseDir string) {},
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			baseDir := t.TempDir()
			targetDir := t.TempDir()

			if tc.setupFS != nil {
				tc.setupFS(t, baseDir)
			}

			plan := &planner.RestorePlan{
				Paths: planner.PathKeys{
					RelCurrentPathKey: relCurrent,
					RelArchivePathKey: relArchive,
					RelContentPathKey: relContent,
				},
				Preflight:  &preflight.Plan{},
				Sync:       &pathsync.Plan{},
				Extraction: &pathcompression.ExtractPlan{},
			}

			v := &mockValidator{}
			s := &mockSyncer{}
			a := &mockArchiver{}
			r := &mockRetainer{}
			c := &mockCompressor{}

			runner := engine.NewRunner(v, s, a, r, c)

			err := runner.ExecuteRestore(context.Background(), baseDir, tc.backupName, targetDir, plan)

			if tc.expectError {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tc.expectExtract {
				if c.extractRelPathKey != util.NormalizePath(tc.expectedPath) {
					t.Errorf("Expected extract path %q, got %q", util.NormalizePath(tc.expectedPath), c.extractRelPathKey)
				}
			} else {
				if s.restoreRelPathKey != util.NormalizePath(tc.expectedPath) {
					t.Errorf("Expected restore path %q, got %q", util.NormalizePath(tc.expectedPath), s.restoreRelPathKey)
				}
			}
		})
	}
}

func TestExecuteBackup_RetentionExcludesCurrent(t *testing.T) {
	// Setup
	baseDir := t.TempDir()
	relArchive := "archive"
	relCurrent := "current"
	prefix := "backup_"

	// Create the archive directory
	archivePath := filepath.Join(baseDir, relArchive)
	os.MkdirAll(archivePath, 0755)

	// Create 'current' backup so fetchBackup succeeds and Archive is called
	currentPath := filepath.Join(baseDir, relCurrent)
	os.MkdirAll(currentPath, 0755)
	metafile.Write(currentPath, &metafile.MetafileContent{TimestampUTC: time.Now()})

	// 1. Create an OLD backup on disk (should be pruned)
	oldBackupName := prefix + "old"
	oldBackupPath := filepath.Join(archivePath, oldBackupName)
	os.MkdirAll(oldBackupPath, 0755)
	metafile.Write(oldBackupPath, &metafile.MetafileContent{TimestampUTC: time.Now().Add(-24 * time.Hour)})

	// 2. Create the NEW backup on disk (simulating what Archiver just did)
	newBackupRelPath := filepath.Join(relArchive, prefix+"new")
	newBackupPath := filepath.Join(baseDir, newBackupRelPath)
	os.MkdirAll(newBackupPath, 0755)
	metafile.Write(newBackupPath, &metafile.MetafileContent{TimestampUTC: time.Now()})

	// Plan
	plan := &planner.BackupPlan{
		Mode: planner.Incremental,
		Paths: planner.PathKeys{
			RelArchivePathKey: relArchive,
			RelCurrentPathKey: relCurrent,
			BackupNamePrefix:  prefix,
		},
		Preflight:   &preflight.Plan{},
		Sync:        &pathsync.Plan{Enabled: true},
		Archive:     &patharchive.Plan{Enabled: true},
		Retention:   &pathretention.Plan{Enabled: true}, // Enabled!
		Compression: &pathcompression.CompressPlan{Enabled: false},
	}

	// Mocks
	v := &mockValidator{}
	s := &mockSyncer{
		resultInfo: metafile.MetafileInfo{RelPathKey: relCurrent},
	}

	// Mock Archiver that returns the path of the NEW backup we created
	a := &mockArchiver{
		resultPath: newBackupRelPath,
	}

	r := &mockRetainer{}
	c := &mockCompressor{}

	runner := engine.NewRunner(v, s, a, r, c)

	// Execute
	err := runner.ExecuteBackup(context.Background(), baseDir, "src", plan)
	if err != nil {
		t.Fatalf("ExecuteBackup failed: %v", err)
	}

	// Assert
	// We expect toPrune to contain ONLY the old backup.
	if len(r.toPrune) != 1 {
		t.Errorf("Expected 1 backup to prune, got %d", len(r.toPrune))
	} else {
		expectedKey := util.NormalizePath(filepath.Join(relArchive, oldBackupName))
		if r.toPrune[0].RelPathKey != expectedKey {
			t.Errorf("Expected to prune %s, got %s", expectedKey, r.toPrune[0].RelPathKey)
		}
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

		// Filesystem setup
		setupFS func(t *testing.T, baseDir string)

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
		{
			name:                "Retention Nothing To Prune (Ignored)",
			retentionIncEnabled: true,
			retentionErr:        pathretention.ErrNothingToPrune,
			expectError:         false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			baseDir := t.TempDir()

			if tc.setupFS != nil {
				tc.setupFS(t, baseDir)
			} else {
				// Ensure archive directories exist for fetchBackups
				os.MkdirAll(filepath.Join(baseDir, relArchiveInc), 0755)
				os.MkdirAll(filepath.Join(baseDir, relArchiveSnap), 0755)
			}

			plan := &planner.PrunePlan{
				Preflight: &preflight.Plan{},
				PathsIncremental: planner.PathKeys{
					RelArchivePathKey: relArchiveInc,
					BackupNamePrefix:  prefix,
				},
				PathsSnapshot: planner.PathKeys{
					RelArchivePathKey: relArchiveSnap,
					BackupNamePrefix:  prefix,
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

			err := runner.ExecutePrune(context.Background(), baseDir, plan)

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
