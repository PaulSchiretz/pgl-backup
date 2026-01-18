package preflight_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/preflight"
)

func TestCheckBackupTargetAccessible(t *testing.T) {
	tests := []struct {
		name          string
		setupTarget   func(t *testing.T) string
		expectError   bool
		errorContains string
	}{
		{
			name: "Happy Path - Target Exists",
			setupTarget: func(t *testing.T) string {
				return t.TempDir()
			},
			expectError: false,
		},
		{
			name: "Happy Path - Target Does Not Exist, Parent Exists",
			setupTarget: func(t *testing.T) string {
				parentDir := t.TempDir()
				return filepath.Join(parentDir, "new_dir")
			},
			expectError: false,
		},
		{
			name: "Error - Target Is a File",
			setupTarget: func(t *testing.T) string {
				targetFile := filepath.Join(t.TempDir(), "target.txt")
				if err := os.WriteFile(targetFile, []byte("i am a file"), 0644); err != nil {
					t.Fatalf("failed to create test file: %v", err)
				}
				return targetFile
			},
			expectError:   true,
			errorContains: "is not a directory",
		},
		{
			name: "Error - Target Path is Current Directory",
			setupTarget: func(t *testing.T) string {
				return "."
			},
			expectError: true,
		},
		{
			name: "Error - Target Path is Root Directory",
			setupTarget: func(t *testing.T) string {
				return string(filepath.Separator)
			},
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			target := tc.setupTarget(t)
			v := preflight.NewValidator()
			err := v.Run(context.Background(), "", target, &preflight.Plan{TargetAccessible: true}, time.Now())

			if tc.expectError {
				if err == nil {
					t.Fatal("expected an error, but got nil")
				}
				if tc.errorContains != "" && !strings.Contains(err.Error(), tc.errorContains) {
					t.Errorf("expected error to contain %q, but got: %v", tc.errorContains, err)
				}
			} else {
				if err != nil {
					t.Errorf("expected no error, but got: %v", err)
				}
			}
		})
	}
}

func TestCheckBackupSourceAccessible(t *testing.T) {
	tests := []struct {
		name          string
		setupSource   func(t *testing.T) string
		expectError   bool
		errorContains string
	}{
		{
			name: "Happy Path - Source is a directory",
			setupSource: func(t *testing.T) string {
				return t.TempDir()
			},
			expectError: false,
		},
		{
			name: "Error - Source does not exist",
			setupSource: func(t *testing.T) string {
				return filepath.Join(t.TempDir(), "nonexistent")
			},
			expectError:   true,
			errorContains: "does not exist",
		},
		{
			name: "Error - Source is a file",
			setupSource: func(t *testing.T) string {
				srcFile := filepath.Join(t.TempDir(), "source.txt")
				if err := os.WriteFile(srcFile, []byte("i am a file"), 0644); err != nil {
					t.Fatalf("failed to create test file: %v", err)
				}
				return srcFile
			},
			expectError:   true,
			errorContains: "is not a directory",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			src := tc.setupSource(t)
			v := preflight.NewValidator()
			err := v.Run(context.Background(), src, "", &preflight.Plan{SourceAccessible: true}, time.Now())

			if tc.expectError {
				if err == nil {
					t.Fatal("expected an error, but got nil")
				}
				if tc.errorContains != "" && !strings.Contains(err.Error(), tc.errorContains) {
					t.Errorf("expected error to contain %q, but got: %v", tc.errorContains, err)
				}
			} else {
				if err != nil {
					t.Errorf("expected no error, but got: %v", err)
				}
			}
		})
	}
}

func TestValidator_Run(t *testing.T) {
	tests := []struct {
		name          string
		setup         func(t *testing.T) (src, target string)
		dryRun        bool
		plan          *preflight.Plan
		expectError   bool
		errorContains string
		validate      func(t *testing.T, target string)
	}{
		{
			name: "Happy Path - Normal Run",
			setup: func(t *testing.T) (string, string) {
				return t.TempDir(), filepath.Join(t.TempDir(), "target")
			},
			dryRun: false,
			plan: &preflight.Plan{
				SourceAccessible:   true,
				TargetAccessible:   true,
				TargetWriteable:    true,
				CaseMismatch:       true,
				PathNesting:        true,
				EnsureTargetExists: true,
			},
			expectError: false,
			validate: func(t *testing.T, target string) {
				if _, err := os.Stat(target); os.IsNotExist(err) {
					t.Error("expected target directory to be created, but it was not")
				}
			},
		},
		{
			name: "Happy Path - Dry Run",
			setup: func(t *testing.T) (string, string) {
				return t.TempDir(), filepath.Join(t.TempDir(), "target")
			},
			dryRun: true,
			plan: &preflight.Plan{
				SourceAccessible:   true,
				TargetAccessible:   true,
				TargetWriteable:    true,
				CaseMismatch:       true,
				PathNesting:        true,
				EnsureTargetExists: true,
			},
			expectError: false,
			validate: func(t *testing.T, target string) {
				if _, err := os.Stat(target); !os.IsNotExist(err) {
					t.Error("expected target directory NOT to be created on a dry run, but it was")
				}
			},
		},
		{
			name: "Happy Path - Skip Source Check",
			setup: func(t *testing.T) (string, string) {
				return "/non/existent/path", filepath.Join(t.TempDir(), "target")
			},
			dryRun: false,
			plan: &preflight.Plan{
				SourceAccessible:   false,
				TargetAccessible:   true,
				TargetWriteable:    true,
				CaseMismatch:       false,
				PathNesting:        false,
				EnsureTargetExists: true,
			},
			expectError: false,
		},
		{
			name: "Failure - Inaccessible Target",
			setup: func(t *testing.T) (string, string) {
				targetFile := filepath.Join(t.TempDir(), "file.txt")
				if err := os.WriteFile(targetFile, []byte("not a dir"), 0644); err != nil {
					t.Fatalf("failed to create test file: %v", err)
				}
				return t.TempDir(), targetFile
			},
			dryRun: false,
			plan: &preflight.Plan{
				SourceAccessible:   true,
				TargetAccessible:   true,
				TargetWriteable:    true,
				CaseMismatch:       true,
				PathNesting:        true,
				EnsureTargetExists: true,
			},
			expectError:   true,
			errorContains: "target path accessibility check failed",
		},
		{
			name: "Failure - Inaccessible Source",
			setup: func(t *testing.T) (string, string) {
				sourceFile := filepath.Join(t.TempDir(), "file.txt")
				if err := os.WriteFile(sourceFile, []byte("not a dir"), 0644); err != nil {
					t.Fatalf("failed to create test file: %v", err)
				}
				return sourceFile, filepath.Join(t.TempDir(), "target")
			},
			dryRun: false,
			plan: &preflight.Plan{
				SourceAccessible:   true,
				TargetAccessible:   true,
				TargetWriteable:    true,
				CaseMismatch:       true,
				PathNesting:        true,
				EnsureTargetExists: true,
			},
			expectError:   true,
			errorContains: "source path validation failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			src, target := tc.setup(t)
			v := preflight.NewValidator()
			tc.plan.DryRun = tc.dryRun
			err := v.Run(context.Background(), src, target, tc.plan, time.Now())

			if tc.expectError {
				if err == nil {
					t.Fatal("expected an error, but got nil")
				}
				if tc.errorContains != "" && !strings.Contains(err.Error(), tc.errorContains) {
					t.Errorf("expected error to contain %q, but got: %v", tc.errorContains, err)
				}
			} else {
				if err != nil {
					t.Errorf("expected no error, but got: %v", err)
				}
			}

			if tc.validate != nil {
				tc.validate(t, target)
			}
		})
	}
}

func TestCheckBackupTargetWritable(t *testing.T) {
	tests := []struct {
		name          string
		setupTarget   func(t *testing.T) string
		expectError   bool
		errorContains string
	}{
		{
			name: "Happy Path - Directory is writable",
			setupTarget: func(t *testing.T) string {
				return t.TempDir()
			},
			expectError: false,
		},
		{
			name: "Error - Target is a file",
			setupTarget: func(t *testing.T) string {
				targetFile := filepath.Join(t.TempDir(), "target.txt")
				os.WriteFile(targetFile, []byte("i am a file"), 0644)
				return targetFile
			},
			expectError:   true,
			errorContains: "target path exists but is not a directory",
		},
		{
			name: "Error - Target does not exist",
			setupTarget: func(t *testing.T) string {
				return filepath.Join(t.TempDir(), "nonexistent")
			},
			expectError:   true,
			errorContains: "target directory does not exist",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			target := tc.setupTarget(t)
			v := preflight.NewValidator()
			err := v.Run(context.Background(), "", target, &preflight.Plan{TargetWriteable: true}, time.Now())

			if tc.expectError {
				if err == nil {
					t.Fatal("expected an error, but got nil")
				}
				if tc.errorContains != "" && !strings.Contains(err.Error(), tc.errorContains) {
					t.Errorf("expected error to contain %q, but got: %v", tc.errorContains, err)
				}
			} else {
				if err != nil {
					t.Errorf("expected no error, but got: %v", err)
				}
			}
		})
	}
}

func TestCheckPathNesting(t *testing.T) {
	tests := []struct {
		name          string
		setup         func(t *testing.T) (src, target string)
		expectError   bool
		errorContains string
	}{
		{
			name: "Happy Path - Separate Directories",
			setup: func(t *testing.T) (string, string) {
				return t.TempDir(), t.TempDir()
			},
			expectError: false,
		},
		{
			name: "Happy Path - Siblings",
			setup: func(t *testing.T) (string, string) {
				root := t.TempDir()
				return filepath.Join(root, "src"), filepath.Join(root, "target")
			},
			expectError: false,
		},
		{
			name: "Happy Path - Empty Strings",
			setup: func(t *testing.T) (string, string) {
				return "", ""
			},
			expectError: false,
		},
		{
			name: "Error - Target Inside Source",
			setup: func(t *testing.T) (string, string) {
				src := t.TempDir()
				target := filepath.Join(src, "nested_target")
				return src, target
			},
			expectError:   true,
			errorContains: "inside or same as source path",
		},
		{
			name: "Error - Source Inside Target",
			setup: func(t *testing.T) (string, string) {
				target := t.TempDir()
				src := filepath.Join(target, "nested_source")
				return src, target
			},
			expectError:   true,
			errorContains: "inside or same as target path",
		},
		{
			name: "Error - Same Directory",
			setup: func(t *testing.T) (string, string) {
				dir := t.TempDir()
				return dir, dir
			},
			expectError: true,
		},
		{
			name: "Error - Relative Paths Nested",
			setup: func(t *testing.T) (string, string) {
				return "data", filepath.Join("data", "backup")
			},
			expectError:   true,
			errorContains: "inside or same as source path",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			src, target := tc.setup(t)
			v := preflight.NewValidator()
			err := v.Run(context.Background(), src, target, &preflight.Plan{PathNesting: true}, time.Now())

			if tc.expectError {
				if err == nil {
					t.Fatal("expected an error, but got nil")
				}
				if tc.errorContains != "" && !strings.Contains(err.Error(), tc.errorContains) {
					t.Errorf("expected error to contain %q, but got: %v", tc.errorContains, err)
				}
			} else {
				if err != nil {
					t.Errorf("expected no error, but got: %v", err)
				}
			}
		})
	}
}
