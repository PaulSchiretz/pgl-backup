// Package preflight provides functions for validation and checks that run before
// a main operation begins. These checks are designed to be stateless and
// idempotent, ensuring the system is in a suitable state for an operation to
// proceed without changing the system's state itself.
package preflight

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/util"

	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

type Validator struct {
}

// NewChecker creates a new PathArchiver with the given configuration.
func NewValidator() *Validator {
	return &Validator{}
}

// Run performs all necessary validations and setup before an operation runs.
// It's an orchestrator function that calls other checks in a specific order.
func (v *Validator) Run(ctx context.Context, absSourcePath, absTargetPath string, p *Plan, timestampUTC time.Time) error {

	// 1. Check target accessibility.
	if p.TargetAccessible {
		if err := checkTargetAccessible(absTargetPath); err != nil {
			return fmt.Errorf("target path accessibility check failed: %w", err)
		}
	}
	// 2. Check source accessibility.
	if p.SourceAccessible {
		if err := checkSourceAccessible(absSourcePath); err != nil {
			return fmt.Errorf("source path validation failed: %w", err)
		}
	}

	// 3. If not a dry run, perform state-changing checks (create dir, check writability).
	if !p.DryRun {
		if p.EnsureTargetExists {
			if err := os.MkdirAll(absTargetPath, util.UserWritableDirPerms); err != nil {
				return fmt.Errorf("failed to create target directory: %w", err)
			}
		}
		if p.TargetWriteable {
			if err := checkTargetWritable(absTargetPath); err != nil {
				return fmt.Errorf("target path writable check failed: %w", err)
			}
		}
	}

	// 4. Perform cross-platform safety checks.
	if p.CaseMismatch {
		if err := checkCaseSensitivityMismatch(absSourcePath); err != nil {
			return err // This returns a warning as an error to halt execution.
		}
	}

	// 5. Check for path nesting.
	if p.PathNesting {
		if err := checkPathNesting(absSourcePath, absTargetPath); err != nil {
			return err
		}
	}

	return nil
}

// checkTargetAccessible performs pre-run checks to ensure the target is usable.
// It provides more user-friendly errors than letting os.MkdirAll fail.
//
// The checks include:
//  1. On Windows, verifies that the drive or network share (e.g., "Z:", "\\Server\Share") exists.
//  2. If the target path exists, confirms it is a directory.
//  3. If the target path does not exist, it confirms its immediate parent directory is accessible.
//  4. On Unix, it verifies that the target path is not on the system disk when it's expected to be
//     on a separate mounted drive. This prevents writing to a "ghost" directory if a drive is not
//     mounted. This check is performed on the target path if it exists, or its deepest existing
//     ancestor if it does not.
func checkTargetAccessible(targetPath string) error {
	// It's unsafe to operate on the current directory or the root of a filesystem.
	if isUnsafeRoot(targetPath) {
		return fmt.Errorf("target path cannot be the current directory ('.') or the root directory ('/')")
	}

	info, err := os.Stat(targetPath)
	if os.IsNotExist(err) {
		// Target doesn't exist. We must check its ancestors.

		// Find the Deepest Existing Ancestor
		ancestor := targetPath
		for {
			parent := filepath.Dir(ancestor)
			if parent == ancestor {
				break // Hit root
			}
			if _, err := os.Stat(parent); err == nil {
				ancestor = parent
				break // Found the deepest directory that actually exists
			}
			ancestor = parent
		}

		// Platform-specific: Validate the ancestor (e.g., Unix mount point check)
		if err := platformValidateMountPoint(ancestor); err != nil {
			return err
		}

		// The target path doesn't exist. Check accessibility of the deepest existing ancestor
		// to ensure os.MkdirAll can create the required subdirectories. This provides a
		// more specific error message on permission failure letting any subsequent os.MkdirAll fail alone.
		if _, err := os.Stat(ancestor); err != nil {
			return fmt.Errorf("cannot access ancestor directory %s: %w", ancestor, err)
		}

		return nil
	} else if err != nil {
		return fmt.Errorf("cannot access target path: %w", err)
	}

	// --- 2. The Target Path Exists ---
	if !info.IsDir() {
		return fmt.Errorf("target path exists but is not a directory: %s", targetPath)
	}

	// Platform-specific: If the folder exists, we check it directly (e.g., Unix mount point check).
	if err := platformValidateMountPoint(targetPath); err != nil {
		return err
	}

	return nil
}

// checkPathNesting ensures that source and target paths are not nested within each other.
// This prevents infinite recursion loops (Target inside Source) and unsupported configurations
// (Source inside Target).
func checkPathNesting(source, target string) error {
	if source == "" || target == "" {
		return nil
	}

	absSource, err := filepath.Abs(source)
	if err != nil {
		return fmt.Errorf("failed to resolve absolute path for source: %w", err)
	}

	absTarget, err := filepath.Abs(target)
	if err != nil {
		return fmt.Errorf("failed to resolve absolute path for target: %w", err)
	}

	// Check if Target is inside Source (Infinite Recursion Risk)
	if rel, err := filepath.Rel(absSource, absTarget); err == nil && !strings.HasPrefix(rel, "..") {
		return fmt.Errorf("target path '%s' is inside or same as source path '%s'. This causes infinite recursion", target, source)
	}

	// Check if Source is inside Target (Unsupported)
	if rel, err := filepath.Rel(absTarget, absSource); err == nil && !strings.HasPrefix(rel, "..") {
		return fmt.Errorf("source path '%s' is inside or same as target path '%s'. This is not supported", source, target)
	}

	// Check for physical identity (e.g. bind mounts, symlinks resolving to same dir).
	// This catches cases where string comparison fails (e.g. different paths pointing to the same inode).
	srcInfo, errSrc := os.Stat(absSource)
	trgInfo, errTrg := os.Stat(absTarget)
	if errSrc == nil && errTrg == nil {
		if os.SameFile(srcInfo, trgInfo) {
			return fmt.Errorf("source and target paths resolve to the same physical directory")
		}
	}

	return nil
}

// checkSourceAccessible validates that the source path exists and is a directory.
func checkSourceAccessible(srcPath string) error {
	srcInfo, err := os.Stat(srcPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("source directory %s does not exist", srcPath)
		}
		return fmt.Errorf("cannot stat source directory %s: %w", srcPath, err)
	}

	if !srcInfo.IsDir() {
		return fmt.Errorf("source path %s is not a directory", srcPath)
	}

	// Verify that the source directory is readable.
	f, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("source directory is not readable: %w", err)
	}
	defer f.Close()

	return nil
}

// checkTargetWritable ensures the target directory can be created and is writable
// by performing filesystem modifications.
func checkTargetWritable(targetPath string) error {
	// This function assumes the directory has been created by the caller.
	// It first verifies the path exists and is a directory.
	info, err := os.Stat(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("target directory does not exist: %s", targetPath)
		}
		return fmt.Errorf("cannot stat target directory %s: %w", targetPath, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("target path exists but is not a directory: %s", targetPath)
	}

	// Perform a thorough write check by creating and deleting a temporary file.
	tempFile := filepath.Join(targetPath, ".pgl-backup-writetest.tmp")
	if f, err := os.Create(tempFile); err != nil {
		return fmt.Errorf("target directory %s is not writable: %w", targetPath, err)
	} else {
		f.Close()
	}

	if err := os.Remove(tempFile); err != nil {
		// This is not a critical failure, but worth logging.
		plog.Warn("Failed to remove temporary write-test file", "path", tempFile, "error", err)
	}
	return nil
}

// checkCaseSensitivityMismatch warns the user if they are running on a case-insensitive OS (like Windows)
// but the source path appears to be from a case-sensitive one (like Linux, e.g., /home/user).
// This is a high-risk scenario that can lead to silent data loss due to file collisions.
func checkCaseSensitivityMismatch(sourcePath string) error {
	// Determine the case-sensitivity of the filesystem where the backup tool is running.
	// We check the temp dir as a proxy for the host's default filesystem.
	hostIsCaseSensitive, err := util.IsPathCaseSensitive(os.TempDir())
	if err != nil {
		// If we can't determine host sensitivity, log a warning but don't block the backup.
		plog.Warn("Could not determine host filesystem case-sensitivity; skipping mismatch check.", "error", err)
		return nil
	}

	// Now, determine the case-sensitivity of the source path's filesystem.
	sourceIsCaseSensitive, err := util.IsPathCaseSensitive(sourcePath)
	if err != nil {
		plog.Warn("Could not determine source filesystem case-sensitivity; skipping mismatch check.", "path", sourcePath, "error", err)
		return nil
	}

	// The dangerous scenario: a case-insensitive host trying to back up a case-sensitive source.
	if !hostIsCaseSensitive && sourceIsCaseSensitive {
		plog.Warn("CRITICAL RISK: Case-sensitivity mismatch detected.")
		return fmt.Errorf("the host filesystem is case-insensitive, but the source path '%s' is on a case-sensitive filesystem. This can cause file collisions and silent data loss (e.g., 'File.txt' overwriting 'file.txt'). It is strongly recommended to run this backup from a case-sensitive host (e.g., Linux, or from within WSL on Windows)", sourcePath)
	}

	return nil
}
