//go:build windows

package pathsync

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"strconv"

	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

// isRobocopySuccessHelper checks if a robocopy error is actually a success code.
// Robocopy returns exit codes < 8 for successful operations that involved copying/deleting files.
func isRobocopySuccessHelper(err error) bool {
	if exitErr, ok := err.(*exec.ExitError); ok {
		// Exit codes 0-7 are considered success by robocopy.
		if exitErr.ExitCode() < 8 {
			return true
		}
	}
	return false
}

// handleSyncRobocopy uses the Windows `robocopy` utility to perform a highly
// efficient and robust directory mirror. It is much faster for incremental
// backups than a manual walk. It returns a list of copied files.
func (s *PathSyncer) handleRobocopy(ctx context.Context, src, dst string, mirror bool, excludeFiles, excludeDirs []string, enableMetrics bool) error {
	// Robocopy command arguments:
	// /MIR :: MIRror a directory tree (equivalent to /E plus /PURGE).
	// /E :: copy subdirectories, including Empty ones.
	// /V :: Verbose output, showing skipped files.
	// /TEE :: output to console window as well as the log file.
	// /R:n :: Retry n times on failed copies.
	// /W:n :: Wait n seconds between retries.
	// /NP :: No Progress - don't display % copied.
	// /NJH :: No Job Header.
	// /NJS :: No Job Summary.
	args := []string{src, dst, "/V", "/TEE", "/NP", "/NJH"}
	args = append(args, "/R:"+strconv.Itoa(s.engine.RetryCount))
	args = append(args, "/W:"+strconv.Itoa(s.engine.RetryWaitSeconds))

	if mirror {
		args = append(args, "/MIR")
	} else {
		args = append(args, "/E")
	}

	// If metrics are disabled, suppress Robocopy's job summary.
	if !enableMetrics {
		args = append(args, "/NJS")
	}

	// If the log level is WARN or higher, suppress Robocopy's file/dir list.
	if !plog.Default().Enabled(context.Background(), slog.Level(plog.LevelInfo)) {
		args = append(args, "/NFL") // No File List - don't log individual files.
		args = append(args, "/NDL") // No Directory List - don't log individual directories.
	}

	if s.dryRun {
		args = append(args, "/L") // /L :: List only - don't copy, delete, or timestamp files.
	}

	// Add files to exclude.
	if len(excludeFiles) > 0 {
		args = append(args, "/XF")
		args = append(args, excludeFiles...)
	}
	// Add directories to exclude.
	if len(excludeDirs) > 0 {
		args = append(args, "/XD")
		args = append(args, excludeDirs...)
	}

	plog.Info("Starting sync with robocopy")
	cmd := exec.CommandContext(ctx, "robocopy", args...)

	// Pipe robocopy's stdout and stderr directly to our program's stdout/stderr
	// This provides real-time logging.
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Run()
	// Robocopy returns non-zero exit codes for success cases (e.g., files were copied).
	// A nil error means exit code 0, which is a clean success.
	if err == nil {
		return nil
	}
	// If the error is not nil, we check if it's one of robocopy's "success" exit codes.
	if !isRobocopySuccessHelper(err) {
		return err // It's a real error
	}
	return nil // It was a success code or no error
}
