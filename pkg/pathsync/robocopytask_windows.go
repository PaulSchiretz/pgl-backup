//go:build windows

package pathsync

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

// isRobocopySuccessHelper checks if a robocopy error is actually a success code.
// Robocopy returns exit codes < 8 for successful operations that involved copying/deleting files.
func isRobocopySuccessHelper(err error) bool {
	if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
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
func (t *robocopyTask) execute() error {
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
	// /SL :: Copy symbolic links instead of the target (matches native engine behavior).
	// /A-:R :: Remove Read-Only attribute from copied files (matches native engine's WithUserWritePermission).
	args := []string{t.src, t.trg, "/V", "/TEE", "/NP", "/NJH", "/SL", "/A-:R"}
	args = append(args, "/R:"+strconv.Itoa(t.retryCount))
	args = append(args, "/W:"+strconv.Itoa(int(t.retryWait/time.Second)))

	if t.mirror {
		args = append(args, "/MIR")
	} else {
		args = append(args, "/E")
	}

	// If metrics are disabled, suppress Robocopy's job summary.
	if !t.metrics {
		args = append(args, "/NJS")
	}

	// If the log level is WARN or higher, suppress Robocopy's file/dir list.
	if !plog.Default().Enabled(context.Background(), slog.Level(plog.LevelInfo)) {
		args = append(args, "/NFL") // No File List - don't log individual files.
		args = append(args, "/NDL") // No Directory List - don't log individual directories.
	}

	if t.dryRun {
		args = append(args, "/L") // /L :: List only - don't copy, delete, or timestamp files.
	}

	// Map OverwriteBehavior to Robocopy flags
	switch t.overwriteBehavior {
	case OverwriteNever:
		// Exclude Changed, Newer, and Older files. Effectively only copy new files (files that don't exist in dest).
		args = append(args, "/XC", "/XN", "/XO")
	case OverwriteIfNewer:
		// Exclude Older files. Copy if source is Newer.
		args = append(args, "/XO")
	case OverwriteAlways:
		args = append(args, "/IS", "/IT") // Include Same and Tweaked files (force overwrite even if identical)
	}

	// Add files to exclude.
	if len(t.fileExcludes) > 0 {
		args = append(args, "/XF")
		args = append(args, t.fileExcludes...)
	}
	// Add directories to exclude.
	if len(t.dirExcludes) > 0 {
		args = append(args, "/XD")
		args = append(args, t.dirExcludes...)
	}

	plog.Info("Starting sync with robocopy")
	cmd := exec.CommandContext(t.ctx, "robocopy", args...)

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
