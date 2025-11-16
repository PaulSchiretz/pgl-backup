//go:build windows

package pathsync

import (
	"log"
	"os"
	"os/exec"
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
func (s *PathSyncer) handleRobocopy(src, dst string, mirror bool) error {
	// Robocopy command arguments:
	// /MIR :: MIRror a directory tree (equivalent to /E plus /PURGE).
	// /E :: copy subdirectories, including Empty ones.
	// /V :: Verbose output, showing skipped files.
	// /TEE :: output to console window as well as the log file.
	// /R:3 :: Retry 3 times on failed copies.
	// /W:5 :: Wait 5 seconds between retries.
	// /NP :: No Progress - don't display % copied.
	// /NJH :: No Job Header.
	// /NJS :: No Job Summary. (We keep the summary for a good overview)
	args := []string{src, dst, "/V", "/TEE", "/R:3", "/W:5", "/NP", "/NJH"}
	if mirror {
		args = append(args, "/MIR")
	} else {
		args = append(args, "/E")
	}

	if s.quiet {
		args = append(args, "/NFL") // No File List - don't log individual files.
		args = append(args, "/NDL") // No Directory List - don't log individual directories.
	}

	if s.dryRun {
		args = append(args, "/L") // /L :: List only - don't copy, delete, or timestamp files.
	}

	log.Println("Starting sync with robocopy...")
	cmd := exec.Command("robocopy", args...)

	// Pipe robocopy's stdout and stderr directly to our program's stdout/stderr
	// This provides real-time logging.
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Run()
	// Robocopy returns non-zero exit codes for success cases (e.g., files were copied).
	// We check if the error is a "successful" one and return nil if so.
	if err != nil && !isRobocopySuccessHelper(err) {
		return err // It's a real error
	}
	return nil // It was a success code or no error
}
