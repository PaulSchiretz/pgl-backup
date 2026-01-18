//go:build windows

package engine

import (
	"context"
	"os/exec"

	"golang.org/x/sys/windows"
)

// createHookCommand creates an exec.Cmd for a hook on Windows.
func (r *Runner) createHookCommand(ctx context.Context, command string) *exec.Cmd {
	cmd := r.hookCommandExecutor(ctx, "cmd", "/C", command)
	// On Windows, create a new process group to ensure that when the context is
	// canceled, the entire process tree is terminated, not just the parent cmd.
	// This is crucial for killing child processes spawned by the hook command.
	cmd.SysProcAttr = &windows.SysProcAttr{CreationFlags: windows.CREATE_NEW_PROCESS_GROUP}
	return cmd
}
