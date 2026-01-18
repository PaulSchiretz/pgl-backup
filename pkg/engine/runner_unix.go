//go:build !windows

package engine

import (
	"context"
	"os/exec"

	"golang.org/x/sys/unix"
)

// createHookCommand creates an exec.Cmd for a hook on Unix-like systems.
func (r *Runner) createHookCommand(ctx context.Context, command string) *exec.Cmd {
	cmd := r.hookCommandExecutor(ctx, "/bin/sh", "-c", command)
	// On Unix-like systems, create a new process group (PGRP) and make the command
	// the session leader. This allows sending signals to the entire process group
	// when the context is canceled, ensuring all child processes are terminated.
	cmd.SysProcAttr = &unix.SysProcAttr{Setpgid: true}
	return cmd
}
