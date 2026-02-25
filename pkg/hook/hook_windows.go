//go:build windows

package hook

import (
	"context"
	"os/exec"

	"golang.org/x/sys/windows"
)

// createCommand creates an exec.Cmd for a hook on Windows.
func (e *HookExecutor) createCommand(ctx context.Context, command string) *exec.Cmd {
	cmd := e.commandContext(ctx, "cmd", "/C", command)
	// On Windows, create a new process group to ensure that when the context is
	// canceled, the entire process tree is terminated, not just the parent cmd.
	// This is crucial for killing child processes spawned by the hook command.
	cmd.SysProcAttr = &windows.SysProcAttr{CreationFlags: windows.CREATE_NEW_PROCESS_GROUP}
	return cmd
}
