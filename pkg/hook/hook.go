package hook

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/hints"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

var ErrNothingToExecute = hints.New("nothing to execute")
var ErrDisabled = hints.New("hook execution is disabled")

type HookExecutor struct {
	// commandContext allows mocking os/exec for testing hooks.
	commandContext func(ctx context.Context, name string, arg ...string) *exec.Cmd
}

// NewChecker creates a new HookExecutor with the given configuration.
func NewHookExecutor(commandContext func(ctx context.Context, name string, arg ...string) *exec.Cmd) *HookExecutor {
	return &HookExecutor{
		commandContext: commandContext,
	}
}

func (e *HookExecutor) RunPreHook(ctx context.Context, hookName string, p *Plan, timestampUTC time.Time) error {
	if !p.Enabled {
		return ErrDisabled
	}

	if len(p.PreHookCommands) <= 0 {
		return ErrNothingToExecute
	}

	plog.Info(fmt.Sprintf("Running Pre-%s hook commands", hookName))

	for _, hookCommand := range p.PreHookCommands {

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if p.DryRun {
			plog.Info("[DRY RUN] Executing command", "command", hookCommand)
			continue
		}
		plog.Info("Executing command", "command", hookCommand)

		cmd := e.createCommand(ctx, hookCommand)

		// Pipe output to our logger for visibility
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Run(); err != nil {
			// Check if the context was canceled, which can cause cmd.Wait() to return an error.
			// If so, we should return the context's error to be more specific.
			if ctx.Err() == context.Canceled {
				return context.Canceled
			}
			if p.FailFast {
				return fmt.Errorf("command '%s' failed: %w", hookCommand, err)
			}
			plog.Warn("Hook command failed", "command", hookCommand, "error", err)
		}
	}
	return nil
}

func (e *HookExecutor) RunPostHook(ctx context.Context, hookName string, p *Plan, timestampUTC time.Time) error {
	if !p.Enabled {
		return ErrDisabled
	}

	if len(p.PostHookCommands) <= 0 {
		return ErrNothingToExecute
	}

	plog.Info(fmt.Sprintf("Running Post-%s hook commands", hookName))

	for _, hookCommand := range p.PostHookCommands {

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if p.DryRun {
			plog.Info("[DRY RUN] Executing command", "command", hookCommand)
			continue
		}
		plog.Info("Executing command", "command", hookCommand)
		cmd := e.createCommand(ctx, hookCommand)

		// Pipe output to our logger for visibility
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Run(); err != nil {
			// Check if the context was canceled, which can cause cmd.Wait() to return an error.
			// If so, we should return the context's error to be more specific.
			if ctx.Err() == context.Canceled {
				return context.Canceled
			}
			if p.FailFast {
				return fmt.Errorf("command '%s' failed: %w", hookCommand, err)
			}
			plog.Warn("Hook command failed", "command", hookCommand, "error", err)
		}
	}
	return nil
}
