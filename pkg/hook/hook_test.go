package hook_test

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/hook"
)

// TestHelperProcess is a helper for testing exec.
func TestHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
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

func TestHookExecutor(t *testing.T) {
	mockExecutor := func(ctx context.Context, name string, arg ...string) *exec.Cmd {
		// On Windows, the command is wrapped in `cmd /C`. We need to extract the actual command.
		var cmdLine string
		if len(arg) > 1 && arg[0] == "/C" {
			cmdLine = strings.Join(arg[1:], " ")
		} else if len(arg) > 1 && arg[0] == "-c" {
			cmdLine = strings.Join(arg[1:], " ")
		} else {
			cmdLine = name + " " + strings.Join(arg, " ")
		}

		cs := []string{"-test.run=TestHelperProcess", "--", cmdLine}
		cmd := exec.CommandContext(ctx, os.Args[0], cs...)
		cmd.Env = []string{"GO_WANT_HELPER_PROCESS=1"}
		return cmd
	}

	tests := []struct {
		name          string
		plan          *hook.Plan
		hookType      string // "pre" or "post"
		expectError   bool
		errorContains string
	}{
		{
			name: "Pre-hook success",
			plan: &hook.Plan{
				Enabled:         true,
				PreHookCommands: []string{"echo pre-hook-works"},
			},
			hookType:    "pre",
			expectError: false,
		},
		{
			name: "Post-hook success",
			plan: &hook.Plan{
				Enabled:          true,
				PostHookCommands: []string{"echo post-hook-works"},
			},
			hookType:    "post",
			expectError: false,
		},
		{
			name: "Pre-hook failure with FailFast",
			plan: &hook.Plan{
				Enabled:         true,
				PreHookCommands: []string{"fail this"},
				FailFast:        true,
			},
			hookType:      "pre",
			expectError:   true,
			errorContains: "command 'fail this' failed",
		},
		{
			name: "Pre-hook failure without FailFast",
			plan: &hook.Plan{
				Enabled:         true,
				PreHookCommands: []string{"fail this"},
				FailFast:        false,
			},
			hookType:    "pre",
			expectError: false,
		},
		{
			name: "Post-hook failure without FailFast",
			plan: &hook.Plan{
				Enabled:          true,
				PostHookCommands: []string{"fail this"},
				FailFast:         false,
			},
			hookType:    "post",
			expectError: false,
		},
		{
			name: "Dry run",
			plan: &hook.Plan{
				Enabled:         true,
				PreHookCommands: []string{"echo should-not-run"},
				DryRun:          true,
			},
			hookType:    "pre",
			expectError: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			executor := hook.NewHookExecutor(mockExecutor)
			var err error
			if tc.hookType == "pre" {
				err = executor.RunPreHook(context.Background(), "test", tc.plan, time.Now())
			} else {
				err = executor.RunPostHook(context.Background(), "test", tc.plan, time.Now())
			}

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
