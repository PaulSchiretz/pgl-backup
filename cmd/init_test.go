package cmd_test

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/paulschiretz/pgl-backup/cmd"
)

func TestPromptForConfirmation(t *testing.T) {
	// Helper to mock stdin/stdout and run the function
	mockPrompt := func(input string, prompt string, defaultYes bool) (bool, string) {
		// Pipe for stdin
		rIn, wIn, _ := os.Pipe()
		// Pipe for stdout
		rOut, wOut, _ := os.Pipe()

		// Save original stdin/stdout
		origStdin := os.Stdin
		origStdout := os.Stdout
		defer func() {
			os.Stdin = origStdin
			os.Stdout = origStdout
		}()

		// Redirect
		os.Stdin = rIn
		os.Stdout = wOut

		// Write input
		go func() {
			_, _ = wIn.WriteString(input)
			_ = wIn.Close()
		}()

		// Run the function
		result := cmd.PromptForConfirmation(prompt, defaultYes)

		// Close writer to read output
		_ = wOut.Close()
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, rOut)

		return result, buf.String()
	}

	tests := []struct {
		name       string
		input      string
		prompt     string
		defaultYes bool
		want       bool
		wantPrompt string
	}{
		{"Explicit Yes", "y\n", "Continue?", false, true, "Continue? [y/N]: "},
		{"Explicit No", "n\n", "Continue?", true, false, "Continue? [Y/n]: "},
		{"Default Yes (Empty)", "\n", "Sure?", true, true, "Sure? [Y/n]: "},
		{"Default No (Empty)", "\n", "Sure?", false, false, "Sure? [y/N]: "},
		{"Case Insensitive", "YES\n", "Go?", false, true, "Go? [y/N]: "},
		{"Whitespace Handling", "   y   \n", "Clean?", false, true, "Clean? [y/N]: "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, output := mockPrompt(tt.input, tt.prompt, tt.defaultYes)
			if got != tt.want {
				t.Errorf("promptForConfirmation() = %v, want %v", got, tt.want)
			}
			if !strings.Contains(output, tt.wantPrompt) {
				t.Errorf("Output = %q, want substring %q", output, tt.wantPrompt)
			}
		})
	}
}
