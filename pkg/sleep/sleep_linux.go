//go:build linux

package sleep

import (
	"io"
	"os/exec"
	"sync"

	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

type linuxSleeper struct {
	mu    sync.Mutex
	cmd   *exec.Cmd
	stdin io.WriteCloser
}

func newSleeper() Sleeper {
	return &linuxSleeper{}
}

func (s *linuxSleeper) Prevent(appName, appCommand string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cmd != nil {
		return
	}

	cmd := exec.Command("systemd-inhibit", "--what=sleep:idle", "--who="+appName, "--why="+appCommand, "cat")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		plog.Warn("Warning: failed to create stdin pipe for sleep prevention: %v", err)
		return
	}

	// If the command doesn't exist, Start() will return an error.
	// We check for it and just reset the command to nil so the app continues.
	if err := cmd.Start(); err != nil {
		plog.Warn("Warning: sleep prevention not supported on this system: %v", err)
		return
	}

	s.cmd = cmd
	s.stdin = stdin
}

func (s *linuxSleeper) Allow() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stdin != nil {
		s.stdin.Close()
		s.cmd.Wait()
		s.cmd = nil
		s.stdin = nil
	}
}
