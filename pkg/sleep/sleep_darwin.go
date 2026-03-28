//go:build darwin

package sleep

import (
	"fmt"
	"os"
	"os/exec"
	"sync"

	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

type darwinSleeper struct {
	mu  sync.Mutex
	cmd *exec.Cmd
}

func newSleeper() Sleeper {
	return &darwinSleeper{}
}

func (s *darwinSleeper) Prevent(appName, appCommand string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cmd != nil {
		return
	}

	pid := fmt.Sprintf("%d", os.Getpid())
	cmd := exec.Command("caffeinate", "-i", "-w", pid)
	// If the command doesn't exist, Start() will return an error.
	if err := cmd.Start(); err != nil {
		plog.Warn("Warning: sleep prevention not supported on this system: %v", err)
		return
	}

	s.cmd = cmd
}

func (s *darwinSleeper) Allow() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cmd != nil && s.cmd.Process != nil {
		s.cmd.Process.Kill()
		s.cmd.Wait()
		s.cmd = nil
	}
}
