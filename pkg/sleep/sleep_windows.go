//go:build windows

package sleep

import (
	"runtime"
	"sync"

	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"golang.org/x/sys/windows"
)

const (
	esContinuous     uintptr = 0x80000000
	esSystemRequired uintptr = 0x00000001
)

var (
	kernel32                = windows.NewLazySystemDLL("kernel32.dll")
	setThreadExecutionState = kernel32.NewProc("SetThreadExecutionState")
)

type windowsSleeper struct {
	mu   sync.Mutex
	done chan struct{}
}

func newSleeper() Sleeper {
	return &windowsSleeper{}
}

func (s *windowsSleeper) Prevent(appName, appCommand string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.done != nil {
		return
	}

	// Ensure the API is available on this system to avoid a panic.
	if err := setThreadExecutionState.Find(); err != nil {
		plog.Warn("Warning: sleep prevention not supported on this system: %v", err)
		return
	}

	done := make(chan struct{})

	// Use a buffered channel to ensure the goroutine doesn't block if the
	// main thread returns early due to an unexpected error or panic.
	started := make(chan bool, 1)

	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		// SetThreadExecutionState returns the previous state on success, or 0 on failure.
		r1, _, err := setThreadExecutionState.Call(esContinuous | esSystemRequired)
		if r1 == 0 {
			plog.Warn("Warning: failed to prevent sleep on Windows: %v", err)
			started <- false
			return
		}

		started <- true
		<-done
		setThreadExecutionState.Call(esContinuous)
	}()

	if <-started {
		s.done = done
	}
}

func (s *windowsSleeper) Allow() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.done != nil {
		close(s.done)
		s.done = nil
	}
}
