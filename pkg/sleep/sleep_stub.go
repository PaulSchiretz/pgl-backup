//go:build !windows && !darwin && !linux

package sleep

// noopSleeper is a no-op implementation for unsupported operating systems.
type noopSleeper struct{}

// newSleeper returns a stub that does nothing.
func newSleeper() Sleeper {
	return &noopSleeper{}
}

// Prevent does nothing on unsupported platforms.
func (s *noopSleeper) Prevent(appName, appCommand string) {}

// Allow does nothing on unsupported platforms.
func (s *noopSleeper) Allow() {}
