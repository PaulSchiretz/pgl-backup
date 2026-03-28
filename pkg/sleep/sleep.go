package sleep

// Sleeper defines the behavior for managing system power states.
type Sleeper interface {
	Prevent(appName, appCommand string)
	Allow()
}

// New returns a platform-specific implementation of the Sleeper interface.
func New() Sleeper {
	return newSleeper()
}
