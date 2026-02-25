package hook

type Plan struct {
	Enabled bool

	PreHookCommands  []string
	PostHookCommands []string

	// Global Flags
	DryRun   bool
	FailFast bool
	Metrics  bool
}
