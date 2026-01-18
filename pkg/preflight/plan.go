package preflight

type Plan struct {
	ConfigValid        bool
	SourceAccessible   bool
	TargetAccessible   bool
	TargetWriteable    bool
	CaseMismatch       bool
	PathNesting        bool
	EnsureTargetExists bool

	// Global Flags
	DryRun   bool
	FailFast bool
	Metrics  bool
}
