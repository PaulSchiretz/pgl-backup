package patharchive

type IntervalModeConstraints struct {
	Hours  int
	Days   int
	Weeks  int
	Months int
	Years  int
}

type Plan struct {
	Enabled         bool
	IntervalSeconds int
	IntervalMode    IntervalMode
	Constraints     IntervalModeConstraints

	// Global Flags
	DryRun   bool
	FailFast bool
	Metrics  bool
}
