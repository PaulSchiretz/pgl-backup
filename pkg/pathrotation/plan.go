package pathrotation

type ArchiveIntervalModeConstraints struct {
	Hours  int
	Days   int
	Weeks  int
	Months int
	Years  int
}

type Plan struct {
	ArchiveEnabled         bool
	ArchiveIntervalSeconds int
	ArchiveIntervalMode    ArchiveIntervalMode
	ArchiveConstraints     ArchiveIntervalModeConstraints

	// Global Flags
	DryRun   bool
	FailFast bool
	Metrics  bool
}
