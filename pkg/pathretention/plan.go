package pathretention

type Plan struct {
	Enabled     bool
	Hours       int
	Days        int
	Weeks       int
	Months      int
	Years       int
	ExcludeDirs []string

	// Global Flags
	DryRun   bool
	FailFast bool
	Metrics  bool
}
