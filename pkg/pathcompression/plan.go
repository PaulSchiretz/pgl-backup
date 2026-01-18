package pathcompression

type Plan struct {
	Enabled bool
	Format  Format

	// Global Flags
	DryRun   bool
	FailFast bool
	Metrics  bool
}
