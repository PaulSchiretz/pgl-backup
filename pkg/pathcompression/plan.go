package pathcompression

// CompressPlan holds the configuration for a compression operation.
type CompressPlan struct {
	Enabled bool
	Format  Format
	Level   Level

	// Global Flags
	DryRun   bool
	FailFast bool
	Metrics  bool
}

// ExtractPlan holds the configuration for an extraction operation.
type ExtractPlan struct {
	Enabled           bool
	OverwriteBehavior OverwriteBehavior

	// Global Flags
	DryRun   bool
	FailFast bool
	Metrics  bool
}
