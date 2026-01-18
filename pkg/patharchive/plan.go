package patharchive

import (
	"github.com/paulschiretz/pgl-backup/pkg/metafile"
)

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
	ResultInfo      metafile.MetafileInfo

	// Global Flags
	DryRun   bool
	FailFast bool
	Metrics  bool
}
