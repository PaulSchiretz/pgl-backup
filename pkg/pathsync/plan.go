package pathsync

import (
	"time"
)

type Plan struct {
	Enabled               bool
	ModeIdentifier        string
	Engine                Engine
	PreserveSourceDirName bool
	Mirror                bool
	DisableSafeCopy       bool

	RetryCount        int
	RetryWait         time.Duration
	ModTimeWindow     time.Duration // The time window to consider file modification times equal.
	OverwriteBehavior OverwriteBehavior

	ExcludeFiles []string
	ExcludeDirs  []string

	// Global Flags
	DryRun   bool
	FailFast bool
	Metrics  bool
}
