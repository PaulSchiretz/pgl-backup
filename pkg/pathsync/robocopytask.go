package pathsync

import (
	"context"
	"time"
)

type robocopyTask struct {
	src string
	trg string

	retryCount int
	retryWait  time.Duration

	mirror       bool
	dryRun       bool
	failFast     bool
	fileExcludes []string
	dirExcludes  []string
	ctx          context.Context
	metrics      bool
}
