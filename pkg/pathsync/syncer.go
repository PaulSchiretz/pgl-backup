package pathsync

import (
	"context"
)

// syncer defines the interface for syncing directories
type syncer interface {
	Sync(ctx context.Context, absSourcePath, absTargetPath string) error
}
