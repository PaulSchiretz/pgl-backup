package filelock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"pixelgardenlabs.io/pgl-backup/pkg/plog"
)

// LockContent defines the structure of the data written to the lock file.
type LockContent struct {
	PID        int64     `json:"pid"`
	LastUpdate time.Time `json:"last_update"`
	AppID      string    `json:"app_id"` // Optional identifier for clarity
}

// ErrLockActive is a structured error returned when a lock is already held by another process.
type ErrLockActive struct {
	PID       int64
	AppID     string
	TimeSince time.Duration
}

// Error implements the error interface for ErrLockActive.
func (e *ErrLockActive) Error() string {
	// Truncate for cleaner output, e.g., "3m2s" instead of "3m2.123456789s"
	return fmt.Sprintf("lock is active, held by PID %d (App: %s), last updated %s ago", e.PID, e.AppID, e.TimeSince.Truncate(time.Second))
}

// Lock manages the state of the acquired lock file.
type Lock struct {
	path              string
	appID             string
	heartbeatInterval time.Duration
	// The context and cancel function are used to stop the background heartbeat goroutine.
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	// We keep track if we actually hold the lock to prevent double release
	held bool
}

const (
	// staleTimeout: If a lock hasn't been updated in 3 minutes, it's considered dead.
	staleTimeout = 3 * time.Minute
	lockFileMode = 0644
)

// Acquire attempts to acquire the lock.
// ctx is used for the lifecycle of the acquisition attempt, not the background heartbeat.
func Acquire(ctx context.Context, lockFilePath string, appID string, heartbeatInterval time.Duration) (*Lock, error) {
	// We will attempt to acquire multiple times in case of race conditions during cleanup
	maxAttempts := 3

	for i := 0; i < maxAttempts; i++ {
		// Check context cancellation
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// --- 1. Attempt Atomic Acquisition ---
		lock, err := tryAcquire(lockFilePath, appID, heartbeatInterval)
		if err == nil {
			return lock, nil
		}

		// If error is NOT "file exists", it's a real filesystem error (permissions, disk full, etc)
		if !os.IsExist(err) {
			return nil, fmt.Errorf("failed to access lock file: %w", err)
		}

		// --- 2. Lock is Held, Check for Staleness ---
		content, staleErr := readLockContentSafely(lockFilePath)
		if staleErr != nil {
			// If we can't read it, wait a split second and retry loop (it might be in middle of an update)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		elapsed := time.Since(content.LastUpdate)

		// Logging at debug level to reduce noise, or Info if preferred
		// plog.Info("Lock held", "pid", content.PID, "age", elapsed)

		if elapsed < staleTimeout {
			return nil, &ErrLockActive{
				PID:       content.PID,
				AppID:     content.AppID,
				TimeSince: elapsed,
			}
		}

		// --- 3. Lock is Stale, Attempt Cleanup ---
		plog.Warn("Found stale lock", "pid", content.PID, "age", elapsed)

		// Atomic delete attempt: strictly speaking, Remove isn't atomic with the check,
		// but we are assuming the previous owner is dead.
		if removeErr := os.Remove(lockFilePath); removeErr != nil {
			// If remove fails, it might be that it was removed by someone else already
			if !os.IsNotExist(removeErr) {
				return nil, fmt.Errorf("failed to remove stale lock: %w", removeErr)
			}
		}

		// Loop continues to tryAcquire again
		plog.Info("Stale lock removed, retrying acquisition")
	}

	return nil, fmt.Errorf("failed to acquire lock after %d attempts (contention)", maxAttempts)
}

// tryAcquire attempts atomic creation using O_EXCL
func tryAcquire(path string, appID string, heartbeatInterval time.Duration) (*Lock, error) {
	// O_CREATE|O_EXCL guarantees we only succeed if file doesn't exist
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, lockFileMode)
	if err != nil {
		return nil, err
	}
	f.Close() // Close immediately, we will overwrite content via updateContent

	// Setup context for the heartbeat
	ctx, cancel := context.WithCancel(context.Background())

	l := &Lock{
		path:              path,
		appID:             appID,
		heartbeatInterval: heartbeatInterval,
		ctx:               ctx,
		cancel:            cancel,
		held:              true,
	}

	// Write initial data immediately.
	// If this fails, we must clean up the empty file we just created.
	if err := l.updateContent(); err != nil {
		l.cleanup()
		return nil, err
	}

	go l.heartbeat()

	return l, nil
}

// Release stops heartbeat and removes file.
func (l *Lock) Release() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.held {
		return
	}

	l.cancel() // Stop heartbeat
	l.cleanup()
	l.held = false
}

func (l *Lock) cleanup() {
	if err := os.Remove(l.path); err != nil {
		// If file is already gone, that's fine.
		if !os.IsNotExist(err) {
			plog.Warn("Failed to remove lock file", "path", l.path, "error", err)
		}
	} else {
		plog.Info("Lock released", "path", l.path)
	}
}

func (l *Lock) heartbeat() {
	ticker := time.NewTicker(l.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-l.ctx.Done():
			return
		case <-ticker.C:
			if err := l.updateContent(); err != nil {
				plog.Warn("Heartbeat failed to update lock file", "error", err)
				// Optional: If we fail to write too many times, should we suicide?
				// For now, we just log.
			}
		}
	}
}

// updateContent writes current state to file.
// Note: We use os.WriteFile which truncates. Ideally, we'd write to temp and rename,
// but Rename changes the inode/file identity which might confuse some external observers.
// Given the readLockContentSafely implementation, truncation is acceptable.
func (l *Lock) updateContent() error {
	content := LockContent{
		PID:        int64(os.Getpid()),
		LastUpdate: time.Now(),
		AppID:      l.appID,
	}

	data, err := json.MarshalIndent(content, "", "  ")
	if err != nil {
		return err
	}

	// os.WriteFile opens with O_WRONLY|O_CREATE|O_TRUNC
	return os.WriteFile(l.path, data, lockFileMode)
}

// readLockContentSafely attempts to read the lock file, handling the race condition
// where the file exists but is currently being truncated/written to (empty or partial).
func readLockContentSafely(path string) (LockContent, error) {
	var lastErr error

	// Try reading a few times if we encounter JSON syntax errors or empty files
	// which happen during the updateContent() write cycle.
	for i := 0; i < 3; i++ {
		f, err := os.Open(path)
		if err != nil {
			return LockContent{}, err
		}

		data, err := io.ReadAll(f)
		f.Close() // Close explicitly before potential sleep
		if err != nil {
			lastErr = err
			time.Sleep(50 * time.Millisecond)
			continue
		}

		if len(data) == 0 {
			lastErr = errors.New("lock file is empty")
			time.Sleep(50 * time.Millisecond)
			continue
		}

		var content LockContent
		if err := json.Unmarshal(data, &content); err != nil {
			lastErr = err
			time.Sleep(50 * time.Millisecond)
			continue
		}

		return content, nil
	}

	return LockContent{}, fmt.Errorf("failed to read valid lock content: %w", lastErr)
}
