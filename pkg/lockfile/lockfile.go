package lockfile

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// LockFileName is the name of the lock file created in the target directory.
// The '~' prefix marks it as temporary.
const LockFileName = ".~pgl-backup.lock"

// LockContent defines the structure of the data written to the lock file.
type LockContent struct {
	PID        int64     `json:"pid"`
	Hostname   string    `json:"hostname"`
	LastUpdate time.Time `json:"lastUpdate"`
	Nonce      string    `json:"nonce,omitempty"` // Used for takeover race resolution
	AppID      string    `json:"appID"`
}

// ErrLockActive is a structured error returned when a lock is already held by another process.
type ErrLockActive struct {
	PID       int64
	Hostname  string
	AppID     string
	TimeSince time.Duration
}

// Error implements the error interface for ErrLockActive.
func (e *ErrLockActive) Error() string {
	// Truncate for cleaner output, e.g., "3m2s" instead of "3m2.123456789s".
	return fmt.Sprintf("lock is active, held by PID %d on host '%s' (App: %s), last updated %s ago", e.PID, e.Hostname, e.AppID, e.TimeSince.Truncate(time.Second))
}

// ErrLostRace is a sentinel error returned when a process attempts to take over a stale lock but another process wins.
var ErrLostRace = errors.New("lost race during stale lock takeover")

// ErrCorruptLockFile indicates that the lock file on disk is unreadable, either empty or containing invalid JSON.
var ErrCorruptLockFile = errors.New("lock file is corrupt or empty")

// Lock manages the state of the acquired lock file.
type Lock struct {
	path    string
	content LockContent
	// The context and cancel function are used to stop the background heartbeat goroutine.
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	// We keep track if we actually hold the lock to prevent double release
	held bool
}

// These are vars to allow modification during testing.
var (
	heartbeatInterval = 1 * time.Minute
	// staleTimeout is defined in relation to the heartbeat to ensure a safe margin.
	staleTimeout = 3 * heartbeatInterval
)

// Acquire attempts to acquire the lock.
// ctx is used for the lifecycle of the acquisition attempt, not the background heartbeat.
// It returns a non-nil Lock on success.
// It returns (nil, *ErrLockActive) if the lock is already held.
// It returns (nil, error) for any other failure.
func Acquire(ctx context.Context, dirPath string, appID string) (*Lock, error) {

	absLockFilePath := filepath.Join(dirPath, LockFileName)
	// We will attempt to acquire multiple times in case of race conditions during cleanup
	maxAttempts := 3

	for range maxAttempts {
		// Check context cancellation
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// --- 1. Attempt Atomic Acquisition ---
		lock, err := tryAcquire(absLockFilePath, appID)
		if err == nil {
			// Synchronously clean up any old temp files before starting the heartbeat.
			cleanupTempLockFiles(absLockFilePath)
			go lock.heartbeat()
			return lock, nil
		}

		// If error is NOT "file exists", it's a real filesystem error (permissions, disk full, etc)
		if !os.IsExist(err) {
			return nil, fmt.Errorf("failed to access lock file: %w", err)
		}

		// --- 2. Lock is Held, Check for Staleness ---
		content, staleErr := readLockContentSafely(absLockFilePath)
		if staleErr != nil {
			// If the file is persistently corrupt or empty, we can treat it as stale and attempt a takeover.
			if errors.Is(staleErr, ErrCorruptLockFile) {
				plog.Warn("Found corrupt lock file, treating as stale", "path", absLockFilePath, "error", staleErr)
				// Fall through to the takeover logic below.
			} else {
				// A different read error occurred (e.g., permissions), so retry.
				time.Sleep(100 * time.Millisecond)
				continue
			}
		} else {
			// Check valid content time
			elapsed := time.Since(content.LastUpdate)
			if elapsed < staleTimeout {
				return nil, &ErrLockActive{
					PID:       content.PID,
					Hostname:  content.Hostname,
					AppID:     content.AppID,
					TimeSince: elapsed,
				}
			}
			plog.Warn("Found stale lock, attempting takeover", "pid", content.PID, "age", elapsed)
		}

		// 3. Lock is Stale or Corrupt, Attempt Takeover
		lock, takeoverErr := attemptStaleLockTakeover(absLockFilePath, appID)
		if takeoverErr != nil {
			if errors.Is(takeoverErr, ErrLostRace) {
				plog.Debug("Lock takeover race lost, retrying acquisition")
			} else {
				plog.Warn("Failed to attempt lock takeover, retrying", "error", takeoverErr)
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Synchronously clean up any old temp files before starting the heartbeat.
		cleanupTempLockFiles(absLockFilePath)
		go lock.heartbeat()
		return lock, nil
	}

	return nil, fmt.Errorf("failed to acquire lock after %d attempts (contention)", maxAttempts)
}

// tryAcquire attempts atomic creation using O_EXCL to guarantee "I created this file first".
func tryAcquire(absLockFilePath string, appID string) (*Lock, error) {
	// O_CREATE|O_EXCL guarantees we only succeed if file doesn't exist
	f, err := os.OpenFile(absLockFilePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, util.UserWritableFilePerms)
	if err != nil {
		return nil, err
	}
	// We have the file handle. Defer closing it, but ensure it's closed before we exit this function.
	defer f.Close()

	nonce, err := generateNonce()
	if err != nil {
		return nil, err
	}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	content := LockContent{
		PID:        int64(os.Getpid()),
		Hostname:   hostname,
		LastUpdate: time.Now().UTC(),
		Nonce:      nonce,
		AppID:      appID,
	}

	// Create the lock object and start its lifecycle.
	l := newLock(absLockFilePath, content)

	// Write initial data immediately.
	// If this fails, we must clean up the empty file we just created.
	if err := writeLockContent(f, content); err != nil {
		l.cleanup()
		return nil, err // writeLockContent will provide a descriptive error
	}

	return l, nil
}

// newLock creates a new Lock object and sets up its context for the heartbeat.
func newLock(absLockFilePath string, content LockContent) *Lock {
	// Setup context for the heartbeat
	ctx, cancel := context.WithCancel(context.Background())
	return &Lock{
		path:    absLockFilePath,
		content: content,
		ctx:     ctx,
		cancel:  cancel,
		held:    true,
	}
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

// attemptStaleLockTakeover uses an atomic rename strategy to seize a stale or
// corrupt lock. It writes new lock content to a temporary file and then renames
// it over the existing lock file, guaranteeing an atomic update.
func attemptStaleLockTakeover(absLockFilePath, appID string) (*Lock, error) {
	// Generate a unique nonce for this specific takeover attempt. This is the key
	nonce, err := generateNonce()
	if err != nil {
		return nil, err
	}

	myPID := int64(os.Getpid())
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	takeoverContent := LockContent{
		PID:        myPID,
		Hostname:   hostname,
		LastUpdate: time.Now().UTC(),
		AppID:      appID,
		Nonce:      nonce,
	}

	// This ensures that if we crash during takeover, we don't leave a 0-byte file.
	if err := updateLockFileAtomic(absLockFilePath, takeoverContent); err != nil {
		return nil, err
	}

	// Read back immediately to verify we won the race.
	readbackContent, readbackErr := readLockContentSafely(absLockFilePath)
	if readbackErr != nil {
		return nil, fmt.Errorf("failed to read back lock file after takeover: %w", readbackErr)
	}

	if readbackContent.PID == myPID && readbackContent.Nonce == nonce {
		plog.Debug("Successfully took over stale lock")
		l := newLock(absLockFilePath, takeoverContent)
		return l, nil
	}
	return nil, ErrLostRace
}

func (l *Lock) cleanup() {
	if err := os.Remove(l.path); err != nil {
		// If file is already gone, that's fine.
		if !os.IsNotExist(err) {
			plog.Warn("Failed to remove lock file", "path", l.path, "error", err)
		}
	} else {
		plog.Debug("Lock released", "path", l.path)
	}
}

func (l *Lock) heartbeat() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-l.ctx.Done():
			return
		case <-ticker.C:
			// Update the timestamp on our internal content and update the file
			l.content.LastUpdate = time.Now().UTC()
			if err := updateLockFileAtomic(l.path, l.content); err != nil {
				plog.Warn("Heartbeat failed to update lock file", "error", err)
				// Note: We do not exit the loop. We try again next tick.
			}
		}
	}
}

// updateLockFileAtomic writes the content to a temporary file and then renames it
// over the target path. This ensures the file at 'path' is never empty/corrupt.
func updateLockFileAtomic(absLockFilePath string, content LockContent) error {
	// 1. Create a temp file in the SAME DIRECTORY as the target.
	// This is crucial: os.Rename ensures atomicity only within the same filesystem.
	dir := filepath.Dir(absLockFilePath)

	// Pattern "lock.*.tmp" helps identify these if they get left behind (unlikely)
	tmpF, err := os.CreateTemp(dir, filepath.Base(absLockFilePath)+".*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temp lock file: %w", err)
	}

	// Ensure we clean up the temp file if we error out before the rename
	defer func() {
		// Clean up the temp file. We only care about errors that are NOT "file not found",
		// as that error is expected on a successful rename.
		if err := os.Remove(tmpF.Name()); err != nil && !os.IsNotExist(err) {
			plog.Warn("Failed to remove temporary lock file", "path", tmpF.Name(), "error", err)
		}
	}()

	// 2. Write the JSON content
	if err := writeLockContent(tmpF, content); err != nil {
		tmpF.Close()
		return err
	}

	// 3. Sync and Close
	// Sync ensures data is flushed to physical disk (or at least OS cache) before rename
	if err := tmpF.Sync(); err != nil {
		tmpF.Close()
		return err
	}

	// Must close the file before renaming (mandatory on Windows, good practice elsewhere)
	if err := tmpF.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	// 4. Atomic Rename
	// This replaces 'targetPath' with 'tmpF.Name()' atomically.
	if err := os.Rename(tmpF.Name(), absLockFilePath); err != nil {
		return fmt.Errorf("failed to rename temp file to lock file: %w", err)
	}

	return nil
}

// cleanupTempLockFiles scans the lock directory for any leftover temporary files
// from previous crashed runs. It only deletes files older than the staleTimeout
// to avoid deleting temp files currently being written by active processes.
func cleanupTempLockFiles(absLockFilePath string) {
	dir := filepath.Dir(absLockFilePath)
	pattern := filepath.Join(dir, filepath.Base(absLockFilePath)+".*.tmp")

	matches, err := filepath.Glob(pattern)
	if err != nil {
		plog.Warn("Failed to glob for temporary lock files", "pattern", pattern, "error", err)
		return
	}

	// Safety threshold: Only delete files unmodified for longer than the stale timeout.
	// This prevents us from deleting a temp file that is currently being written
	// by the owner of the lock during a heartbeat.
	threshold := time.Now().Add(-staleTimeout)

	for _, match := range matches {
		info, err := os.Stat(match)
		if err != nil {
			// If stat fails (e.g. file already gone), just skip it
			continue
		}

		if info.ModTime().Before(threshold) {
			plog.Debug("Removing old temporary lock file", "path", match, "age", time.Since(info.ModTime()))
			if err := os.Remove(match); err != nil && !os.IsNotExist(err) {
				plog.Warn("Failed to remove leftover temporary lock file", "path", match, "error", err)
			}
		}
	}
}

// generateNonce creates a new random 16-byte token and returns it as a hex string.
func generateNonce() (string, error) {
	nonceBytes := make([]byte, 16)
	if _, err := rand.Read(nonceBytes); err != nil {
		return "", fmt.Errorf("failed to generate nonce: %w", err)
	}
	return fmt.Sprintf("%x", nonceBytes), nil
}

// writeLockContent marshals the LockContent and writes it to the provided io.Writer.
func writeLockContent(w io.Writer, content LockContent) error {
	data, err := json.MarshalIndent(content, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal lock content: %w", err)
	}
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("failed to write lock content: %w", err)
	}
	return nil
}

// readLockContentSafely attempts to read the lock file, handling the race condition
// where the file exists but is currently being truncated/written to (empty or partial).
// NOTE: Even with an atomic rename strategy for writes, filesystems can have
// transient states. This retry logic provides a robust defense against such edge cases.
func readLockContentSafely(absLockFilePath string) (LockContent, error) {
	var lastErr error
	var lastEmptyOrCorruptErr error
	// Try reading a few times if we encounter JSON syntax errors or empty files
	// which happen during the updateContent() write cycle.
	for range 3 {
		f, err := os.Open(absLockFilePath)
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
			lastEmptyOrCorruptErr = fmt.Errorf("lock file is empty")
			time.Sleep(50 * time.Millisecond)
			continue
		}

		var content LockContent
		lastEmptyOrCorruptErr = json.Unmarshal(data, &content)
		if lastEmptyOrCorruptErr != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		return content, nil
	}

	// After multiple retries, if the last error was due to an empty or corrupt file,
	// it indicates a persistent issue. We return a more specific error.
	if lastEmptyOrCorruptErr != nil {
		return LockContent{}, fmt.Errorf("%w: %v", ErrCorruptLockFile, lastEmptyOrCorruptErr)
	}
	return LockContent{}, fmt.Errorf("failed to read valid lock content: %w", lastErr)
}
