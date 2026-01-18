package lockfile

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// TestAcquireAndRelease verifies the basic functionality of acquiring and releasing a lock.
func TestAcquireAndRelease(t *testing.T) {
	dir := t.TempDir()
	expectedLockPath := filepath.Join(dir, LockFileName)

	// Acquire the lock
	lock, err := Acquire(context.Background(), dir, "test-app")
	if err != nil {
		t.Fatalf("expected to acquire lock, but got error: %v", err)
	}

	// Check that the lock file was created
	if _, err := os.Stat(expectedLockPath); os.IsNotExist(err) {
		t.Fatal("lock file was not created after acquiring lock")
	}

	// Release the lock
	lock.Release()

	// Check that the lock file was removed
	if _, err := os.Stat(expectedLockPath); !os.IsNotExist(err) {
		t.Fatal("lock file was not removed after releasing lock")
	}
}

// TestContention ensures that a second process cannot acquire an active lock.
func TestContention(t *testing.T) {
	dir := t.TempDir()

	// Process 1 acquires the lock
	lock1, err := Acquire(context.Background(), dir, "app-1")
	if err != nil {
		t.Fatalf("Process 1 failed to acquire lock: %v", err)
	}
	defer lock1.Release()

	// Process 2 attempts to acquire the same lock
	_, err = Acquire(context.Background(), dir, "app-2")
	if err == nil {
		t.Fatal("Process 2 unexpectedly acquired an active lock")
	}

	// Check that the error is the specific ErrLockActive type
	var lockErr *ErrLockActive
	if !errors.As(err, &lockErr) {
		t.Fatalf("expected error of type *ErrLockActive, but got %T: %v", err, err)
	}

	if lockErr.AppID != "app-1" {
		t.Errorf("expected lock error to report AppID 'app-1', but got '%s'", lockErr.AppID)
	}
}

// TestStaleLockCleanup verifies that a stale lock can be acquired.
func TestStaleLockCleanup(t *testing.T) {
	dir := t.TempDir()
	lockPath := filepath.Join(dir, LockFileName)

	// Manually create a stale lock file
	staleTimeVal := time.Now().Add(-(staleTimeout + time.Minute)) // Well past the stale timeout
	staleContent := LockContent{
		PID:        12345, // A fake PID from a "dead" process
		Hostname:   "stale-host",
		LastUpdate: staleTimeVal,
		Nonce:      "stale-nonce",
		AppID:      "stale-app",
	}
	data, _ := json.Marshal(staleContent)
	if err := os.WriteFile(lockPath, data, util.UserWritableFilePerms); err != nil {
		t.Fatalf("failed to create stale lock file: %v", err)
	}

	// Attempt to acquire the stale lock
	lock, err := Acquire(context.Background(), dir, "new-app")
	if err != nil {
		t.Fatalf("failed to acquire stale lock: %v", err)
	}
	defer lock.Release()

	// Verify the new lock content
	content, err := readLockContentSafely(lockPath)
	if err != nil {
		t.Fatalf("failed to read content of newly acquired lock: %v", err)
	}

	if content.AppID != "new-app" {
		t.Errorf("expected new lock to have AppID 'new-app', but got '%s'", content.AppID)
	}
}

// TestStaleLockContention simulates a race condition where two processes
// try to acquire the same stale lock simultaneously.
func TestStaleLockContention(t *testing.T) {
	dir := t.TempDir()
	lockPath := filepath.Join(dir, LockFileName)

	// 1. Create a stale lock file that both processes will try to acquire.
	staleTimeVal := time.Now().Add(-(staleTimeout + time.Minute))
	staleContent := LockContent{
		PID:        12345, // A fake PID from a "dead" process
		Hostname:   "stale-host",
		LastUpdate: staleTimeVal,
		Nonce:      "stale-nonce",
		AppID:      "stale-app",
	}
	data, _ := json.Marshal(staleContent)
	if err := os.WriteFile(lockPath, data, util.UserWritableFilePerms); err != nil {
		t.Fatalf("failed to create stale lock file: %v", err)
	}

	// 2. Use a WaitGroup and channels to run two acquisition attempts concurrently.
	var wg sync.WaitGroup
	results := make(chan error, 2)
	acquiredLocks := make(chan *Lock, 2)

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			lock, err := Acquire(context.Background(), dir, "contender")
			if err != nil {
				results <- err
				return
			}
			acquiredLocks <- lock
		}(i)
	}

	wg.Wait()
	close(results)
	close(acquiredLocks)

	// 3. Analyze the results.
	// We expect exactly one goroutine to acquire the lock and the other to fail.
	if len(acquiredLocks) != 1 {
		t.Fatalf("expected exactly one process to acquire the lock, but %d succeeded", len(acquiredLocks))
	}

	// Clean up the one successful lock.
	for lock := range acquiredLocks {
		lock.Release()
	}

	// We don't need to check the specific error from the losing goroutine,
	// as it might be ErrLostRace on one attempt and ErrLockActive on the next.
	// The critical assertion is that only one goroutine succeeded.
}

// TestHeartbeatEffect ensures an active lock with a heartbeat is not considered stale.
func TestHeartbeatEffect(t *testing.T) {
	// Temporarily override package-level vars for a fast test.
	originalHeartbeat := heartbeatInterval
	originalStale := staleTimeout
	heartbeatInterval = 50 * time.Millisecond
	staleTimeout = 3 * heartbeatInterval
	t.Cleanup(func() {
		heartbeatInterval = originalHeartbeat
		staleTimeout = originalStale
	})

	dir := t.TempDir()

	// Acquire the lock, which starts the heartbeat
	lock1, err := Acquire(context.Background(), dir, "app-1")
	if err != nil {
		t.Fatalf("failed to acquire initial lock: %v", err)
	}
	defer lock1.Release()

	// Wait for a period longer than one heartbeat but shorter than the stale timeout
	time.Sleep(heartbeatInterval + 25*time.Millisecond)

	// Attempt to acquire the lock again. It should fail because the heartbeat kept it fresh.
	_, err = Acquire(context.Background(), dir, "app-2")
	if err == nil {
		t.Fatal("expected lock acquisition to fail, but it succeeded")
	}

	var lockErr *ErrLockActive
	if !errors.As(err, &lockErr) {
		t.Fatalf("expected ErrLockActive, but got %T", err)
	}
}

// TestReleaseIdempotency verifies that calling Release multiple times is safe.
func TestReleaseIdempotency(t *testing.T) {
	dir := t.TempDir()
	expectedLockPath := filepath.Join(dir, LockFileName)

	lock, err := Acquire(context.Background(), dir, "test-app")
	if err != nil {
		t.Fatalf("failed to acquire lock: %v", err)
	}

	// Release multiple times
	lock.Release()
	lock.Release() // This should not panic or cause an error

	// Check that the lock file is gone
	if _, err := os.Stat(expectedLockPath); !os.IsNotExist(err) {
		t.Fatal("lock file still exists after multiple releases")
	}
}

// TestReadLockContentSafely tests the retry logic for reading a lock file.
func TestReadLockContentSafely(t *testing.T) {
	lockPath := filepath.Join(t.TempDir(), "test.lock")

	t.Run("Reads valid file", func(t *testing.T) {
		hostname, _ := os.Hostname()
		content := LockContent{PID: 1, AppID: "valid", Hostname: hostname, Nonce: "abc"}
		data, _ := json.Marshal(content)
		if err := os.WriteFile(lockPath, data, util.UserWritableFilePerms); err != nil {
			t.Fatalf("failed to write test lock file: %v", err)
		}
		readContent, err := readLockContentSafely(lockPath)
		if err != nil {
			t.Fatalf("failed to read valid content: %v", err)
		}
		if readContent.AppID != "valid" {
			t.Errorf("expected AppID 'valid', got '%s'", readContent.AppID)
		}
	})

	t.Run("Fails on persistently empty file", func(t *testing.T) {
		os.WriteFile(lockPath, []byte{}, util.UserWritableFilePerms)
		if err := os.WriteFile(lockPath, []byte{}, util.UserWritableFilePerms); err != nil {
			t.Fatalf("failed to write empty file: %v", err)
		}
		_, err := readLockContentSafely(lockPath)
		if err == nil {
			t.Fatal("expected error reading empty file, but got nil")
		}
		if !errors.Is(err, ErrCorruptLockFile) {
			t.Errorf("expected error to be ErrCorruptLockFile, got: %v", err)
		}
	})

	t.Run("Fails on persistently corrupt file", func(t *testing.T) {
		if err := os.WriteFile(lockPath, []byte("{corrupt"), util.UserWritableFilePerms); err != nil {
			t.Fatalf("failed to write corrupt file: %v", err)
		}
		_, err := readLockContentSafely(lockPath)
		if err == nil {
			t.Fatal("expected error reading corrupt file, but got nil")
		}
		if !errors.Is(err, ErrCorruptLockFile) {
			t.Errorf("expected error to be ErrCorruptLockFile, got: %v", err)
		}
	})

	t.Run("Succeeds after transient empty state", func(t *testing.T) {
		// Simulate a file being written: empty -> content
		if err := os.WriteFile(lockPath, []byte{}, util.UserWritableFilePerms); err != nil {
			t.Fatalf("failed to write initial empty file: %v", err)
		}

		go func() {
			time.Sleep(20 * time.Millisecond) // Give read a chance to see the empty file
			hostname, _ := os.Hostname()
			content := LockContent{PID: 2, AppID: "transient", Hostname: hostname, Nonce: "xyz"}
			data, _ := json.Marshal(content)
			if err := os.WriteFile(lockPath, data, util.UserWritableFilePerms); err != nil {
				// Can't t.Fatal in a goroutine, but this will cause the main test to fail.
				t.Logf("error writing final content in goroutine: %v", err)
			}
		}()

		readContent, err := readLockContentSafely(lockPath)
		if err != nil {
			t.Fatalf("failed to read transiently empty file: %v", err)
		}
		if readContent.AppID != "transient" {
			t.Errorf("expected AppID 'transient', got '%s'", readContent.AppID)
		}
	})
}
func TestCleanupTempLockFiles(t *testing.T) {
	dir := t.TempDir()
	lockPath := filepath.Join(dir, "test.lock")

	// 1. Create an old temp file that should be deleted.
	oldTempPath := filepath.Join(dir, "test.lock.123.tmp")
	if err := os.WriteFile(oldTempPath, []byte("old"), 0644); err != nil {
		t.Fatalf("failed to create old temp file: %v", err)
	}
	// Set its modification time to be older than the stale timeout.
	oldTime := time.Now().Add(-(staleTimeout + time.Minute))
	if err := os.Chtimes(oldTempPath, oldTime, oldTime); err != nil {
		t.Fatalf("failed to set mod time on old temp file: %v", err)
	}

	// 2. Create a new temp file that should NOT be deleted.
	newTempPath := filepath.Join(dir, "test.lock.456.tmp")
	if err := os.WriteFile(newTempPath, []byte("new"), 0644); err != nil {
		t.Fatalf("failed to create new temp file: %v", err)
	}

	// Act
	cleanupTempLockFiles(lockPath)

	// Assert
	if _, err := os.Stat(oldTempPath); !os.IsNotExist(err) {
		t.Error("expected old temporary file to be deleted, but it still exists")
	}
	if _, err := os.Stat(newTempPath); err != nil {
		t.Errorf("expected new temporary file to be kept, but it was deleted or an error occurred: %v", err)
	}
}
