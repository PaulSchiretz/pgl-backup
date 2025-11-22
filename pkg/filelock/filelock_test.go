package filelock

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestAcquireAndRelease verifies the basic functionality of acquiring and releasing a lock.
func TestAcquireAndRelease(t *testing.T) {
	lockPath := filepath.Join(t.TempDir(), "test.lock")

	// Acquire the lock
	lock, err := Acquire(context.Background(), lockPath, "test-app", 1*time.Minute)
	if err != nil {
		t.Fatalf("expected to acquire lock, but got error: %v", err)
	}

	// Check that the lock file was created
	if _, err := os.Stat(lockPath); os.IsNotExist(err) {
		t.Fatal("lock file was not created after acquiring lock")
	}

	// Release the lock
	lock.Release()

	// Check that the lock file was removed
	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Fatal("lock file was not removed after releasing lock")
	}
}

// TestContention ensures that a second process cannot acquire an active lock.
func TestContention(t *testing.T) {
	lockPath := filepath.Join(t.TempDir(), "test.lock")

	// Process 1 acquires the lock
	lock1, err := Acquire(context.Background(), lockPath, "app-1", 1*time.Minute)
	if err != nil {
		t.Fatalf("Process 1 failed to acquire lock: %v", err)
	}
	defer lock1.Release()

	// Process 2 attempts to acquire the same lock
	_, err = Acquire(context.Background(), lockPath, "app-2", 1*time.Minute)
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
	lockPath := filepath.Join(t.TempDir(), "test.lock")

	// Manually create a stale lock file
	staleTimeVal := time.Now().Add(-(staleTimeout + time.Minute)) // Well past the stale timeout
	staleContent := LockContent{
		PID:        12345,
		LastUpdate: staleTimeVal,
		AppID:      "stale-app",
	}
	data, _ := json.Marshal(staleContent)
	if err := os.WriteFile(lockPath, data, lockFileMode); err != nil {
		t.Fatalf("failed to create stale lock file: %v", err)
	}

	// Attempt to acquire the stale lock
	lock, err := Acquire(context.Background(), lockPath, "new-app", 1*time.Minute)
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

// TestHeartbeatEffect ensures an active lock with a heartbeat is not considered stale.
func TestHeartbeatEffect(t *testing.T) {
	// Use a very short heartbeat for this test to run quickly.
	testHeartbeat := 50 * time.Millisecond

	lockPath := filepath.Join(t.TempDir(), "test.lock")

	// Acquire the lock, which starts the heartbeat
	lock1, err := Acquire(context.Background(), lockPath, "app-1", testHeartbeat)
	if err != nil {
		t.Fatalf("failed to acquire initial lock: %v", err)
	}
	defer lock1.Release()

	// Wait for a period longer than one heartbeat but shorter than the stale timeout
	time.Sleep(testHeartbeat + 25*time.Millisecond)

	// Attempt to acquire the lock again. It should fail because the heartbeat kept it fresh.
	_, err = Acquire(context.Background(), lockPath, "app-2", testHeartbeat)
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
	lockPath := filepath.Join(t.TempDir(), "test.lock")

	lock, err := Acquire(context.Background(), lockPath, "test-app", 1*time.Minute)
	if err != nil {
		t.Fatalf("failed to acquire lock: %v", err)
	}

	// Release multiple times
	lock.Release()
	lock.Release() // This should not panic or cause an error

	// Check that the lock file is gone
	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Fatal("lock file still exists after multiple releases")
	}
}

// TestReadLockContentSafely tests the retry logic for reading a lock file.
func TestReadLockContentSafely(t *testing.T) {
	lockPath := filepath.Join(t.TempDir(), "test.lock")

	t.Run("Reads valid file", func(t *testing.T) {
		content := LockContent{PID: 1, AppID: "valid"}
		data, _ := json.Marshal(content)
		os.WriteFile(lockPath, data, lockFileMode)

		readContent, err := readLockContentSafely(lockPath)
		if err != nil {
			t.Fatalf("failed to read valid content: %v", err)
		}
		if readContent.AppID != "valid" {
			t.Errorf("expected AppID 'valid', got '%s'", readContent.AppID)
		}
	})

	t.Run("Fails on persistently empty file", func(t *testing.T) {
		os.WriteFile(lockPath, []byte{}, lockFileMode)
		_, err := readLockContentSafely(lockPath)
		if err == nil {
			t.Fatal("expected error reading empty file, but got nil")
		}
		if !strings.Contains(err.Error(), "lock file is empty") {
			t.Errorf("expected error about empty file, got: %v", err)
		}
	})

	t.Run("Fails on persistently corrupt file", func(t *testing.T) {
		os.WriteFile(lockPath, []byte("{corrupt"), lockFileMode)
		_, err := readLockContentSafely(lockPath)
		if err == nil {
			t.Fatal("expected error reading corrupt file, but got nil")
		}
		// The specific error for "{corrupt" is about the invalid character 'c'.
		if !strings.Contains(err.Error(), "invalid character 'c' looking for beginning of object key string") {
			t.Errorf("expected JSON syntax error, got: %v", err)
		}
	})

	t.Run("Succeeds after transient empty state", func(t *testing.T) {
		// Simulate a file being written: empty -> content
		os.WriteFile(lockPath, []byte{}, lockFileMode)

		go func() {
			time.Sleep(20 * time.Millisecond) // Give read a chance to see the empty file
			content := LockContent{PID: 2, AppID: "transient"}
			data, _ := json.Marshal(content)
			os.WriteFile(lockPath, data, lockFileMode)
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
