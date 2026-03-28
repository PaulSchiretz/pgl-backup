package sleep

import (
	"testing"
)

// TestInterfaceSatisfaction ensures that New() actually returns
// something that adheres to our Sleeper interface.
func TestInterfaceSatisfaction(t *testing.T) {
	var s Sleeper = New()
	if s == nil {
		t.Fatal("New() returned nil; expected a platform-specific Sleeper")
	}
}

// TestExecutionFlow runs the methods to ensure no nil-pointer
// dereferences or panics occur during a standard lifecycle.
func TestExecutionFlow(t *testing.T) {
	s := New()

	// We test Allow first to ensure it handles "not-yet-prevented" state gracefully
	s.Allow()

	// Test a standard cycle
	s.Prevent("test-app", "test-reason")
	s.Allow()
}
