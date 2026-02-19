package hints_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/paulschiretz/pgl-backup/pkg/hints"
)

func TestHint(t *testing.T) {
	// Define some base errors for testing.
	var (
		errBase      = errors.New("base error")
		errAnother   = errors.New("another error")
		errHinted    = hints.Wrap(errBase)
		errHintedMsg = hints.New("hint message")
		errWrapped   = hints.Wrap(errBase)
	)

	t.Run("Wrap", func(t *testing.T) {
		if hints.Wrap(nil) != nil {
			t.Error("Wrap(nil) should return nil")
		}

		if errWrapped == nil {
			t.Fatal("Wrap(err) should return a non-nil error")
		}
	})

	t.Run("New", func(t *testing.T) {
		if errHintedMsg == nil {
			t.Fatal("New should return a non-nil error")
		}
		if errHintedMsg.Error() != "hint message" {
			t.Errorf("expected error message %q, got %q", "hint message", errHintedMsg.Error())
		}
	})

	t.Run("IsHint", func(t *testing.T) {
		testCases := []struct {
			name     string
			err      error
			expected bool
		}{
			{"NilError", nil, false},
			{"StandardError", errBase, false},
			{"HintedError", errHinted, true},
			{"HintedMsgError", errHintedMsg, true},
			{"WrappedError", errWrapped, true},
			{"WrappedHint", fmt.Errorf("wrapper: %w", errHinted), true},
			{"WrappedStandardError", fmt.Errorf("wrapper: %w", errBase), false},
			{"DoubleWrappedHint", fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", errHinted)), true},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				if got := hints.IsHint(tc.err); got != tc.expected {
					t.Errorf("IsHint() = %v, want %v", got, tc.expected)
				}
			})
		}
	})

	t.Run("Unwrap and Is", func(t *testing.T) {
		if !errors.Is(errHinted, errBase) {
			t.Error("errors.Is should find the underlying error in a hint")
		}

		if errors.Is(errHinted, errAnother) {
			t.Error("errors.Is should not find an unrelated error")
		}

		unwrapped := errors.Unwrap(errHinted)
		if unwrapped != errBase {
			t.Errorf("errors.Unwrap should return the original error, got %v", unwrapped)
		}
	})

	t.Run("Is (Target)", func(t *testing.T) {
		if !hints.Is(errHinted, errBase) {
			t.Error("Is(hinted, base) should be true")
		}
		if hints.Is(errBase, errBase) {
			t.Error("Is(base, base) should be false because it is not a hint")
		}
		if hints.Is(errHinted, errAnother) {
			t.Error("Is(hinted, another) should be false")
		}
	})
}
