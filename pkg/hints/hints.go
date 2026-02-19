// Package hints provides a mechanism for identifying "soft failures" or ignorable errors
// within the system.
//
// In complex pipelines, some errors (like "task disabled" or "nothing to process")
// are not actually failures that require retries or alerts; they are simply signals
// that a step was skipped. This package allows producers to "label" these errors
// as hints, and allows consumers to identify them without needing to import
// specific sentinel errors from the producing package.
//
// This pattern promotes decoupling by using behavioral interfaces rather than
// checking for concrete error types across package boundaries.
package hints

import "errors"

type hintErr struct {
	err error
}

func (h *hintErr) Error() string {
	if h == nil || h.err == nil {
		return "unknown hint"
	}
	return h.err.Error()
}
func (h *hintErr) IsHint() bool  { return true }
func (h *hintErr) Unwrap() error { return h.err }

// New creates a hint from a string.
func New(msg string) error {
	return &hintErr{err: errors.New(msg)}
}

// Wrap takes an existing error and "promotes" it to a hint.
func Wrap(err error) error {
	if err == nil {
		return nil
	}
	return &hintErr{err: err}
}

// IsHint checks if any error in the chain behaves like a hint.
func IsHint(err error) bool {
	var h interface{ IsHint() bool }
	return errors.As(err, &h) && h.IsHint()
}

// Is checks if the error is a hint AND matches the target error.
func Is(err, target error) bool {
	return IsHint(err) && errors.Is(err, target)
}
