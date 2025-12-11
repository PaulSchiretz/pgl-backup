package util

import (
	"os"
	"runtime"
	"testing"
)

func TestWithWritePermission(t *testing.T) {
	testCases := []struct {
		name     string
		input    os.FileMode
		expected os.FileMode
	}{
		{
			name:     "Read-only permission",
			input:    0444, // r--r--r--
			expected: 0644, // rw-r--r--
		},
		{
			name:     "Already has write permission",
			input:    0755, // rwxr-xr-x
			expected: 0755, // rwxr-xr-x (should not change)
		},
		{
			name:     "No permissions",
			input:    0000, // ---------
			expected: 0200, // -w-------
		},
		{
			name:     "Execute-only permission",
			input:    0111, // --x--x--x
			expected: 0311, // -wx--x--x
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := WithWritePermission(tc.input)
			if result != tc.expected {
				t.Errorf("expected permission %o, but got %o", tc.expected, result)
			}
		})
	}
}

func TestIsCaseInsensitiveFS(t *testing.T) {
	expected := (runtime.GOOS == "windows" || runtime.GOOS == "darwin")
	if IsCaseInsensitiveFS() != expected {
		t.Errorf("IsCaseInsensitiveFS() returned %v, but expected %v for OS %s", IsCaseInsensitiveFS(), expected, runtime.GOOS)
	}
}
