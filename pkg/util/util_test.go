package util

import (
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"testing"
)

func TestWithUserReadPermission(t *testing.T) {
	testCases := []struct {
		name     string
		input    os.FileMode
		expected os.FileMode
	}{
		{
			name:     "Write-only permission",
			input:    PermUserWrite,                // 0200
			expected: PermUserWrite | PermUserRead, // 0600
		},
		{
			name:     "Already has read permission",
			input:    UserWritableDirPerms, // 0755
			expected: UserWritableDirPerms, // 0755 (should not change)
		},
		{
			name:     "No permissions",
			input:    0000,         // ---------
			expected: PermUserRead, // 0400
		},
		{
			name:     "Execute-only permission",
			input:    PermUserExecute,                // 0100
			expected: PermUserExecute | PermUserRead, // 0500
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := WithUserReadPermission(tc.input)
			if result != tc.expected {
				t.Errorf("expected permission %o, but got %o", tc.expected, result)
			}
		})
	}
}

func TestWithUserWritePermission(t *testing.T) {
	testCases := []struct {
		name     string
		input    os.FileMode
		expected os.FileMode
	}{
		{
			name:     "Read-only permission",
			input:    PermUserRead,                 // 0400
			expected: PermUserRead | PermUserWrite, // 0600
		},
		{
			name:     "Already has write permission",
			input:    UserWritableDirPerms, // 0755
			expected: UserWritableDirPerms, // 0755 (should not change)
		},
		{
			name:     "No permissions",
			input:    0000,          // ---------
			expected: PermUserWrite, // 0200
		},
		{
			name:     "Execute-only permission",
			input:    PermUserExecute,                 // 0100
			expected: PermUserExecute | PermUserWrite, // 0300
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := WithUserWritePermission(tc.input)
			if result != tc.expected {
				t.Errorf("expected permission %o, but got %o", tc.expected, result)
			}
		})
	}
}

func TestWithUserExecutePermission(t *testing.T) {
	testCases := []struct {
		name     string
		input    os.FileMode
		expected os.FileMode
	}{
		{
			name:     "Read-only permission",
			input:    PermUserRead,                   // 0400
			expected: PermUserRead | PermUserExecute, // 0500
		},
		{
			name:     "Already has execute permission",
			input:    UserWritableDirPerms, // 0755
			expected: UserWritableDirPerms, // 0755 (should not change)
		},
		{
			name:     "No permissions",
			input:    0000,            // ---------
			expected: PermUserExecute, // 0100
		},
		{
			name:     "Write-only permission",
			input:    PermUserWrite,                   // 0200
			expected: PermUserWrite | PermUserExecute, // 0300
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := WithUserExecutePermission(tc.input)
			if result != tc.expected {
				t.Errorf("expected permission %o, but got %o", tc.expected, result)
			}
		})
	}
}

func TestIsHostCaseInsensitiveFS(t *testing.T) {
	expected := (runtime.GOOS == "windows" || runtime.GOOS == "darwin")
	if IsHostCaseInsensitiveFS() != expected {
		t.Errorf("IsHostCaseInsensitiveFS() returned %v, but expected %v for OS %s", IsHostCaseInsensitiveFS(), expected, runtime.GOOS)
	}
}

func TestNormalizeAndDenormalizePath(t *testing.T) {
	// On Windows, this will be `a\b\c`
	// On Linux/macOS, this will be `a/b/c`
	nativePath := filepath.Join("a", "b", "c")

	// Normalization should always produce forward slashes.
	normalized := NormalizePath(nativePath)
	expectedNormalized := "a/b/c"
	if normalized != expectedNormalized {
		t.Errorf("NormalizePath: expected %q, but got %q", expectedNormalized, normalized)
	}

	// Denormalization should convert back to the native format.
	denormalized := DenormalizePath(normalized)
	if denormalized != nativePath {
		t.Errorf("DenormalizePath: expected %q, but got %q", nativePath, denormalized)
	}
}

func TestExpandPath(t *testing.T) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("could not get user home directory: %v", err)
	}

	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Path with tilde",
			input:    "~/testdir",
			expected: filepath.Join(homeDir, "testdir"),
		},
		{
			name:     "Path with just tilde",
			input:    "~",
			expected: homeDir,
		},
		{
			name:     "Path with tilde and separator",
			input:    "~/",
			expected: homeDir,
		},
		{
			name:     "Path without tilde",
			input:    "/var/log",
			expected: "/var/log",
		},
		{
			name:     "Empty path",
			input:    "",
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ExpandPath(tc.input)
			if err != nil {
				t.Errorf("ExpandPath() returned an unexpected error: %v", err)
			}
			if result != tc.expected {
				t.Errorf("expected path %q, but got %q", tc.expected, result)
			}
		})
	}
}

func TestInvertMap(t *testing.T) {
	t.Run("String to Int", func(t *testing.T) {
		input := map[string]int{"a": 1, "b": 2}
		expected := map[int]string{1: "a", 2: "b"}
		result := InvertMap(input)
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("expected %v, but got %v", expected, result)
		}
	})

	t.Run("Empty Map", func(t *testing.T) {
		input := map[string]int{}
		expected := map[int]string{}
		result := InvertMap(input)
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("expected %v, but got %v", expected, result)
		}
	})
}

func TestMergeAndDeduplicate(t *testing.T) {
	testCases := []struct {
		name     string
		input    [][]string
		expected []string
	}{
		{
			name:     "No duplicates",
			input:    [][]string{{"a", "b"}, {"c", "d"}},
			expected: []string{"a", "b", "c", "d"},
		},
		{
			name:     "Duplicates within one slice",
			input:    [][]string{{"a", "a", "b"}},
			expected: []string{"a", "b"},
		},
		{
			name:     "Duplicates across slices",
			input:    [][]string{{"a", "b"}, {"b", "c"}},
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "With empty slices",
			input:    [][]string{{"a"}, {}, {"b"}},
			expected: []string{"a", "b"},
		},
		{
			name:     "Single slice",
			input:    [][]string{{"a", "b", "a"}},
			expected: []string{"a", "b"},
		},
		{
			name:     "No slices",
			input:    [][]string{},
			expected: []string{},
		},
		{
			name:     "All slices empty",
			input:    [][]string{{}, {}},
			expected: []string{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := MergeAndDeduplicate(tc.input...)

			// Sort both slices because the order of elements from a map is not guaranteed.
			sort.Strings(result)
			sort.Strings(tc.expected)

			if !reflect.DeepEqual(result, tc.expected) {
				t.Errorf("expected %v, but got %v", tc.expected, result)
			}
		})
	}
}
