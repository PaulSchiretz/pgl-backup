package util

import (
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
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
