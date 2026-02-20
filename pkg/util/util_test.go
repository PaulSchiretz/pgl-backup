package util

import (
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
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

func TestExpandedDenormalizedAbsPath(t *testing.T) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("could not get user home directory: %v", err)
	}

	// Create a platform-agnostic absolute path for testing
	cwd, _ := os.Getwd()
	absPath := filepath.Join(cwd, "testdir")

	testCases := []struct {
		name      string
		input     string
		expected  string
		expectErr bool
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
			name:     "Absolute path",
			input:    absPath,
			expected: absPath,
		},
		{
			name:      "Empty path",
			input:     "",
			expected:  "",
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ExpandedDenormalizedAbsPath(tc.input)
			if tc.expectErr {
				if err == nil {
					t.Errorf("expected error, but got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("ExpandedDenormalizedAbsPath() returned an unexpected error: %v", err)
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

func TestMergeAndDeduplicate_Ints(t *testing.T) {
	input1 := []int{1, 5, 2}
	input2 := []int{5, 3, 4}
	expected := []int{1, 2, 3, 4, 5}

	result := MergeAndDeduplicate(input1, input2)
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("expected %v, but got %v", expected, result)
	}
}

func TestIsPathCaseSensitive(t *testing.T) {
	// This test checks the case-sensitivity of the filesystem where the tests are running.
	// It doesn't mock different filesystem types, but validates that the detection
	// logic works correctly for the current environment.

	// 1. Create a temporary directory to perform the check in.
	tempDir := t.TempDir()

	// 2. Run the function under test on the directory.
	isSensitive, err := IsPathCaseSensitive(tempDir)
	if err != nil {
		t.Fatalf("IsPathCaseSensitive failed on directory: %v", err)
	}

	// 3. Determine the expected outcome based on the host OS.
	// IsHostCaseInsensitiveFS() returns true for Windows/macOS.
	// So, isSensitive should be false on those platforms, and true on Linux.
	expectedIsSensitive := !IsHostCaseInsensitiveFS()

	// 4. Assert that the detected sensitivity matches the expected sensitivity.
	if isSensitive != expectedIsSensitive {
		t.Errorf("detected case sensitivity (%v) does not match expected for OS %s (%v)", isSensitive, runtime.GOOS, expectedIsSensitive)
	}

	// 5. Test again, but this time passing a file path to ensure it correctly uses the parent dir.
	tempFile := filepath.Join(tempDir, "testfile.txt")
	if err := os.WriteFile(tempFile, []byte("data"), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	isSensitiveFile, err := IsPathCaseSensitive(tempFile)
	if err != nil {
		t.Fatalf("IsPathCaseSensitive failed on file path: %v", err)
	}
	if isSensitiveFile != expectedIsSensitive {
		t.Errorf("detected case sensitivity for file path (%v) does not match expected for OS %s (%v)", isSensitiveFile, runtime.GOOS, expectedIsSensitive)
	}
}

func TestGenerateUUID_Uniqueness(t *testing.T) {
	// Generate a large number of UUIDs and ensure no duplicates.
	// While not a mathematical proof of uniqueness, it catches implementation errors
	// like static seeds or fixed buffers.
	const iterations = 10000
	seen := make(map[string]struct{}, iterations)

	for i := 0; i < iterations; i++ {
		uuid, err := GenerateUUID()
		if err != nil {
			t.Fatalf("GenerateUUID failed: %v", err)
		}

		// Basic format validation (8-4-4-4-12)
		if len(uuid) != 36 {
			t.Errorf("UUID length mismatch: got %d, want 36", len(uuid))
		}
		if uuid[8] != '-' || uuid[13] != '-' || uuid[18] != '-' || uuid[23] != '-' {
			t.Errorf("UUID format invalid (missing hyphens): %s", uuid)
		}

		// Version 4 check (byte 6 high nibble is 4)
		// 00000000-0000-4000-8000-000000000000
		//               ^ index 14
		if uuid[14] != '4' {
			t.Errorf("UUID version mismatch: expected version 4, got %c in %s", uuid[14], uuid)
		}

		// Variant check (byte 8 high nibble is 8, 9, a, or b)
		// 00000000-0000-4000-8000-000000000000
		//                    ^ index 19
		switch uuid[19] {
		case '8', '9', 'a', 'b':
			// Valid variant 1 (RFC 4122)
		default:
			t.Errorf("UUID variant mismatch: expected 8, 9, a, or b, got %c in %s", uuid[19], uuid)
		}

		if _, exists := seen[uuid]; exists {
			t.Fatalf("Duplicate UUID generated: %s", uuid)
		}
		seen[uuid] = struct{}{}
	}
}

func TestFormatTimestampWithOffset(t *testing.T) {
	// Use a fixed UTC time
	ts := time.Date(2023, 10, 27, 14, 0, 0, 123456789, time.UTC)
	result := FormatTimestampWithOffset(ts)

	// The result contains the UTC time formatted, followed by the local offset.
	// Since we can't easily predict the local offset of the test runner,
	// we verify the prefix which is the UTC part.
	expectedPrefix := "2023-10-27-14-00-00-123456789"
	if !strings.HasPrefix(result, expectedPrefix) {
		t.Errorf("FormatTimestampWithOffset(%v) = %q; want prefix %q", ts, result, expectedPrefix)
	}
}

func TestNormalizedRelPath(t *testing.T) {
	base := filepath.Join("a", "b")
	target := filepath.Join("a", "b", "c", "d")

	got, err := NormalizedRelPath(base, target)
	if err != nil {
		t.Fatalf("NormalizedRelPath failed: %v", err)
	}

	// Should always be forward slashes
	expected := "c/d"
	if got != expected {
		t.Errorf("NormalizedRelPath(%q, %q) = %q; want %q", base, target, got, expected)
	}
}

func TestDenormalizedAbsPath(t *testing.T) {
	base := filepath.Join("a", "b")
	relKey := "c/d"

	got := DenormalizedAbsPath(base, relKey)

	// Should be native separators
	expected := filepath.Join("a", "b", "c", "d")
	if got != expected {
		t.Errorf("DenormalizedAbsPath(%q, %q) = %q; want %q", base, relKey, got, expected)
	}
}

func TestByteCountIEC(t *testing.T) {
	testCases := []struct {
		input    int64
		expected string
	}{
		{0, "0 B"},
		{100, "100 B"},
		{1023, "1023 B"},
		{1024, "1.0 KiB"},
		{1536, "1.5 KiB"},
		{1048576, "1.0 MiB"},
		{1073741824, "1.0 GiB"},
	}

	for _, tc := range testCases {
		got := ByteCountIEC(tc.input)
		if got != tc.expected {
			t.Errorf("ByteCountIEC(%d) = %q; want %q", tc.input, got, tc.expected)
		}
	}
}
