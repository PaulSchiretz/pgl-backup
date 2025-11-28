package flagparse

import (
	"testing"
)

// equalSlices is a helper to compare two string slices for equality.
func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func TestParseExcludeList(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected []string
	}{
		{"Simple List", "a,b,c", []string{"a", "b", "c"}},
		{"List with Spaces", " a , b, c ", []string{"a", "b", "c"}},
		{"Empty String", "", nil},
		{"Quoted Item with Spaces", "'item with spaces',b", []string{"item with spaces", "b"}},
		{"Quoted Item with Comma", "'a,b',c", []string{"a,b", "c"}},
		{"Mixed Quoted and Unquoted", "a,'b,c',d", []string{"a", "b,c", "d"}},
		{"Unmatched Quote", "'a,b", []string{"a,b"}},
		{"Multiple Quoted Items", "'a b','c d'", []string{"a b", "c d"}},
		{"Double Quoted Item with Spaces", "\"item with spaces\",b", []string{"item with spaces", "b"}},
		{"Nested Quotes", "'a \"b\" c',d", []string{"a \"b\" c", "d"}},
		{"Nested Quotes 2", "\"it's a test\",d", []string{"it's a test", "d"}},
		{"Windows Path with Backslashes", `C:\Users\Test,D:\Data`, []string{`C:\Users\Test`, `D:\Data`}},
		{"Unix Path with Slashes", "/home/user/test,/var/log", []string{"/home/user/test", "/var/log"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := ParseExcludeList(tc.input)

			// Handle the case where an empty input should result in a nil or empty slice.
			if len(tc.expected) == 0 && len(result) == 0 {
				// This is a pass, so we can return early.
				return
			}

			if !equalSlices(result, tc.expected) {
				t.Errorf("expected %v, but got %v", tc.expected, result)
			}
		})
	}
}

func TestParseCmdList(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected []string
	}{
		{"Simple List", "cmd1,cmd2", []string{"cmd1", "cmd2"}},
		{"Quoted Item with Spaces", "'echo hello',cmd2", []string{"'echo hello'", "cmd2"}},
		{"Quoted Item with Comma", "'echo a,b',c", []string{"'echo a,b'", "c"}},
		{"Unmatched Quote", "'a,b", []string{"'a,b"}},
		{"Multiple Quoted Items", "'a b','c d'", []string{"'a b'", "'c d'"}},
		{"Double Quoted Item with Spaces", "\"item with spaces\",b", []string{"\"item with spaces\"", "b"}},
		{"Mixed Single and Double Quotes", "'a b',\"c,d\",e", []string{"'a b'", "\"c,d\"", "e"}},
		{"Nested Quotes", "'a \"b\" c',d", []string{"'a \"b\" c'", "d"}},
		{"Escaped Single Quote Inside Single Quotes", "'hello\\'world',next", []string{"'hello\\'world'", "next"}},
		{"Escaped Double Quote Inside Double Quotes", "\"hello\\\"world\",next", []string{"\"hello\\\"world\"", "next"}},
		{"Escaped Comma Outside Quotes", "a\\,b,c", []string{"a\\,b", "c"}},
		{"Escaped Backslash", "'a\\\\b',c", []string{"'a\\\\b'", "c"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := ParseCmdList(tc.input)

			// Handle the case where an empty input should result in a nil or empty slice.
			if len(tc.expected) == 0 && len(result) == 0 {
				// This is a pass, so we can return early.
				return
			}

			if !equalSlices(result, tc.expected) {
				t.Errorf("expected %v, but got %v", tc.expected, result)
			}
		})
	}
}
