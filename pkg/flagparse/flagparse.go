package flagparse

import "strings"

// ParseCmdList parses a comma-separated list of shell-like commands.
// It preserves quotes and handles backslash escapes so they can be interpreted by the shell.
func ParseCmdList(s string) []string {
	return parseListInternal(s, true, true)
}

// ParseExcludeList parses a comma-separated list of file or directory patterns.
// It removes quotes, as they are only used for grouping items with spaces.
// It treats backslashes as literal characters for Windows path compatibility.
func ParseExcludeList(s string) []string {
	return parseListInternal(s, false, false)
}

// parseListInternal is the core implementation for parsing a comma-separated list. It supports
// both single (') and double (") quotes to allow items to contain commas or spaces.
// - `keepQuotes`: Preserves quote characters in the output.
// - `handleEscapes`: Treats backslashes as escape characters.
func parseListInternal(s string, keepQuotes, handleEscapes bool) []string {
	var list []string
	var current strings.Builder
	var quoteChar rune

	// Helper to add the current buffered item to the list after trimming whitespace.
	appendItem := func() {
		trimmed := strings.TrimSpace(current.String())
		if trimmed != "" {
			list = append(list, trimmed)
		}
		current.Reset()
	}

	var isEscaped bool
	for _, r := range s {
		if isEscaped {
			current.WriteRune(r)
			isEscaped = false
			continue
		}

		switch {
		case r == '\\' && handleEscapes:
			isEscaped = true
			// For commands, we also keep the backslash for the shell to interpret.
			current.WriteRune(r)
		case r == '\'' || r == '"':
			if quoteChar == 0 { // Start of a new quoted section.
				quoteChar = r
				if keepQuotes {
					current.WriteRune(r)
				}
			} else if quoteChar == r { // End of the current quoted section.
				quoteChar = 0
				if keepQuotes {
					current.WriteRune(r)
				}
			} else { // A different quote character inside an existing quoted section.
				current.WriteRune(r) // Treat it as a literal character.
			}
		case r == ',' && quoteChar == 0: // Comma outside of any quotes.
			appendItem()
		default:
			current.WriteRune(r)
		}
	}
	appendItem() // Add the final item after the loop finishes.
	return list
}
