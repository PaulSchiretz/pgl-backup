package pathsync

import (
	"path/filepath"
	"strings"

	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

type exclusionMatchType int

const (
	literalMatch exclusionMatchType = iota
	prefixMatch
	suffixMatch
	globMatch
)

// exclusionSet holds the categorized exclusion patterns for efficient matching.
type exclusionSet struct {
	// literals are for exact full-path matches, which are the fastest to check.
	literals map[string]struct{}
	// basenameLiterals are for exact basename matches (e.g., "node_modules"), also very fast.
	basenameLiterals map[string]struct{}
	// nonLiterals are for patterns requiring more complex logic (wildcards, basename matches).
	nonLiterals []exclusion
}

// exclusion stores the pre-analyzed pattern details.
type exclusion struct {
	pattern       string             // The original pattern for logging/debugging.
	cleanPattern  string             // The pattern without wildcards for prefix/suffix matching, or the full pattern for glob/literal.
	matchType     exclusionMatchType // The type of match to perform (prefix, suffix, glob, literal).
	matchBasename bool               // If true, the match is against the path's basename; otherwise, the full relative path.
}

// makeExclusionSet analyzes and categorizes patterns to enable optimized matching later.
func makeExclusionSet(patterns []string) exclusionSet {
	set := exclusionSet{
		literals:         make(map[string]struct{}),
		basenameLiterals: make(map[string]struct{}),
		nonLiterals:      make([]exclusion, 0, len(patterns)),
	}

	// A pattern should match against the basename if it does NOT contain a path separator.
	// This aligns with .gitignore behavior (e.g., "node_modules" matches anywhere).
	shouldMatchBasename := func(p string) bool { return !strings.Contains(p, "/") }

	for _, p := range patterns {
		// Normalize to a consistent, case-insensitive key.
		p = normalizeExclusionPattern(p)
		if strings.ContainsAny(p, "*?[]") {
			// If it's a prefix pattern like `node_modules/*`, we can optimize it.
			if strings.HasSuffix(p, "/*") {
				set.nonLiterals = append(set.nonLiterals, exclusion{
					pattern:       p,
					cleanPattern:  strings.TrimSuffix(p, "/*"), // e.g., "build"
					matchType:     prefixMatch,
					matchBasename: false, // This is a full-path prefix.
				})
			} else if strings.HasSuffix(p, "*") && !strings.ContainsAny(p[:len(p)-1], "*?[]") {
				// A pattern like `~*` or `temp_*`.
				set.nonLiterals = append(set.nonLiterals, exclusion{
					pattern:       p,
					cleanPattern:  strings.TrimSuffix(p, "*"), // e.g., "~"
					matchType:     prefixMatch,
					matchBasename: shouldMatchBasename(p),
				})
			} else if strings.HasPrefix(p, "*") && !strings.ContainsAny(p[1:], "*?[]") {
				// A pattern like `*.log` or `*.tmp`.
				set.nonLiterals = append(set.nonLiterals, exclusion{
					pattern:       p,
					cleanPattern:  p[1:], // e.g., ".log"
					matchType:     suffixMatch,
					matchBasename: shouldMatchBasename(p),
				})
			} else {
				// Otherwise, it's a general glob pattern.
				set.nonLiterals = append(set.nonLiterals, exclusion{
					pattern: p, cleanPattern: p, matchType: globMatch, matchBasename: shouldMatchBasename(p),
				})
			}
		} else {
			// No wildcards.
			if strings.HasSuffix(p, "/") {
				// A pattern like `build/` is explicitly a full-path prefix match.
				set.nonLiterals = append(set.nonLiterals, exclusion{
					pattern:       p,
					cleanPattern:  strings.TrimSuffix(p, "/"),
					matchType:     prefixMatch,
					matchBasename: false,
				})
			} else {
				// A pattern like "node_modules" or "docs/config.json".
				// If it contains a path separator, it's a full-path literal match.
				// If not, it's a basename literal match.
				isBasenameMatch := shouldMatchBasename(p)
				if isBasenameMatch { // e.g., "node_modules"
					set.basenameLiterals[p] = struct{}{}
				} else { // e.g., "docs/config.json"
					set.literals[p] = struct{}{}
				}
			}
		}
	}
	return set
}

// matches checks if a given relative path key matches any of the exclusion patterns.
func (es *exclusionSet) matches(relPathKey, relPathBasename string) bool {
	// Normalize paths to the same case-insensitive format as the patterns.
	normalizedPath := normalizeExclusionPattern(relPathKey)
	normalizedBasename := normalizeExclusionPattern(relPathBasename)

	// 1. Check for O(1) full-path literal matches.
	if _, ok := es.literals[normalizedPath]; ok {
		return true
	}

	// 2. Check for O(1) basename literal matches if the map is not empty.
	if _, ok := es.basenameLiterals[normalizedBasename]; ok {
		return true
	}

	// 3. If no literal match, check other pattern types (wildcards).
	for _, p := range es.nonLiterals {
		pathToCheck := normalizedPath
		if p.matchBasename {
			pathToCheck = normalizedBasename
		}

		switch p.matchType {
		case prefixMatch:
			if strings.HasPrefix(pathToCheck, p.cleanPattern) {
				// For full-path directory prefixes ("build/"), we must avoid false positives on "build-tools".
				// This check is only relevant for full-path matches.
				if !p.matchBasename && strings.HasSuffix(p.pattern, "/") {
					if pathToCheck != p.cleanPattern && !strings.HasPrefix(pathToCheck, p.cleanPattern+"/") {
						continue // Not a true directory prefix match.
					}
				}
				return true
			}
		case suffixMatch:
			if strings.HasSuffix(pathToCheck, p.cleanPattern) {
				return true
			}

		case globMatch:
			match, err := filepath.Match(p.cleanPattern, pathToCheck)
			if err != nil {
				// Log the error for the invalid pattern but continue checking others.
				plog.Warn("Invalid exclusion pattern", "pattern", p.cleanPattern, "error", err)
				continue
			}
			if match {
				return true
			}
		}
	}
	return false
}

// normalizeExclusionPattern converts a path or pattern into a standardized,
// case-insensitive key format (forward slashes, lowercase).
func normalizeExclusionPattern(p string) string {
	return strings.ToLower(filepath.ToSlash(p))
}
