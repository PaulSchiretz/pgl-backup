package pathsync

import (
	"encoding/json"
	"fmt"

	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// Engine represents the file synchronization engine to use.
type Engine int

const (
	// Native uses the cross-platform Go implementation.
	Native Engine = iota
	// Robocopy uses the Windows-specific robocopy utility.
	Robocopy
)

var engineToString = map[Engine]string{Native: "native", Robocopy: "robocopy"}
var stringToEngine = map[string]Engine{}

func init() {
	stringToEngine = util.InvertMap(engineToString)
}

// String returns the string representation of a Engine.
func (e Engine) String() string {
	if str, ok := engineToString[e]; ok {
		return str
	}
	return fmt.Sprintf("unknown_engine(%d)", e)
}

// ParseEngine parses a string and returns the corresponding Engine.
func ParseEngine(s string) (Engine, error) {
	if engine, ok := stringToEngine[s]; ok {
		return engine, nil
	}
	return 0, fmt.Errorf("invalid engine: %q. Must be 'native' or 'robocopy'", s)
}

// MarshalJSON implements the json.Marshaler interface for Engine.
func (e Engine) MarshalJSON() ([]byte, error) {
	return json.Marshal(e.String())
}

// UnmarshalJSON implements the json.Unmarshaler interface for Engine.
func (e *Engine) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("Engine should be a string, got %s", data)
	}

	engine, err := ParseEngine(s) // Use the helper for parsing
	if err != nil {
		return err
	}
	*e = engine
	return nil
}
