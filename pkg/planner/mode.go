package planner

import (
	"encoding/json"
	"fmt"

	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// Mode represents the operational mode of the engine (incremental or snapshot).
type Mode int

// Constants for Mode, acting as an enum.
const (
	Any Mode = iota
	Incremental
	Snapshot
)

var modeToString = map[Mode]string{
	Any:         "any",
	Incremental: "incremental",
	Snapshot:    "snapshot",
}
var stringToMode = map[string]Mode{}

func init() {
	stringToMode = util.InvertMap(modeToString)
}

// String returns the string representation of a Mode.
func (m Mode) String() string {
	if str, ok := modeToString[m]; ok {
		return str
	}
	return fmt.Sprintf("unknown_engine_mode(%d)", m)
}

// ParseMode parses a string and returns the corresponding Mode.
func ParseMode(s string) (Mode, error) {
	if mode, ok := stringToMode[s]; ok {
		return mode, nil
	}
	return 0, fmt.Errorf("invalid engine mode: %q. Must be 'any','incremental' or 'snapshot'", s)
}

// MarshalJSON implements the json.Marshaler interface for Mode.
func (m Mode) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.String())
}

// UnmarshalJSON implements the json.Unmarshaler interface for Mode.
func (m *Mode) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("Mode should be a string, got %s", data)
	}

	mode, err := ParseMode(s) // Use the helper for parsing
	if err != nil {
		return err
	}
	*m = mode
	return nil
}
