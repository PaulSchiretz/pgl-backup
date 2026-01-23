package pathcompression

import (
	"encoding/json"
	"fmt"

	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// Level represents the desired trade-off between speed and size of the compression.
type Level string

const (
	Default Level = "default"
	Fastest Level = "fastest"
	Better  Level = "better"
	Best    Level = "best"
)

var levelToString = map[Level]string{
	Default: "default",
	Fastest: "fastest",
	Better:  "better",
	Best:    "best",
}

var stringToLevel map[string]Level

func init() {
	stringToLevel = util.InvertMap(levelToString)
}

func (l Level) String() string {
	if str, ok := levelToString[l]; ok {
		return str
	}
	return string(Default)
}

// ParseLevel parses a string into a compression Level.
// It defaults to default level if the string is empty.
func ParseLevel(s string) (Level, error) {
	if s == "" {
		return Default, nil
	}
	if l, ok := stringToLevel[s]; ok {
		return l, nil
	}
	return "", fmt.Errorf("invalid compression level: %q. Must be 'default', 'fastest', 'better', or 'best'", s)
}

// MarshalJSON implements the json.Marshaler interface.
func (l Level) MarshalJSON() ([]byte, error) {
	return json.Marshal(l.String())
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (l *Level) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("compression level should be a string, got %s", data)
	}
	level, err := ParseLevel(s)
	if err != nil {
		return err
	}
	*l = level
	return nil
}
