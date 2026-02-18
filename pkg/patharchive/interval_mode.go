package patharchive

import (
	"encoding/json"
	"fmt"

	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// IntervalMode represents how the archive interval is determined.
type IntervalMode int

const (
	// ManualInterval uses the user-specified interval value directly.
	Manual IntervalMode = iota // 0
	// AutoInterval calculates the interval based on the finest-grained retention policy.
	Auto // 1
)

var intervalModeToString = map[IntervalMode]string{Manual: "manual", Auto: "auto"}
var stringToIntervalMode map[string]IntervalMode

func init() {
	stringToIntervalMode = util.InvertMap(intervalModeToString)
}

// String returns the string representation of a IntervalMode.
func (im IntervalMode) String() string {
	if str, ok := intervalModeToString[im]; ok {
		return str
	}
	return fmt.Sprintf("unknown_interval_mode(%d)", im)
}

// ParseIntervalMode parses a string and returns the corresponding IntervalMode.
func ParseIntervalMode(s string) (IntervalMode, error) {
	if mode, ok := stringToIntervalMode[s]; ok {
		return mode, nil
	}
	return 0, fmt.Errorf("invalid IntervalMode: %q. Must be 'manual' or 'auto'", s)
}

// MarshalJSON implements the json.Marshaler interface for IntervalMode.
func (im IntervalMode) MarshalJSON() ([]byte, error) {
	return json.Marshal(im.String())
}

// UnmarshalJSON implements the json.Unmarshaler interface for IntervalMode.
func (im *IntervalMode) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("IntervalMode should be a string, got %s", data)
	}
	mode, err := ParseIntervalMode(s)
	if err != nil {
		return err
	}
	*im = mode
	return nil
}
