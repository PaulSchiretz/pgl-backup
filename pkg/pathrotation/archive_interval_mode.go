package pathrotation

import (
	"encoding/json"
	"fmt"

	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// ArchiveIntervalMode represents how the archive interval is determined.
type ArchiveIntervalMode int

const (
	// ManualInterval uses the user-specified interval value directly.
	Manual ArchiveIntervalMode = iota // 0
	// AutoInterval calculates the interval based on the finest-grained retention policy.
	Auto // 1
)

var intervalModeToString = map[ArchiveIntervalMode]string{Manual: "manual", Auto: "auto"}
var stringToIntervalMode map[string]ArchiveIntervalMode

func init() {
	stringToIntervalMode = util.InvertMap(intervalModeToString)
}

// String returns the string representation of a ArchiveIntervalMode.
func (im ArchiveIntervalMode) String() string {
	if str, ok := intervalModeToString[im]; ok {
		return str
	}
	return fmt.Sprintf("unknown_interval_mode(%d)", im)
}

// ParseArchiveIntervalMode parses a string and returns the corresponding ArchiveIntervalMode.
func ParseArchiveIntervalMode(s string) (ArchiveIntervalMode, error) {
	if mode, ok := stringToIntervalMode[s]; ok {
		return mode, nil
	}
	return 0, fmt.Errorf("invalid ArchiveIntervalMode: %q. Must be 'manual' or 'auto'", s)
}

// MarshalJSON implements the json.Marshaler interface for ArchiveIntervalMode.
func (im ArchiveIntervalMode) MarshalJSON() ([]byte, error) {
	return json.Marshal(im.String())
}

// UnmarshalJSON implements the json.Unmarshaler interface for ArchiveIntervalMode.
func (im *ArchiveIntervalMode) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("ArchiveIntervalMode should be a string, got %s", data)
	}
	mode, err := ParseArchiveIntervalMode(s)
	if err != nil {
		return err
	}
	*im = mode
	return nil
}
