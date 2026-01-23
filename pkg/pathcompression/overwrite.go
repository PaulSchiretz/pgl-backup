package pathcompression

import (
	"encoding/json"
	"fmt"

	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// OverwriteBehavior defines how to handle existing files during extraction.
type OverwriteBehavior string

const (
	// OverwriteAlways will always overwrite an existing file. This is the default.
	OverwriteAlways OverwriteBehavior = "always"
	// OverwriteNever will never overwrite an existing file.
	OverwriteNever OverwriteBehavior = "never"
	// OverwriteIfNewer will only overwrite if the file in the archive is newer.
	OverwriteIfNewer OverwriteBehavior = "if-newer"
)

var behaviorToString = map[OverwriteBehavior]string{
	OverwriteAlways:  "always",
	OverwriteNever:   "never",
	OverwriteIfNewer: "if-newer",
}

var stringToBehavior map[string]OverwriteBehavior

func init() {
	// Inverting the map at runtime ensures behaviorToString is fully loaded
	stringToBehavior = util.InvertMap(behaviorToString)
}

func (ob OverwriteBehavior) String() string {
	if str, ok := behaviorToString[ob]; ok {
		return str
	}
	return fmt.Sprintf("unknown_overwrite_behavior(%s)", string(ob))
}

func ParseOverwriteBehavior(s string) (OverwriteBehavior, error) {
	if behavior, ok := stringToBehavior[s]; ok {
		return behavior, nil
	}
	return "", fmt.Errorf("invalid overwrite behavior: %q. Must be 'always', 'never', or 'if-newer'", s)
}

// MarshalJSON implements the json.Marshaler interface for OverwriteBehavior.
func (ob OverwriteBehavior) MarshalJSON() ([]byte, error) {
	return json.Marshal(ob.String())
}

// UnmarshalJSON implements the json.Unmarshaler interface for OverwriteBehavior.
func (ob *OverwriteBehavior) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("overwrite behavior should be a string, got %s", data)
	}
	behavior, err := ParseOverwriteBehavior(s)
	if err != nil {
		return err
	}
	*ob = behavior
	return nil
}
