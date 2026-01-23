package pathsync

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// OverwriteBehavior defines how to handle existing files during sync.
type OverwriteBehavior string

const (
	// OverwriteAlways will always overwrite an existing file.
	OverwriteAlways OverwriteBehavior = "always"
	// OverwriteNever will never overwrite an existing file.
	OverwriteNever OverwriteBehavior = "never"
	// OverwriteIfNewer will only overwrite if the source file is newer than the destination.
	OverwriteIfNewer OverwriteBehavior = "if-newer"
	// OverwriteUpdate will overwrite if the file is different (size/modtime).
	// This is the default for Sync/Backup.
	OverwriteUpdate OverwriteBehavior = "update"
)

var behaviorToString = map[OverwriteBehavior]string{
	OverwriteAlways:  "always",
	OverwriteNever:   "never",
	OverwriteIfNewer: "if-newer",
	OverwriteUpdate:  "update",
}

var stringToBehavior map[string]OverwriteBehavior

func init() {
	stringToBehavior = util.InvertMap(behaviorToString)
}

func (ob OverwriteBehavior) String() string {
	if str, ok := behaviorToString[ob]; ok {
		return str
	}
	return fmt.Sprintf("unknown_overwrite_behavior(%s)", string(ob))
}

func ParseOverwriteBehavior(s string) (OverwriteBehavior, error) {
	if behavior, ok := stringToBehavior[strings.ToLower(s)]; ok {
		return behavior, nil
	}
	return "", fmt.Errorf("invalid sync overwrite behavior: %q. Must be 'always', 'never', 'if-newer', or 'update'", s)
}

// MarshalJSON implements the json.Marshaler interface.
func (ob OverwriteBehavior) MarshalJSON() ([]byte, error) {
	return json.Marshal(ob.String())
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (ob *OverwriteBehavior) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("OverwriteBehavior should be a string, got %s", data)
	}
	parsed, err := ParseOverwriteBehavior(s)
	if err != nil {
		return err
	}
	*ob = parsed
	return nil
}
