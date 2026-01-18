package pathcompression

import (
	"encoding/json"
	"fmt"

	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// Format represents the archive format for compression.
type Format string

const (
	Zip    Format = "zip"
	TarGz  Format = "tar.gz"
	TarZst Format = "tar.zst"
)

var formatToString = map[Format]string{
	Zip:    "zip",
	TarGz:  "tar.gz",
	TarZst: "tar.zst",
}

var stringToFormat map[string]Format

func init() {
	// Inverting the map at runtime ensures formatToString is fully loaded
	stringToFormat = util.InvertMap(formatToString)
}

func (f Format) String() string {
	if str, ok := formatToString[f]; ok {
		return str
	}
	return fmt.Sprintf("unknown_compression_format(%s)", string(f))
}

func ParseFormat(s string) (Format, error) {
	if format, ok := stringToFormat[s]; ok {
		return format, nil
	}
	return "", fmt.Errorf("invalid compression format: %q. Must be 'zip', 'tar.gz', or 'tar.zst'", s)
}

// MarshalJSON implements the json.Marshaler interface for Format.
func (cf Format) MarshalJSON() ([]byte, error) {
	return json.Marshal(cf.String())
}

// UnmarshalJSON implements the json.Unmarshaler interface for Format.
func (cf *Format) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("compression format should be a string, got %s", data)
	}
	format, err := ParseFormat(s)
	if err != nil {
		return err
	}
	*cf = format
	return nil
}
