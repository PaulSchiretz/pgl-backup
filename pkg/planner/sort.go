package planner

import (
	"encoding/json"
	"fmt"

	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// SortOrder represents the sorting order for lists.
type SortOrder int

const (
	Desc SortOrder = iota // Newest first
	Asc                   // Oldest first
)

var sortOrderToString = map[SortOrder]string{
	Desc: "desc",
	Asc:  "asc",
}

var stringToSortOrder = map[string]SortOrder{}

func init() {
	stringToSortOrder = util.InvertMap(sortOrderToString)
}

// String returns the string representation of a SortOrder.
func (s SortOrder) String() string {
	if str, ok := sortOrderToString[s]; ok {
		return str
	}
	return fmt.Sprintf("unknown_sort_order(%d)", s)
}

// ParseSortOrder parses a string and returns the corresponding SortOrder.
func ParseSortOrder(s string) (SortOrder, error) {
	if order, ok := stringToSortOrder[s]; ok {
		return order, nil
	}
	return 0, fmt.Errorf("invalid sort order: %q. Must be 'desc' or 'asc'", s)
}

// MarshalJSON implements the json.Marshaler interface for SortOrder.
func (s SortOrder) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

// UnmarshalJSON implements the json.Unmarshaler interface for SortOrder.
func (s *SortOrder) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return fmt.Errorf("SortOrder should be a string, got %s", data)
	}
	order, err := ParseSortOrder(str)
	if err != nil {
		return err
	}
	*s = order
	return nil
}
