package flagparse

import (
	"fmt"

	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// CommandFlag defines the command to execute.
type Command int

const (
	None = iota
	Backup
	Version
	Init
	Prune
	Restore
)

var commandToString = map[Command]string{
	None:    "none",
	Backup:  "backup",
	Version: "version",
	Init:    "init",
	Prune:   "prune",
	Restore: "restore",
}

var stringToCommand map[string]Command

func init() {
	stringToCommand = util.InvertMap(commandToString)
}

func (c Command) String() string {
	if str, ok := commandToString[c]; ok {
		return str
	}
	return fmt.Sprintf("unknown_command(%d)", c)
}

func ParseCommand(s string) (Command, error) {
	if command, ok := stringToCommand[s]; ok {
		return command, nil
	}
	return None, fmt.Errorf("invalid command: %q. Must be 'backup', 'restore', 'version', 'prune', or 'init'", s)
}
