package cmd

import (
	"fmt"

	"github.com/paulschiretz/pgl-backup/pkg/buildinfo"
)

// RunVersion prints the application version.
func RunVersion() error {
	fmt.Printf("%s version %s\n", buildinfo.Name, buildinfo.Version)
	return nil
}
