package cmd

import "fmt"

// RunVersion prints the application version.
func RunVersion(appName, appVersion string) error {
	fmt.Printf("%s version %s\n", appName, appVersion)
	return nil
}
