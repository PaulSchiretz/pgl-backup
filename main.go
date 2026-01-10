package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"

	"github.com/paulschiretz/pgl-backup/cmd"
	"github.com/paulschiretz/pgl-backup/pkg/flagparse"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

// appName is the canonical name of the application used for logging.
const appName = "PGL-Backup"

// appVersion holds the application's version string.
// It's a `var` so it can be set at compile time using ldflags.
// Example: go build -ldflags="-X main.appVersion=1.0.0"
var appVersion = "dev"

// run encapsulates the main application logic and returns an error if something
// goes wrong, allowing the main function to handle exit codes.
func run(ctx context.Context) error {
	plog.Info("Starting "+appName, "version", appVersion, "pid", os.Getpid())

	appCommand, flagMap, err := flagparse.Parse(appName, appVersion, os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	switch appCommand {
	case flagparse.NoCommand:
		return nil
	case flagparse.VersionCommand:
		return cmd.RunVersion(appName, appVersion)
	case flagparse.InitCommand:
		return cmd.RunInit(ctx, flagMap, appName, appVersion)
	case flagparse.BackupCommand:
		return cmd.RunBackup(ctx, flagMap, appName, appVersion)
	case flagparse.PruneCommand:
		return cmd.RunPrune(ctx, flagMap, appName, appVersion)
	default:
		return fmt.Errorf("internal error: unknown command %d", appCommand)
	}
}

func main() {
	// Set up a context that is canceled when an interrupt signal is received.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Listen for interrupt signals (like Ctrl+C) in a separate goroutine.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	go func() {
		<-sigChan
		cancel()
	}()

	if err := run(ctx); err != nil {
		plog.Error(appName+" exited with error", "error", err)
		os.Exit(1)
	}
}
