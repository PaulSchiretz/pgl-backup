package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"

	"github.com/paulschiretz/pgl-backup/cmd"
	"github.com/paulschiretz/pgl-backup/pkg/buildinfo"
	"github.com/paulschiretz/pgl-backup/pkg/flagparse"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

// run encapsulates the main application logic and returns an error if something
// goes wrong, allowing the main function to handle exit codes.
func run(ctx context.Context) error {
	plog.Info("Starting "+buildinfo.Name, "version", buildinfo.Version, "pid", os.Getpid())

	command, flagMap, err := flagparse.Parse(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	switch command {
	case flagparse.None:
		return nil
	case flagparse.Version:
		return cmd.RunVersion()
	case flagparse.Init:
		return cmd.RunInit(ctx, flagMap)
	case flagparse.Backup:
		return cmd.RunBackup(ctx, flagMap)
	case flagparse.List:
		return cmd.RunList(ctx, flagMap)
	case flagparse.Restore:
		return cmd.RunRestore(ctx, flagMap)
	case flagparse.Prune:
		return cmd.RunPrune(ctx, flagMap)
	default:
		return fmt.Errorf("internal error: unknown command %d", command)
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
		plog.Error(buildinfo.Name+" exited with error", "error", err)
		os.Exit(1)
	}
}
