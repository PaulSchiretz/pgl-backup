package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"

	"pixelgardenlabs.io/pgl-backup/pkg/config"
	"pixelgardenlabs.io/pgl-backup/pkg/engine"
)

// version holds the application's version string.
// It's a `var` so it can be set at compile time using ldflags.
// Example: go build -ldflags="-X main.version=1.0.0"
var version = "dev"

// init is called before main. We use it to set up a custom, more descriptive
// help message for the command-line flags.
func init() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s (version %s):\n", os.Args[0], version)
		fmt.Fprintf(flag.CommandLine.Output(), "A simple and powerful file backup utility with snapshot and incremental modes.\n\n")
		flag.PrintDefaults()
	}
}

// finalizeConfig takes the base configuration (from file or default) and the parsed
// command-line flags, and constructs the final configuration for the backup job.
func finalizeConfig(baseConfig config.Config, src, target, mode, syncEngine *string, quiet, dryrun *bool, nativeEngineWorkers *int) (config.Config, error) {
	runConfig := baseConfig
	runConfig.Paths.Source = *src
	runConfig.Paths.TargetBase = *target

	switch *mode {
	case "snapshot":
		runConfig.Mode = config.SnapshotMode
	case "incremental":
		runConfig.Mode = config.IncrementalMode
	default:
		return config.Config{}, fmt.Errorf("invalid mode: %q. Must be 'incremental' or 'snapshot'", *mode)
	}

	switch *syncEngine {
	case "native":
		runConfig.Engine.Type = config.NativeEngine
	case "robocopy":
		runConfig.Engine.Type = config.RobocopyEngine
	default:
		return config.Config{}, fmt.Errorf("invalid syncEngine: %q. Must be 'native' or 'robocopy'", *syncEngine)
	}

	runConfig.DryRun = *dryrun
	runConfig.Quiet = *quiet
	runConfig.Engine.NativeEngineWorkers = *nativeEngineWorkers

	if runConfig.Engine.NativeEngineWorkers < 1 {
		return config.Config{}, fmt.Errorf("nativeEngineWorkers must be at least 1")
	}

	// Final sanity check: ensure robocopy is disabled if not on Windows.
	if runtime.GOOS != "windows" && runConfig.Engine.Type == config.RobocopyEngine {
		log.Println("Robocopy is not available on this OS. Forcing 'native' sync engine.")
		runConfig.Engine.Type = config.NativeEngine
	}

	return runConfig, nil
}

// run encapsulates the main application logic and returns an error if something
// goes wrong, allowing the main function to handle exit codes.
func run() error {
	loadedConfig, err := config.Load()
	if err != nil {
		log.Printf("Warning: could not parse ppBackup.conf: %v. Using defaults.", err)
		loadedConfig = config.NewDefault()
	}

	srcFlag := flag.String("source", loadedConfig.Paths.Source, "Source directory to copy from")
	targetFlag := flag.String("target", loadedConfig.Paths.TargetBase, "Base destination directory for backups")
	modeFlag := flag.String("mode", loadedConfig.Mode.String(), "Set the backup mode: 'incremental' or 'snapshot'.")
	quietFlag := flag.Bool("quiet", loadedConfig.Quiet, "Suppress individual file operation logs.")
	dryRunFlag := flag.Bool("dryrun", loadedConfig.DryRun, "Show what would be done without making any changes.")
	initFlag := flag.Bool("init", false, "Generate a default ppBackup.conf file and exit.")
	versionFlag := flag.Bool("version", false, "Print the application version and exit.")
	syncEngineFlag := flag.String("syncEngine", loadedConfig.Engine.Type.String(), "Sync engine to use: 'native' or 'robocopy' (Windows only).")
	nativeEngineWorkersFlag := flag.Int("nativeEngineWorkers", loadedConfig.Engine.NativeEngineWorkers, "Number of worker goroutines for native sync.")
	flag.Parse()

	if *versionFlag {
		fmt.Printf("ppBackup version %s\n", version)
		return nil
	}
	if *initFlag {
		return config.Generate()
	}

	runConfig, err := finalizeConfig(loadedConfig, srcFlag, targetFlag, modeFlag, syncEngineFlag, quietFlag, dryRunFlag, nativeEngineWorkersFlag)
	if err != nil {
		return err
	}
	backupEngine := engine.New(runConfig, version)
	return backupEngine.Execute()
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("Error: %v", err)
	}
}
