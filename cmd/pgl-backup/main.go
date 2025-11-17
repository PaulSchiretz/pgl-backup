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

// flagValues holds the values parsed from the command-line flags.
type flagValues struct {
	source              string
	target              string
	mode                string
	syncEngine          string
	quiet               bool
	dryRun              bool
	nativeEngineWorkers int
}

// finalizeConfig merges the base configuration with the command-line flag values
// to construct the final configuration for the backup job.
func finalizeConfig(baseConfig config.Config, flags flagValues) (config.Config, error) {
	runConfig := baseConfig
	runConfig.Paths.Source = flags.source
	runConfig.Paths.TargetBase = flags.target

	mode, err := config.ModeFromString(flags.mode)
	if err != nil {
		return config.Config{}, err
	}
	runConfig.Mode = mode

	engineType, err := config.EngineTypeFromString(flags.syncEngine)
	if err != nil {
		return config.Config{}, err
	}
	runConfig.Engine.Type = engineType

	runConfig.DryRun = flags.dryRun
	runConfig.Quiet = flags.quiet
	runConfig.Engine.NativeEngineWorkers = flags.nativeEngineWorkers

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
		// Only show a warning if the file exists but is invalid. Not existing is fine.
		if !os.IsNotExist(err) {
			log.Printf("Warning: could not load ppBackup.conf: %v. Using defaults.", err)
		}
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

	flags := flagValues{
		source:              *srcFlag,
		target:              *targetFlag,
		mode:                *modeFlag,
		syncEngine:          *syncEngineFlag,
		quiet:               *quietFlag,
		dryRun:              *dryRunFlag,
		nativeEngineWorkers: *nativeEngineWorkersFlag,
	}
	runConfig, err := finalizeConfig(loadedConfig, flags)
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
