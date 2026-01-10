package flagparse

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/paulschiretz/pgl-backup/pkg/config"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

// CommandFlag defines a special command to execute instead of a backup.
type CommandFlag int

const (
	NoCommand CommandFlag = iota
	BackupCommand
	VersionCommand
	InitCommand
)

// SharedFlags holds pointers to flag values that are common across multiple commands.
// This prevents code duplication when defining flags for 'init', 'backup', etc.
type SharedFlags struct {
	Source                 *string
	Target                 *string
	Mode                   *string
	LogLevel               *string
	FailFast               *bool
	DryRun                 *bool
	Metrics                *bool
	Force                  *bool
	SyncEngine             *string
	SyncWorkers            *int
	MirrorWorkers          *int
	DeleteWorkers          *int
	CompressWorkers        *int
	RetryCount             *int
	RetryWait              *int
	BufferSizeKB           *int
	ModTimeWindow          *int
	UserExcludeFiles       *string
	UserExcludeDirs        *string
	PreserveSourceName     *bool
	PreBackupHooks         *string
	PostBackupHooks        *string
	ArchiveIntervalSeconds *int
	ArchiveIntervalMode    *string
	CompressionEnabled     *bool
	CompressionFormat      *string
}

// registerSharedFlags defines the common flags on the provided FlagSet and returns
// a struct containing pointers to the values.
func registerSharedFlags(fs *flag.FlagSet) *SharedFlags {
	f := &SharedFlags{}
	f.Source = fs.String("source", "", "Source directory to copy from")
	f.Target = fs.String("target", "", "Base destination directory for backups")
	f.Mode = fs.String("mode", "incremental", "Backup mode: 'incremental' or 'snapshot'.")
	f.LogLevel = fs.String("log-level", "info", "Set the logging level: 'debug', 'notice', 'info', 'warn', 'error'.")
	f.FailFast = fs.Bool("fail-fast", false, "Stop the backup immediately on the first file sync error.")
	f.DryRun = fs.Bool("dry-run", false, "Show what would be done without making any changes.")
	f.Metrics = fs.Bool("metrics", false, "Enable detailed performance and file-counting metrics.")
	f.Force = fs.Bool("force", false, "Bypass confirmation prompts.")
	f.SyncEngine = fs.String("sync-engine", "native", "Sync engine to use: 'native' or 'robocopy' (Windows only).")
	f.SyncWorkers = fs.Int("sync-workers", 0, "Number of worker goroutines for file synchronization.")
	f.MirrorWorkers = fs.Int("mirror-workers", 0, "Number of worker goroutines for file deletions in mirror mode.")
	f.DeleteWorkers = fs.Int("delete-workers", 0, "Number of worker goroutines for deleting outdated backups.")
	f.CompressWorkers = fs.Int("compress-workers", 0, "Number of worker goroutines for compressing backups.")
	f.RetryCount = fs.Int("retry-count", 0, "Number of retries for failed file copies.")
	f.RetryWait = fs.Int("retry-wait", 0, "Seconds to wait between retries.")
	f.BufferSizeKB = fs.Int("buffer-size-kb", 0, "Size of the I/O buffer in kilobytes for file copies and compression.")
	f.ModTimeWindow = fs.Int("mod-time-window", 1, "Time window in seconds to consider file modification times equal (0=exact).")
	f.UserExcludeFiles = fs.String("user-exclude-files", "", "Comma-separated list of case-insensitive file names to exclude (supports glob patterns).")
	f.UserExcludeDirs = fs.String("user-exclude-dirs", "", "Comma-separated list of case-insensitive directory names to exclude (supports glob patterns).")
	f.PreserveSourceName = fs.Bool("preserve-source-name", true, "Preserve the source directory's name in the destination path. Set to false to sync contents directly.")
	f.PreBackupHooks = fs.String("pre-backup-hooks", "", "Comma-separated list of commands to run before the backup.")
	f.PostBackupHooks = fs.String("post-backup-hooks", "", "Comma-separated list of commands to run after the backup.")
	f.ArchiveIntervalSeconds = fs.Int("archive-interval-seconds", 0, "In 'manual' mode, the interval in seconds for creating new archives (e.g., 86400 for 24h).")
	f.ArchiveIntervalMode = fs.String("archive-interval-mode", "", "Archive interval mode: 'auto' or 'manual'.")
	f.CompressionEnabled = fs.Bool("compression", true, "Enable compression for backups.")
	f.CompressionFormat = fs.String("compression-format", "", "Compression format for backups: 'zip', 'tar.gz', or 'tar.zst'.")
	return f
}

// Parse parses the provided arguments (usually os.Args[1:]) and returns the action and config map.
func Parse(appName, appVersion string, args []string) (CommandFlag, map[string]interface{}, error) {
	// Handle top-level help
	// If no arguments provided, print help and exit.
	if len(args) == 0 {
		fs := flag.NewFlagSet(appName, flag.ContinueOnError)
		printTopLevelUsage(appName, appVersion, fs)
		return NoCommand, nil, nil
	}

	cmd := strings.ToLower(args[0])

	if cmd == "help" || cmd == "-h" || cmd == "-help" || cmd == "--help" {
		fs := flag.NewFlagSet(appName, flag.ContinueOnError)
		printTopLevelUsage(appName, appVersion, fs)
		return NoCommand, nil, nil
	}

	// Check for subcommand
	switch cmd {
	case "init":
		fs := flag.NewFlagSet("init", flag.ContinueOnError)
		shared := registerSharedFlags(fs)

		// Specific flag for init to handle the old -init-default behavior
		defaultFlag := fs.Bool("default", false, "Overwrite existing configuration with defaults.")

		// Custom usage for the subcommand
		fs.Usage = func() {
			printSubcommandUsage(appName, appVersion, "init", "Initialize a new backup target directory.", fs)
		}

		if err := fs.Parse(args[1:]); err != nil {
			return InitCommand, nil, err
		}

		flagMap, err := flagsToMap(fs, shared)
		if err != nil {
			return InitCommand, nil, err
		}

		if *defaultFlag {
			flagMap["default"] = true
		}
		return InitCommand, flagMap, nil

	case "backup":
		fs := flag.NewFlagSet("backup", flag.ContinueOnError)
		shared := registerSharedFlags(fs)

		fs.Usage = func() {
			printSubcommandUsage(appName, appVersion, "backup", "Run the backup operation.", fs)
		}

		if err := fs.Parse(args[1:]); err != nil {
			return BackupCommand, nil, err
		}
		flagMap, err := flagsToMap(fs, shared)
		return BackupCommand, flagMap, err

	case "version":
		return VersionCommand, nil, nil

	default:
		return BackupCommand, nil, fmt.Errorf("unknown command: %s", args[0])
	}
}

// flagsToMap converts the parsed flags in the FlagSet to a map, using the SharedFlags struct values.
func flagsToMap(fs *flag.FlagSet, sf *SharedFlags) (map[string]interface{}, error) {
	// Create a map of the flags that were explicitly set by the user, along with their values.
	// This map is used to selectively override the base configuration.
	usedFlags := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) { usedFlags[f.Name] = true })

	flagMap := make(map[string]interface{})

	// Helper to add a value to the map only if the corresponding flag was set.
	addIfUsed := func(name string, value interface{}) {
		if usedFlags[name] {
			flagMap[name] = value
		}
	}

	// Helper for flags that need parsing. It only calls the parser if the flag was used.
	addParsedIfUsed := func(name string, rawValue string, parser func(string) []string) {
		if usedFlags[name] {
			flagMap[name] = parser(rawValue)
		}
	}

	// Populate the map with action flags using the helper.
	addIfUsed("source", *sf.Source)
	addIfUsed("target", *sf.Target)
	addIfUsed("log-level", *sf.LogLevel)
	addIfUsed("fail-fast", *sf.FailFast)
	addIfUsed("dry-run", *sf.DryRun)
	addIfUsed("metrics", *sf.Metrics)
	addIfUsed("force", *sf.Force)
	addIfUsed("preserve-source-name", *sf.PreserveSourceName)
	addIfUsed("sync-workers", *sf.SyncWorkers)
	addIfUsed("mirror-workers", *sf.MirrorWorkers)
	addIfUsed("delete-workers", *sf.DeleteWorkers)
	addIfUsed("compress-workers", *sf.CompressWorkers)
	addIfUsed("retry-count", *sf.RetryCount)
	addIfUsed("retry-wait", *sf.RetryWait)
	addIfUsed("buffer-size-kb", *sf.BufferSizeKB)
	addIfUsed("mod-time-window", *sf.ModTimeWindow)
	addIfUsed("archive-interval-seconds", *sf.ArchiveIntervalSeconds)
	addIfUsed("compression", *sf.CompressionEnabled)

	// Handle flags that require parsing/validation.
	addParsedIfUsed("user-exclude-files", *sf.UserExcludeFiles, ParseExcludeList)
	addParsedIfUsed("user-exclude-dirs", *sf.UserExcludeDirs, ParseExcludeList)
	addParsedIfUsed("pre-backup-hooks", *sf.PreBackupHooks, ParseCmdList)
	addParsedIfUsed("post-backup-hooks", *sf.PostBackupHooks, ParseCmdList)

	if usedFlags["mode"] {
		mode, err := config.BackupModeFromString(*sf.Mode)
		if err != nil {
			return nil, err
		}
		flagMap["mode"] = mode
	}
	if usedFlags["sync-engine"] {
		engineType, err := config.SyncEngineFromString(*sf.SyncEngine)
		if err != nil {
			return nil, err
		}
		flagMap["sync-engine"] = engineType
	}
	if usedFlags["archive-interval-mode"] {
		mode, err := config.ArchiveIntervalModeFromString(*sf.ArchiveIntervalMode)
		if err != nil {
			return nil, err
		}
		flagMap["archive-interval-mode"] = mode
	}
	if usedFlags["compression-format"] {
		format, err := config.CompressionFormatFromString(*sf.CompressionFormat)
		if err != nil {
			return nil, err
		}
		flagMap["compression-format"] = format
	}

	// Final sanity check: if robocopy was requested on a non-windows OS, force native.
	if runtime.GOOS != "windows" {
		if val, ok := flagMap["sync-engine"]; ok && val.(config.SyncEngine) == config.RobocopyEngine {
			plog.Warn("Robocopy is not available on this OS. Forcing 'native' sync engine.")
			flagMap["sync-engine"] = config.NativeEngine
		}
	}

	return flagMap, nil
}

// printTopLevelUsage prints the main help message.
func printTopLevelUsage(appName, appVersion string, fs *flag.FlagSet) {

	execName := filepath.Base(os.Args[0])
	fmt.Fprintf(fs.Output(), "%s(%s) ", appName, appVersion)
	fmt.Fprintf(fs.Output(), "A simple and powerful cross-platform backup utility.\n\n")
	fmt.Fprintf(fs.Output(), "Usage: %s <command> [flags]\n\n", execName)
	fmt.Fprintf(fs.Output(), "Commands:\n")
	fmt.Fprintf(fs.Output(), "  backup      Run the backup operation\n")
	fmt.Fprintf(fs.Output(), "  init        Initialize a new configuration\n")
	fmt.Fprintf(fs.Output(), "  version     Print the application version\n")
	fmt.Fprintf(fs.Output(), "\nRun '%s <command> -help' for more information on a command.\n", execName)
}

// printSubcommandUsage prints the help message for a specific subcommand.
func printSubcommandUsage(appName, appVersion, command, desc string, fs *flag.FlagSet) {

	execName := filepath.Base(os.Args[0])
	fmt.Fprintf(fs.Output(), "%s(%s) ", appName, appVersion)
	fmt.Fprintf(fs.Output(), "A simple and powerful cross-platform backup utility.\n\n")
	fmt.Fprintf(fs.Output(), "Usage of the %s command: %s %s [flags]\n\n", command, execName, command)
	fmt.Fprintf(fs.Output(), "%s\n\n", desc)
	fmt.Fprintf(fs.Output(), "Flags:\n")
	fs.PrintDefaults()
}

// ParseCmdList parses a comma-separated list of shell-like commands.
// It preserves quotes and handles backslash escapes so they can be interpreted by the shell.
func ParseCmdList(s string) []string {
	return parseListInternal(s, true, true)
}

// ParseExcludeList parses a comma-separated list of file or directory patterns.
// It removes quotes, as they are only used for grouping items with spaces.
// It treats backslashes as literal characters for Windows path compatibility.
func ParseExcludeList(s string) []string {
	return parseListInternal(s, false, false)
}

// parseListInternal is the core implementation for parsing a comma-separated list. It supports
// both single (') and double (") quotes to allow items to contain commas or spaces.
// - `keepQuotes`: Preserves quote characters in the output.
// - `handleEscapes`: Treats backslashes as escape characters.
func parseListInternal(s string, keepQuotes, handleEscapes bool) []string {
	var list []string
	var current strings.Builder
	var quoteChar rune

	// Helper to add the current buffered item to the list after trimming whitespace.
	appendItem := func() {
		trimmed := strings.TrimSpace(current.String())
		if trimmed != "" {
			list = append(list, trimmed)
		}
		current.Reset()
	}

	var isEscaped bool
	for _, r := range s {
		if isEscaped {
			current.WriteRune(r)
			isEscaped = false
			continue
		}

		switch {
		case r == '\\' && handleEscapes:
			isEscaped = true
			// For commands, we also keep the backslash for the shell to interpret.
			current.WriteRune(r)
		case r == '\'' || r == '"':
			if quoteChar == 0 { // Start of a new quoted section.
				quoteChar = r
				if keepQuotes {
					current.WriteRune(r)
				}
			} else if quoteChar == r { // End of the current quoted section.
				quoteChar = 0
				if keepQuotes {
					current.WriteRune(r)
				}
			} else { // A different quote character inside an existing quoted section.
				current.WriteRune(r) // Treat it as a literal character.
			}
		case r == ',' && quoteChar == 0: // Comma outside of any quotes.
			appendItem()
		default:
			current.WriteRune(r)
		}
	}
	appendItem() // Add the final item after the loop finishes.
	return list
}
