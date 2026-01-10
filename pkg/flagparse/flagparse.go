package flagparse

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/paulschiretz/pgl-backup/pkg/config"
)

// CommandFlag defines a special command to execute instead of a backup.
type CommandFlag int

const (
	NoCommand CommandFlag = iota
	BackupCommand
	VersionCommand
	InitCommand
	PruneCommand
)

// CmdFlags holds pointers to all possible command-line flags.
// Fields are pointers so we can distinguish between "not registered for this command" (nil)
// and "registered but not set by user" (non-nil pointer to zero value).
type CmdFlags struct {
	// Global
	LogLevel *string
	DryRun   *bool
	Metrics  *bool

	// Common / Backup / Init
	Source                 *string
	Target                 *string
	Mode                   *string
	FailFast               *bool
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

	// Init specific
	Force   *bool
	Default *bool
}

func registerGlobalFlags(fs *flag.FlagSet, f *CmdFlags) {
	f.LogLevel = fs.String("log-level", "info", "Set the logging level: 'debug', 'notice', 'info', 'warn', 'error'.")
	f.DryRun = fs.Bool("dry-run", false, "Show what would be done without making any changes.")
	f.Metrics = fs.Bool("metrics", false, "Enable detailed performance and file-counting metrics.")
}

func registerBackupFlags(fs *flag.FlagSet, f *CmdFlags) {
	f.Source = fs.String("source", "", "Source directory to copy from")
	f.Target = fs.String("target", "", "Base destination directory for backups")
	f.Mode = fs.String("mode", "incremental", "Backup mode: 'incremental' or 'snapshot'.")
	f.FailFast = fs.Bool("fail-fast", false, "Stop the backup immediately on the first file sync error.")
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
}

func registerInitFlags(fs *flag.FlagSet, f *CmdFlags) {
	// Init supports all backup flags (to generate config) plus 'force' and 'default'.
	f.Force = fs.Bool("force", false, "Bypass confirmation prompts.")
	f.Default = fs.Bool("default", false, "Overwrite existing configuration with defaults.")
}

func registerPruneFlags(fs *flag.FlagSet, f *CmdFlags) {
	f.Target = fs.String("target", "", "Base destination directory for backups")
	f.DeleteWorkers = fs.Int("delete-workers", 0, "Number of worker goroutines for deleting outdated backups.")
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

	f := &CmdFlags{}

	// Check for subcommand
	switch cmd {
	case "init":
		fs := flag.NewFlagSet("init", flag.ContinueOnError)

		registerGlobalFlags(fs, f)
		registerBackupFlags(fs, f) // Init also supports all backup flags (to generate config)
		registerInitFlags(fs, f)

		// Custom usage for the subcommand
		fs.Usage = func() {
			printSubcommandUsage(appName, appVersion, "init", "Initialize a new backup target directory.", fs)
		}

		if err := fs.Parse(args[1:]); err != nil {
			return InitCommand, nil, err
		}

		flagMap, err := flagsToMap(fs, f)
		return InitCommand, flagMap, err

	case "prune":
		fs := flag.NewFlagSet("prune", flag.ContinueOnError)
		registerGlobalFlags(fs, f)
		registerPruneFlags(fs, f)

		fs.Usage = func() {
			printSubcommandUsage(appName, appVersion, "prune", "Apply retention policies to clean up outdated backups.", fs)
		}

		if err := fs.Parse(args[1:]); err != nil {
			return PruneCommand, nil, err
		}
		flagMap, err := flagsToMap(fs, f)
		return PruneCommand, flagMap, err

	case "backup":
		fs := flag.NewFlagSet("backup", flag.ContinueOnError)
		registerGlobalFlags(fs, f)
		registerBackupFlags(fs, f)

		fs.Usage = func() {
			printSubcommandUsage(appName, appVersion, "backup", "Run the backup operation.", fs)
		}

		if err := fs.Parse(args[1:]); err != nil {
			return BackupCommand, nil, err
		}
		flagMap, err := flagsToMap(fs, f)
		return BackupCommand, flagMap, err

	case "version":
		return VersionCommand, nil, nil

	default:
		return BackupCommand, nil, fmt.Errorf("unknown command: %s", args[0])
	}
}

func flagsToMap(fs *flag.FlagSet, f *CmdFlags) (map[string]interface{}, error) {
	// Create a map of the flags that were explicitly set by the user, along with their values.
	// This map is used to selectively override the base configuration.
	usedFlags := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) { usedFlags[f.Name] = true })

	flagMap := make(map[string]any)

	addIfUsed(flagMap, usedFlags, "log-level", f.LogLevel)
	addIfUsed(flagMap, usedFlags, "dry-run", f.DryRun)
	addIfUsed(flagMap, usedFlags, "metrics", f.Metrics)

	addIfUsed(flagMap, usedFlags, "source", f.Source)
	addIfUsed(flagMap, usedFlags, "target", f.Target)
	addIfUsed(flagMap, usedFlags, "fail-fast", f.FailFast)
	addIfUsed(flagMap, usedFlags, "sync-workers", f.SyncWorkers)
	addIfUsed(flagMap, usedFlags, "mirror-workers", f.MirrorWorkers)
	addIfUsed(flagMap, usedFlags, "delete-workers", f.DeleteWorkers)
	addIfUsed(flagMap, usedFlags, "compress-workers", f.CompressWorkers)
	addIfUsed(flagMap, usedFlags, "retry-count", f.RetryCount)
	addIfUsed(flagMap, usedFlags, "retry-wait", f.RetryWait)
	addIfUsed(flagMap, usedFlags, "buffer-size-kb", f.BufferSizeKB)
	addIfUsed(flagMap, usedFlags, "mod-time-window", f.ModTimeWindow)
	addIfUsed(flagMap, usedFlags, "preserve-source-name", f.PreserveSourceName)
	addIfUsed(flagMap, usedFlags, "archive-interval-seconds", f.ArchiveIntervalSeconds)
	addIfUsed(flagMap, usedFlags, "compression", f.CompressionEnabled)

	// Init specific flags
	addIfUsed(flagMap, usedFlags, "force", f.Force)
	addIfUsed(flagMap, usedFlags, "default", f.Default)

	// Handle flags that require parsing/validation.
	addParsedIfUsed(flagMap, usedFlags, "user-exclude-files", f.UserExcludeFiles, ParseExcludeList)
	addParsedIfUsed(flagMap, usedFlags, "user-exclude-dirs", f.UserExcludeDirs, ParseExcludeList)
	addParsedIfUsed(flagMap, usedFlags, "pre-backup-hooks", f.PreBackupHooks, ParseCmdList)
	addParsedIfUsed(flagMap, usedFlags, "post-backup-hooks", f.PostBackupHooks, ParseCmdList)

	if f.Mode != nil && usedFlags["mode"] {
		mode, err := config.BackupModeFromString(*f.Mode)
		if err != nil {
			return nil, err
		}
		flagMap["mode"] = mode
	}
	if f.SyncEngine != nil && usedFlags["sync-engine"] {
		engineType, err := config.SyncEngineFromString(*f.SyncEngine)
		if err != nil {
			return nil, err
		}

		// Final sanity check: if robocopy was requested on a non-windows OS, force native.
		if runtime.GOOS != "windows" && engineType == config.RobocopyEngine {
			flagMap["sync-engine"] = config.NativeEngine
		} else {
			flagMap["sync-engine"] = engineType
		}
	}
	if f.ArchiveIntervalMode != nil && usedFlags["archive-interval-mode"] {
		mode, err := config.ArchiveIntervalModeFromString(*f.ArchiveIntervalMode)
		if err != nil {
			return nil, err
		}
		flagMap["archive-interval-mode"] = mode
	}
	if f.CompressionFormat != nil && usedFlags["compression-format"] {
		format, err := config.CompressionFormatFromString(*f.CompressionFormat)
		if err != nil {
			return nil, err
		}
		flagMap["compression-format"] = format
	}
	return flagMap, nil
}

// addIfUsed adds the value of ptr to flagMap if ptr is not nil and the flag was set.
func addIfUsed[T any](flagMap map[string]interface{}, usedFlags map[string]bool, name string, ptr *T) {
	if ptr != nil && usedFlags[name] {
		flagMap[name] = *ptr
	}
}

// addParsedIfUsed adds the parsed value of ptr to flagMap if ptr is not nil and the flag was set.
func addParsedIfUsed(flagMap map[string]interface{}, usedFlags map[string]bool, name string, ptr *string, parser func(string) []string) {
	if ptr != nil && usedFlags[name] {
		flagMap[name] = parser(*ptr)
	}
}

// printTopLevelUsage prints the main help message.
func printTopLevelUsage(appName, appVersion string, fs *flag.FlagSet) {

	execName := filepath.Base(os.Args[0])
	fmt.Fprintf(fs.Output(), "%s(%s) ", appName, appVersion)
	fmt.Fprintf(fs.Output(), "A simple and powerful cross-platform backup utility.\n\n")
	fmt.Fprintf(fs.Output(), "Usage: %s <command> [flags]\n\n", execName)
	fmt.Fprintf(fs.Output(), "Commands:\n")
	fmt.Fprintf(fs.Output(), "  backup      Run the backup operation\n")
	fmt.Fprintf(fs.Output(), "  prune       Apply retention policies to clean up outdated backups\n")
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
