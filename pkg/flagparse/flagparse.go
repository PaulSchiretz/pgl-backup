package flagparse

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/paulschiretz/pgl-backup/pkg/buildinfo"
)

// cliFlags holds pointers to all possible command-line flags.
// Fields are pointers so we can distinguish between "not registered for this command" (nil)
// and "registered but not set by user" (non-nil pointer to zero value).
type cliFlags struct {
	// Global
	LogLevel *string
	DryRun   *bool
	Metrics  *bool

	// Common / Backup / Init
	Source        *string
	Target        *string
	FailFast      *bool
	SyncWorkers   *int
	MirrorWorkers *int
	DeleteWorkers *int
	BufferSizeKB  *int

	SyncIncrementalEngine                *string
	SyncIncrementalRetryCount            *int
	SyncIncrementalRetryWait             *int
	SyncIncrementalModTimeWindow         *int
	SyncIncrementalPreserveSourceDirName *bool

	SyncSnapshotEngine                *string
	SyncSnapshotRetryCount            *int
	SyncSnapshotRetryWait             *int
	SyncSnapshotModTimeWindow         *int
	SyncSnapshotPreserveSourceDirName *bool

	UserExcludeFiles *string
	UserExcludeDirs  *string
	PreBackupHooks   *string
	PostBackupHooks  *string

	ArchiveIncrementalEnabled         *bool
	ArchiveIncrementalIntervalSeconds *int
	ArchiveIncrementalIntervalMode    *string
	ArchiveSnapshotEnabled            *bool
	ArchiveSnapshotIntervalSeconds    *int
	ArchiveSnapshotIntervalMode       *string

	CompressionIncrementalEnabled *bool
	CompressionIncrementalFormat  *string
	CompressionIncrementalLevel   *string
	CompressionSnapshotEnabled    *bool
	CompressionSnapshotFormat     *string
	CompressionSnapshotLevel      *string

	RetentionIncrementalEnabled *bool
	RetentionIncrementalHours   *int
	RetentionIncrementalDays    *int
	RetentionIncrementalWeeks   *int
	RetentionIncrementalMonths  *int
	RetentionIncrementalYears   *int
	RetentionSnapshotEnabled    *bool
	RetentionSnapshotHours      *int
	RetentionSnapshotDays       *int
	RetentionSnapshotWeeks      *int
	RetentionSnapshotMonths     *int
	RetentionSnapshotYears      *int

	// Backup specific
	Mode *string

	// Init specific
	Force   *bool
	Default *bool
}

func registerGlobalFlags(fs *flag.FlagSet, f *cliFlags) {
	f.LogLevel = fs.String("log-level", "info", "Set the logging level: 'debug', 'notice', 'info', 'warn', 'error'.")
	f.DryRun = fs.Bool("dry-run", false, "Show what would be done without making any changes.")
	f.Metrics = fs.Bool("metrics", false, "Enable detailed performance and file-counting metrics.")
}

func registerBackupFlags(fs *flag.FlagSet, f *cliFlags) {
	f.Source = fs.String("source", "", "Source directory to copy from")
	f.Target = fs.String("target", "", "Base destination directory for backups")
	f.Mode = fs.String("mode", "incremental", "Backup mode: 'incremental' or 'snapshot'.")
	f.FailFast = fs.Bool("fail-fast", false, "Stop the backup immediately on the first file sync error.")
	f.SyncWorkers = fs.Int("sync-workers", 0, "Number of worker goroutines for file synchronization.")
	f.MirrorWorkers = fs.Int("mirror-workers", 0, "Number of worker goroutines for file deletions in mirror mode.")
	f.DeleteWorkers = fs.Int("delete-workers", 0, "Number of worker goroutines for deleting outdated backups.")
	f.BufferSizeKB = fs.Int("buffer-size-kb", 0, "Size of the I/O buffer in kilobytes for file copies and compression.")

	f.SyncIncrementalEngine = fs.String("sync-incremental-engine", "native", "Sync engine to use: 'native' or 'robocopy' (Windows only).")
	f.SyncIncrementalRetryCount = fs.Int("sync-incremental-retry-count", 0, "Number of retries for failed file copies.")
	f.SyncIncrementalRetryWait = fs.Int("sync-incremental-retry-wait", 0, "Seconds to wait between retries.")
	f.SyncIncrementalModTimeWindow = fs.Int("sync-incremental-mod-time-window", 1, "Time window in seconds to consider file modification times equal (0=exact).")
	f.SyncIncrementalPreserveSourceDirName = fs.Bool("sync-incremental-preserve-source-dir-name", true, "Preserve the source directory's name in the destination path. Set to false to sync contents directly.")

	f.SyncSnapshotEngine = fs.String("sync-snapshot-engine", "native", "Sync engine to use: 'native' or 'robocopy' (Windows only).")
	f.SyncSnapshotRetryCount = fs.Int("sync-snapshot-retry-count", 0, "Number of retries for failed file copies.")
	f.SyncSnapshotRetryWait = fs.Int("sync-snapshot-retry-wait", 0, "Seconds to wait between retries.")
	f.SyncSnapshotModTimeWindow = fs.Int("sync-snapshot-mod-time-window", 1, "Time window in seconds to consider file modification times equal (0=exact).")
	f.SyncSnapshotPreserveSourceDirName = fs.Bool("sync-snapshot-preserve-source-dir-name", true, "Preserve the source directory's name in the destination path. Set to false to sync contents directly.")

	f.UserExcludeFiles = fs.String("user-exclude-files", "", "Comma-separated list of case-insensitive file names to exclude (supports glob patterns).")
	f.UserExcludeDirs = fs.String("user-exclude-dirs", "", "Comma-separated list of case-insensitive directory names to exclude (supports glob patterns).")
	f.PreBackupHooks = fs.String("pre-backup-hooks", "", "Comma-separated list of commands to run before the backup.")
	f.PostBackupHooks = fs.String("post-backup-hooks", "", "Comma-separated list of commands to run after the backup.")

	f.ArchiveIncrementalEnabled = fs.Bool("archive-incremental", true, "Enable archiving for incremental backups.")
	f.ArchiveIncrementalIntervalSeconds = fs.Int("archive-incremental-interval-seconds", 0, "In 'manual' mode, the interval in seconds for creating new incremental archives (e.g., 86400 for 24h).")
	f.ArchiveIncrementalIntervalMode = fs.String("archive-incremental-interval-mode", "", "Incremental Archive interval mode: 'auto' or 'manual'.")
	f.ArchiveSnapshotEnabled = fs.Bool("archive-snapshot", true, "Enable archiving for snapshot backups.")
	f.ArchiveSnapshotIntervalSeconds = fs.Int("archive-snapshot-interval-seconds", 0, "In 'manual' mode, the interval in seconds for creating new snapshot archives (e.g., 86400 for 24h).")
	f.ArchiveSnapshotIntervalMode = fs.String("archive-snapshot-interval-mode", "", "Snapshot Archive interval mode: 'auto' or 'manual'.")

	f.RetentionIncrementalEnabled = fs.Bool("retention-incremental", true, "Enable retention policy for incremental backups.")
	f.RetentionIncrementalHours = fs.Int("retention-incremental-hours", 0, "Number of hourly backups to keep (incremental).")
	f.RetentionIncrementalDays = fs.Int("retention-incremental-days", 0, "Number of daily backups to keep (incremental).")
	f.RetentionIncrementalWeeks = fs.Int("retention-incremental-weeks", 0, "Number of weekly backups to keep (incremental).")
	f.RetentionIncrementalMonths = fs.Int("retention-incremental-months", 0, "Number of monthly backups to keep (incremental).")
	f.RetentionIncrementalYears = fs.Int("retention-incremental-years", 0, "Number of yearly backups to keep (incremental).")
	f.RetentionSnapshotEnabled = fs.Bool("retention-snapshot", false, "Enable retention policy for snapshot backups.")
	f.RetentionSnapshotHours = fs.Int("retention-snapshot-hours", -1, "Number of hourly backups to keep (snapshot).")
	f.RetentionSnapshotDays = fs.Int("retention-snapshot-days", -1, "Number of daily backups to keep (snapshot).")
	f.RetentionSnapshotWeeks = fs.Int("retention-snapshot-weeks", -1, "Number of weekly backups to keep (snapshot).")
	f.RetentionSnapshotMonths = fs.Int("retention-snapshot-months", -1, "Number of monthly backups to keep (snapshot).")
	f.RetentionSnapshotYears = fs.Int("retention-snapshot-years", -1, "Number of yearly backups to keep (snapshot).")

	f.CompressionIncrementalEnabled = fs.Bool("compression-incremental", true, "Enable compression for incremental backups.")
	f.CompressionIncrementalFormat = fs.String("compression-incremental-format", "", "Compression format for incremental backups: 'zip', 'tar.gz', or 'tar.zst'.")
	f.CompressionIncrementalLevel = fs.String("compression-incremental-level", "", "Compression level for incremental backups: 'default', 'fastest', 'better', 'best'.")
	f.CompressionSnapshotEnabled = fs.Bool("compression-snapshot", true, "Enable compression for snapshot backups.")
	f.CompressionSnapshotFormat = fs.String("compression-snapshot-format", "", "Compression format for snapshot backups: 'zip', 'tar.gz', or 'tar.zst'.")
	f.CompressionSnapshotLevel = fs.String("compression-snapshot-level", "", "Compression level for snapshot backups: 'default', 'fastest', 'better', 'best'.")
}

func registerInitFlags(fs *flag.FlagSet, f *cliFlags) {
	// Init supports all backup flags (to generate config) plus 'force' and 'default'.
	f.Force = fs.Bool("force", false, "Bypass confirmation prompts.")
	f.Default = fs.Bool("default", false, "Overwrite existing configuration with defaults.")

	f.Source = fs.String("source", "", "Source directory to copy from")
	f.Target = fs.String("target", "", "Base destination directory for backups")
	f.FailFast = fs.Bool("fail-fast", false, "Stop the backup immediately on the first file sync error.")
	f.SyncWorkers = fs.Int("sync-workers", 0, "Number of worker goroutines for file synchronization.")
	f.MirrorWorkers = fs.Int("mirror-workers", 0, "Number of worker goroutines for file deletions in mirror mode.")
	f.DeleteWorkers = fs.Int("delete-workers", 0, "Number of worker goroutines for deleting outdated backups.")
	f.BufferSizeKB = fs.Int("buffer-size-kb", 0, "Size of the I/O buffer in kilobytes for file copies and compression.")

	f.SyncIncrementalEngine = fs.String("sync-incremental-engine", "native", "Sync engine to use: 'native' or 'robocopy' (Windows only).")
	f.SyncIncrementalRetryCount = fs.Int("sync-incremental-retry-count", 0, "Number of retries for failed file copies.")
	f.SyncIncrementalRetryWait = fs.Int("sync-incremental-retry-wait", 0, "Seconds to wait between retries.")
	f.SyncIncrementalModTimeWindow = fs.Int("sync-incremental-mod-time-window", 1, "Time window in seconds to consider file modification times equal (0=exact).")
	f.SyncIncrementalPreserveSourceDirName = fs.Bool("sync-incremental-preserve-source-dir-name", true, "Preserve the source directory's name in the destination path. Set to false to sync contents directly.")

	f.SyncSnapshotEngine = fs.String("sync-snapshot-engine", "native", "Sync engine to use: 'native' or 'robocopy' (Windows only).")
	f.SyncSnapshotRetryCount = fs.Int("sync-snapshot-retry-count", 0, "Number of retries for failed file copies.")
	f.SyncSnapshotRetryWait = fs.Int("sync-snapshot-retry-wait", 0, "Seconds to wait between retries.")
	f.SyncSnapshotModTimeWindow = fs.Int("sync-snapshot-mod-time-window", 1, "Time window in seconds to consider file modification times equal (0=exact).")
	f.SyncSnapshotPreserveSourceDirName = fs.Bool("sync-snapshot-preserve-source-dir-name", true, "Preserve the source directory's name in the destination path. Set to false to sync contents directly.")
	f.UserExcludeFiles = fs.String("user-exclude-files", "", "Comma-separated list of case-insensitive file names to exclude (supports glob patterns).")
	f.UserExcludeDirs = fs.String("user-exclude-dirs", "", "Comma-separated list of case-insensitive directory names to exclude (supports glob patterns).")
	f.PreBackupHooks = fs.String("pre-backup-hooks", "", "Comma-separated list of commands to run before the backup.")
	f.PostBackupHooks = fs.String("post-backup-hooks", "", "Comma-separated list of commands to run after the backup.")

	f.ArchiveIncrementalEnabled = fs.Bool("archive-incremental", true, "Enable archiving for incremental backups.")
	f.ArchiveIncrementalIntervalSeconds = fs.Int("archive-incremental-interval-seconds", 0, "In 'manual' mode, the interval in seconds for creating new incremental archives (e.g., 86400 for 24h).")
	f.ArchiveIncrementalIntervalMode = fs.String("archive-incremental-interval-mode", "", "Incremental Archive interval mode: 'auto' or 'manual'.")
	f.ArchiveSnapshotEnabled = fs.Bool("archive-snapshot", true, "Enable archiving for snapshot backups.")
	f.ArchiveSnapshotIntervalSeconds = fs.Int("archive-snapshot-interval-seconds", 0, "In 'manual' mode, the interval in seconds for creating new snapshot archives (e.g., 86400 for 24h).")
	f.ArchiveSnapshotIntervalMode = fs.String("archive-snapshot-interval-mode", "", "Snapshot Archive interval mode: 'auto' or 'manual'.")

	f.RetentionIncrementalEnabled = fs.Bool("retention-incremental", true, "Enable retention policy for incremental backups.")
	f.RetentionIncrementalHours = fs.Int("retention-incremental-hours", 0, "Number of hourly backups to keep (incremental).")
	f.RetentionIncrementalDays = fs.Int("retention-incremental-days", 0, "Number of daily backups to keep (incremental).")
	f.RetentionIncrementalWeeks = fs.Int("retention-incremental-weeks", 0, "Number of weekly backups to keep (incremental).")
	f.RetentionIncrementalMonths = fs.Int("retention-incremental-months", 0, "Number of monthly backups to keep (incremental).")
	f.RetentionIncrementalYears = fs.Int("retention-incremental-years", 0, "Number of yearly backups to keep (incremental).")
	f.RetentionSnapshotEnabled = fs.Bool("retention-snapshot", false, "Enable retention policy for snapshot backups.")
	f.RetentionSnapshotHours = fs.Int("retention-snapshot-hours", -1, "Number of hourly backups to keep (snapshot).")
	f.RetentionSnapshotDays = fs.Int("retention-snapshot-days", -1, "Number of daily backups to keep (snapshot).")
	f.RetentionSnapshotWeeks = fs.Int("retention-snapshot-weeks", -1, "Number of weekly backups to keep (snapshot).")
	f.RetentionSnapshotMonths = fs.Int("retention-snapshot-months", -1, "Number of monthly backups to keep (snapshot).")
	f.RetentionSnapshotYears = fs.Int("retention-snapshot-years", -1, "Number of yearly backups to keep (snapshot).")

	f.CompressionIncrementalEnabled = fs.Bool("compression-incremental", true, "Enable compression for incremental backups.")
	f.CompressionIncrementalFormat = fs.String("compression-incremental-format", "", "Compression format for incremental backups: 'zip', 'tar.gz', or 'tar.zst'.")
	f.CompressionIncrementalLevel = fs.String("compression-incremental-level", "", "Compression level for incremental backups: 'default', 'fastest', 'better', 'best'.")
	f.CompressionSnapshotEnabled = fs.Bool("compression-snapshot", true, "Enable compression for snapshot backups.")
	f.CompressionSnapshotFormat = fs.String("compression-snapshot-format", "", "Compression format for snapshot backups: 'zip', 'tar.gz', or 'tar.zst'.")
	f.CompressionSnapshotLevel = fs.String("compression-snapshot-level", "", "Compression level for snapshot backups: 'default', 'fastest', 'better', 'best'.")
}

func registerPruneFlags(fs *flag.FlagSet, f *cliFlags) {
	f.Target = fs.String("target", "", "Base destination directory for backups to prune")
	f.DeleteWorkers = fs.Int("delete-workers", 0, "Number of worker goroutines for deleting outdated backups.")
}

// Parse parses the provided arguments (usually os.Args[1:]) and returns the action and config map.
func Parse(args []string) (Command, map[string]interface{}, error) {
	// Handle top-level help
	// If no arguments provided, print help and exit.
	if len(args) == 0 {
		fs := flag.NewFlagSet("main", flag.ContinueOnError)
		printTopLevelUsage(fs)
		return None, nil, nil
	}

	cmdStr := strings.ToLower(args[0])

	if cmdStr == "help" || cmdStr == "-h" || cmdStr == "-help" || cmdStr == "--help" {
		fs := flag.NewFlagSet("main", flag.ContinueOnError)
		printTopLevelUsage(fs)
		return None, nil, nil
	}

	f := &cliFlags{}

	command, err := ParseCommand(cmdStr)
	if err != nil {
		return None, nil, err
	}

	// Check for subcommand
	switch command {
	case Init:
		fs := flag.NewFlagSet(command.String(), flag.ContinueOnError)

		registerGlobalFlags(fs, f)
		registerInitFlags(fs, f)

		// Custom usage for the subcommand
		fs.Usage = func() {
			printSubcommandUsage(command, "Initialize a new backup target directory.", fs)
		}

		if err := fs.Parse(args[1:]); err != nil {
			return Init, nil, err
		}

		flagMap, err := flagsToMap(command, fs, f)
		return Init, flagMap, err

	case Prune:
		fs := flag.NewFlagSet(command.String(), flag.ContinueOnError)
		registerGlobalFlags(fs, f)
		registerPruneFlags(fs, f)

		fs.Usage = func() {
			printSubcommandUsage(command, "Apply retention policies to clean up outdated backups.", fs)
		}

		if err := fs.Parse(args[1:]); err != nil {
			return Prune, nil, err
		}
		flagMap, err := flagsToMap(command, fs, f)
		return Prune, flagMap, err

	case Backup:
		fs := flag.NewFlagSet(command.String(), flag.ContinueOnError)
		registerGlobalFlags(fs, f)
		registerBackupFlags(fs, f)

		fs.Usage = func() {
			printSubcommandUsage(command, "Run the backup operation.", fs)
		}

		if err := fs.Parse(args[1:]); err != nil {
			return command, nil, err
		}
		flagMap, err := flagsToMap(command, fs, f)
		return command, flagMap, err

	case Version:
		return command, nil, nil

	default:
		return None, nil, fmt.Errorf("unknown command: %s", args[0])
	}
}

func flagsToMap(c Command, fs *flag.FlagSet, f *cliFlags) (map[string]interface{}, error) {
	// Create a map of the flags that were explicitly set by the user, along with their values.
	// This map is used to selectively override the base configuration.
	usedFlags := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) { usedFlags[f.Name] = true })

	flagMap := make(map[string]any)

	addIfUsed(flagMap, usedFlags, "log-level", f.LogLevel)
	addIfUsed(flagMap, usedFlags, "dry-run", f.DryRun)
	addIfUsed(flagMap, usedFlags, "metrics", f.Metrics)

	addIfUsed(flagMap, usedFlags, "mode", f.Mode)
	addIfUsed(flagMap, usedFlags, "metrics", f.Metrics)

	addIfUsed(flagMap, usedFlags, "source", f.Source)
	addIfUsed(flagMap, usedFlags, "target", f.Target)
	addIfUsed(flagMap, usedFlags, "fail-fast", f.FailFast)
	addIfUsed(flagMap, usedFlags, "sync-workers", f.SyncWorkers)
	addIfUsed(flagMap, usedFlags, "mirror-workers", f.MirrorWorkers)
	addIfUsed(flagMap, usedFlags, "delete-workers", f.DeleteWorkers)
	addIfUsed(flagMap, usedFlags, "buffer-size-kb", f.BufferSizeKB)

	addIfUsed(flagMap, usedFlags, "sync-incremental-engine", f.SyncIncrementalEngine)
	addIfUsed(flagMap, usedFlags, "sync-incremental-retry-count", f.SyncIncrementalRetryCount)
	addIfUsed(flagMap, usedFlags, "sync-incremental-retry-wait", f.SyncIncrementalRetryWait)
	addIfUsed(flagMap, usedFlags, "sync-incremental-mod-time-window", f.SyncIncrementalModTimeWindow)
	addIfUsed(flagMap, usedFlags, "sync-incremental-preserve-source-dir-name", f.SyncIncrementalPreserveSourceDirName)

	addIfUsed(flagMap, usedFlags, "sync-snapshot-engine", f.SyncSnapshotEngine)
	addIfUsed(flagMap, usedFlags, "sync-snapshot-retry-count", f.SyncSnapshotRetryCount)
	addIfUsed(flagMap, usedFlags, "sync-snapshot-retry-wait", f.SyncSnapshotRetryWait)
	addIfUsed(flagMap, usedFlags, "sync-snapshot-mod-time-window", f.SyncSnapshotModTimeWindow)
	addIfUsed(flagMap, usedFlags, "sync-snapshot-preserve-source-dir-name", f.SyncSnapshotPreserveSourceDirName)

	addIfUsed(flagMap, usedFlags, "archive-incremental", f.ArchiveIncrementalEnabled)
	addIfUsed(flagMap, usedFlags, "archive-incremental-interval-seconds", f.ArchiveIncrementalIntervalSeconds)
	addIfUsed(flagMap, usedFlags, "archive-incremental-interval-mode", f.ArchiveIncrementalIntervalMode)
	addIfUsed(flagMap, usedFlags, "archive-snapshot", f.ArchiveSnapshotEnabled)
	addIfUsed(flagMap, usedFlags, "archive-snapshot-interval-seconds", f.ArchiveSnapshotIntervalSeconds)
	addIfUsed(flagMap, usedFlags, "archive-snapshot-interval-mode", f.ArchiveSnapshotIntervalMode)

	addIfUsed(flagMap, usedFlags, "compression-incremental", f.CompressionIncrementalEnabled)
	addIfUsed(flagMap, usedFlags, "compression-incremental-format", f.CompressionIncrementalFormat)
	addIfUsed(flagMap, usedFlags, "compression-incremental-level", f.CompressionIncrementalLevel)
	addIfUsed(flagMap, usedFlags, "compression-snapshot", f.CompressionSnapshotEnabled)
	addIfUsed(flagMap, usedFlags, "compression-snapshot-format", f.CompressionSnapshotFormat)
	addIfUsed(flagMap, usedFlags, "compression-snapshot-level", f.CompressionSnapshotLevel)

	addIfUsed(flagMap, usedFlags, "retention-incremental", f.RetentionIncrementalEnabled)
	addIfUsed(flagMap, usedFlags, "retention-incremental-hours", f.RetentionIncrementalHours)
	addIfUsed(flagMap, usedFlags, "retention-incremental-days", f.RetentionIncrementalDays)
	addIfUsed(flagMap, usedFlags, "retention-incremental-weeks", f.RetentionIncrementalWeeks)
	addIfUsed(flagMap, usedFlags, "retention-incremental-months", f.RetentionIncrementalMonths)
	addIfUsed(flagMap, usedFlags, "retention-incremental-years", f.RetentionIncrementalYears)
	addIfUsed(flagMap, usedFlags, "retention-snapshot", f.RetentionSnapshotEnabled)
	addIfUsed(flagMap, usedFlags, "retention-snapshot-hours", f.RetentionSnapshotHours)
	addIfUsed(flagMap, usedFlags, "retention-snapshot-days", f.RetentionSnapshotDays)
	addIfUsed(flagMap, usedFlags, "retention-snapshot-weeks", f.RetentionSnapshotWeeks)
	addIfUsed(flagMap, usedFlags, "retention-snapshot-months", f.RetentionSnapshotMonths)
	addIfUsed(flagMap, usedFlags, "retention-snapshot-years", f.RetentionSnapshotYears)

	addIfUsed(flagMap, usedFlags, "force", f.Force)
	addIfUsed(flagMap, usedFlags, "default", f.Default)

	// Handle flags that require parsing/validation.
	addParsedIfUsed(flagMap, usedFlags, "user-exclude-files", f.UserExcludeFiles, ParseExcludeList)
	addParsedIfUsed(flagMap, usedFlags, "user-exclude-dirs", f.UserExcludeDirs, ParseExcludeList)
	addParsedIfUsed(flagMap, usedFlags, "pre-backup-hooks", f.PreBackupHooks, ParseCmdList)
	addParsedIfUsed(flagMap, usedFlags, "post-backup-hooks", f.PostBackupHooks, ParseCmdList)

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
func printTopLevelUsage(fs *flag.FlagSet) {

	execName := filepath.Base(os.Args[0])
	fmt.Fprintf(fs.Output(), "%s(%s) ", buildinfo.Name, buildinfo.Version)
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
func printSubcommandUsage(command Command, desc string, fs *flag.FlagSet) {

	execName := filepath.Base(os.Args[0])
	fmt.Fprintf(fs.Output(), "%s(%s) ", buildinfo.Name, buildinfo.Version)
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
