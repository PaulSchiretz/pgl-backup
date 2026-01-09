package flagparse

import (
	"flag"
	"fmt"
	"runtime"
	"strings"

	"github.com/paulschiretz/pgl-backup/pkg/config"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// ActionFlag defines a special command to execute instead of a backup.
type ActionFlag int

const (
	BackupAction ActionFlag = iota // The default ActionFlag is to run a backup.
	VersionAction
	InitAction
	InitDefaultAction
)

var actionFlagToString = map[ActionFlag]string{BackupAction: "backup", InitAction: "init", InitDefaultAction: "init-default", VersionAction: "version"}
var stringToActionFlag = util.InvertMap(actionFlagToString)

// ParseFlagConfig defines and parses command-line flags, and constructs a
// configuration object containing only the values provided by those flags.
func ParseFlagConfig() (ActionFlag, map[string]interface{}, error) {
	// --- Flag Design Philosophy ---
	// Flags are exposed for options that are useful to override for a single run
	// (e.g., -dry-run, -mode=snapshot, -log-level=debug).
	//
	// Structural options that define the layout of the backup repository (e.g., subdirectories)
	// and complex long-term policies (e.g., retention counts) generally do not have flags.
	//
	// These should be set consistently in the pgl-backup.config.json file to ensure
	// predictable behavior over time.

	// Define flags with zero-value defaults. We will merge them later.
	initFlag := flag.Bool("init", false, "Generate a default pgl-backup.config.json file (preserves existing settings) and exit.")
	initDefaultFlag := flag.Bool("init-default", false, "Generate a default pgl-backup.config.json file (overwrites existing settings) and exit.")
	backupFlag := flag.Bool("backup", false, "Run the backup operation (default action).")
	versionFlag := flag.Bool("version", false, "Print the application version and exit.")
	srcFlag := flag.String("source", "", "Source directory to copy from")
	targetFlag := flag.String("target", "", "Base destination directory for backups")
	modeFlag := flag.String("mode", "incremental", "Backup mode: 'incremental' or 'snapshot'.")
	logLevelFlag := flag.String("log-level", "info", "Set the logging level: 'debug', 'notice', 'info', 'warn', 'error'.")
	failFastFlag := flag.Bool("fail-fast", false, "Stop the backup immediately on the first file sync error.")
	dryRunFlag := flag.Bool("dry-run", false, "Show what would be done without making any changes.")
	metricsFlag := flag.Bool("metrics", false, "Enable detailed performance and file-counting metrics.")
	forceFlag := flag.Bool("force", false, "Bypass confirmation prompts.")
	syncEngineFlag := flag.String("sync-engine", "native", "Sync engine to use: 'native' or 'robocopy' (Windows only).")
	syncWorkersFlag := flag.Int("sync-workers", 0, "Number of worker goroutines for file synchronization.")
	mirrorWorkersFlag := flag.Int("mirror-workers", 0, "Number of worker goroutines for file deletions in mirror mode.")
	deleteWorkersFlag := flag.Int("delete-workers", 0, "Number of worker goroutines for deleting outdated backups.")
	compressWorkersFlag := flag.Int("compress-workers", 0, "Number of worker goroutines for compressing backups.")
	retryCountFlag := flag.Int("retry-count", 0, "Number of retries for failed file copies.")
	retryWaitFlag := flag.Int("retry-wait", 0, "Seconds to wait between retries.")
	bufferSizeKBFlag := flag.Int("buffer-size-kb", 0, "Size of the I/O buffer in kilobytes for file copies and compression.")
	modTimeWindowFlag := flag.Int("mod-time-window", 1, "Time window in seconds to consider file modification times equal (0=exact).")
	userExcludeFilesFlag := flag.String("user-exclude-files", "", "Comma-separated list of case-insensitive file names to exclude (supports glob patterns).")
	userExcludeDirsFlag := flag.String("user-exclude-dirs", "", "Comma-separated list of case-insensitive directory names to exclude (supports glob patterns).")
	preserveSourceNameFlag := flag.Bool("preserve-source-name", true, "Preserve the source directory's name in the destination path. Set to false to sync contents directly.")
	preBackupHooksFlag := flag.String("pre-backup-hooks", "", "Comma-separated list of commands to run before the backup.")
	postBackupHooksFlag := flag.String("post-backup-hooks", "", "Comma-separated list of commands to run after the backup.")
	archiveIntervalSecondsFlag := flag.Int("archive-interval-seconds", 0, "In 'manual' mode, the interval in seconds for creating new archives (e.g., 86400 for 24h).")
	archiveIntervalModeFlag := flag.String("archive-interval-mode", "", "Archive interval mode: 'auto' or 'manual'.")
	compressionEnabledFlag := flag.Bool("compression", true, "Enable compression for backups.")
	compressionFormatFlag := flag.String("compression-format", "", "Compression format for backups: 'zip', 'tar.gz', or 'tar.zst'.")

	flag.Parse()

	// Create a map of the flags that were explicitly set by the user, along with their values.
	// This map is used to selectively override the base configuration.
	usedFlags := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) { usedFlags[f.Name] = true })

	flagMap := make(map[string]interface{})

	// Helper to add a value to the map only if the corresponding flag was set and is not false.
	addActionFlagIfUsed := func(name string, value interface{}) {
		if usedFlags[name] && value.(bool) {
			flagMap[name] = value
		}
	}

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
	addActionFlagIfUsed("init", *initFlag)
	addActionFlagIfUsed("init-default", *initDefaultFlag)
	addActionFlagIfUsed("backup", *backupFlag)
	addActionFlagIfUsed("version", *versionFlag)

	// Populate the map using the helper.
	addIfUsed("source", *srcFlag)
	addIfUsed("target", *targetFlag)
	addIfUsed("log-level", *logLevelFlag)
	addIfUsed("fail-fast", *failFastFlag)
	addIfUsed("dry-run", *dryRunFlag)
	addIfUsed("metrics", *metricsFlag)
	addIfUsed("force", *forceFlag)
	addIfUsed("preserve-source-name", *preserveSourceNameFlag)
	addIfUsed("sync-workers", *syncWorkersFlag)
	addIfUsed("mirror-workers", *mirrorWorkersFlag)
	addIfUsed("delete-workers", *deleteWorkersFlag)
	addIfUsed("compress-workers", *compressWorkersFlag)
	addIfUsed("retry-count", *retryCountFlag)
	addIfUsed("retry-wait", *retryWaitFlag)
	addIfUsed("buffer-size-kb", *bufferSizeKBFlag)
	addIfUsed("mod-time-window", *modTimeWindowFlag)
	addIfUsed("archive-interval-seconds", *archiveIntervalSecondsFlag)
	addIfUsed("compression", *compressionEnabledFlag)

	// Handle flags that require parsing/validation.
	addParsedIfUsed("user-exclude-files", *userExcludeFilesFlag, ParseExcludeList)
	addParsedIfUsed("user-exclude-dirs", *userExcludeDirsFlag, ParseExcludeList)
	addParsedIfUsed("pre-backup-hooks", *preBackupHooksFlag, ParseCmdList)
	addParsedIfUsed("post-backup-hooks", *postBackupHooksFlag, ParseCmdList)

	if usedFlags["mode"] {
		mode, err := config.BackupModeFromString(*modeFlag)
		if err != nil {
			return BackupAction, nil, err
		}
		flagMap["mode"] = mode
	}
	if usedFlags["sync-engine"] {
		engineType, err := config.SyncEngineFromString(*syncEngineFlag)
		if err != nil {
			return BackupAction, nil, err
		}
		flagMap["sync-engine"] = engineType
	}
	if usedFlags["archive-interval-mode"] {
		mode, err := config.ArchiveIntervalModeFromString(*archiveIntervalModeFlag)
		if err != nil {
			return BackupAction, nil, err
		}
		flagMap["archive-interval-mode"] = mode
	}
	if usedFlags["compression-format"] {
		format, err := config.CompressionFormatFromString(*compressionFormatFlag)
		if err != nil {
			return BackupAction, nil, err
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

	if err := ValidateActionFlags(flagMap); err != nil {
		return BackupAction, nil, err
	}

	// Determine which ActionFlag to set based on flags.
	if *versionFlag {
		return VersionAction, flagMap, nil
	}
	if *initFlag {
		return InitAction, flagMap, nil
	}
	if *initDefaultFlag {
		return InitDefaultAction, flagMap, nil
	}
	return BackupAction, flagMap, nil
}

// ValidateActionFlags checks for conflicting actionFlags in the provided map.
func ValidateActionFlags(flagMap map[string]interface{}) error {
	var setFlags []string
	for name := range stringToActionFlag {
		if val, ok := flagMap[name]; ok && val.(bool) {
			setFlags = append(setFlags, "-"+name)
		}
	}
	if len(setFlags) > 1 {
		return fmt.Errorf("cannot use multiple exclusive action flags together: %s", strings.Join(setFlags, ", "))
	}
	return nil
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
