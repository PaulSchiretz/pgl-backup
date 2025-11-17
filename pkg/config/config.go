package config

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
)

type BackupNamingConfig struct {
	Prefix                string `json:"prefix"`
	TimeFormat            string `json:"timeFormat"`
	IncrementalModeSuffix string `json:"incrementalModeSuffix"`
}

type BackupPathConfig struct {
	Source     string `json:"source"`
	TargetBase string `json:"targetBase"`
}

type BackupRetentionPolicyConfig struct {
	Hours  int `json:"hours"`
	Days   int `json:"days"`
	Weeks  int `json:"weeks"`
	Months int `json:"months"`
}

// BackupMode represents the operational mode of the backup (incremental or snapshot).
type BackupMode int

// Constants for BackupMode, acting as an enum.
const (
	IncrementalMode BackupMode = iota // 0
	SnapshotMode                      // 1
)

// String returns the string representation of a BackupMode.
func (bm BackupMode) String() string {
	switch bm {
	case IncrementalMode:
		return "incremental"
	case SnapshotMode:
		return "snapshot"
	default:
		return fmt.Sprintf("unknown_backup_mode(%d)", bm)
	}
}

// ModeFromString parses a string and returns the corresponding BackupMode.
func ModeFromString(s string) (BackupMode, error) {
	switch s {
	case "incremental":
		return IncrementalMode, nil
	case "snapshot":
		return SnapshotMode, nil
	default:
		return 0, fmt.Errorf("invalid BackupMode: %q. Must be 'incremental' or 'snapshot'", s)
	}
}

// MarshalJSON implements the json.Marshaler interface for BackupMode.
func (bm BackupMode) MarshalJSON() ([]byte, error) {
	return json.Marshal(bm.String())
}

// UnmarshalJSON implements the json.Unmarshaler interface for BackupMode.
func (bm *BackupMode) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("BackupMode should be a string, got %s", data)
	}

	mode, err := ModeFromString(s)
	if err != nil {
		return err
	}
	*bm = mode
	return nil
}

// SyncEngine represents the file synchronization engine to use.
type SyncEngine int

const (
	// NativeEngine uses the cross-platform Go implementation.
	NativeEngine SyncEngine = iota
	// RobocopyEngine uses the Windows-specific robocopy utility.
	RobocopyEngine
)

// String returns the string representation of a SyncEngine.
func (se SyncEngine) String() string {
	switch se {
	case NativeEngine:
		return "native"
	case RobocopyEngine:
		return "robocopy"
	default:
		return fmt.Sprintf("unknown_sync_engine(%d)", se)
	}
}

// EngineTypeFromString parses a string and returns the corresponding SyncEngine.
func EngineTypeFromString(s string) (SyncEngine, error) {
	switch s {
	case "native":
		return NativeEngine, nil
	case "robocopy":
		return RobocopyEngine, nil
	default:
		return 0, fmt.Errorf("invalid SyncEngine: %q. Must be 'native' or 'robocopy'", s)
	}
}

// MarshalJSON implements the json.Marshaler interface for SyncEngine.
func (se SyncEngine) MarshalJSON() ([]byte, error) {
	return json.Marshal(se.String())
}

// UnmarshalJSON implements the json.Unmarshaler interface for SyncEngine.
func (se *SyncEngine) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("SyncEngine should be a string, got %s", data)
	}

	engine, err := EngineTypeFromString(s)
	if err != nil {
		return err
	}
	*se = engine
	return nil
}

type BackupEngineConfig struct {
	Type                SyncEngine `json:"type"`
	NativeEngineWorkers int        `json:"nativeEngineWorkers"`
}

type Config struct {
	Mode            BackupMode                  `json:"mode"`
	Engine          BackupEngineConfig          `json:"engine"`
	Quiet           bool                        `json:"quiet"`
	DryRun          bool                        `json:"dryRun"`
	Naming          BackupNamingConfig          `json:"naming"`
	Paths           BackupPathConfig            `json:"paths"`
	RetentionPolicy BackupRetentionPolicyConfig `json:"retentionPolicy"`
}

// NewDefault creates and returns a Config struct with sensible default
// values. It dynamically sets the sync engine based on the operating system.
func NewDefault() Config {
	// Set a default sync engine based on the OS.
	defaultEngine := NativeEngine
	if runtime.GOOS == "windows" {
		defaultEngine = RobocopyEngine
	}
	return Config{
		Mode:   IncrementalMode, // Default mode
		Quiet:  false,
		DryRun: false,
		Engine: BackupEngineConfig{
			Type:                defaultEngine,
			NativeEngineWorkers: runtime.NumCPU(), // Default to the number of CPU cores.
		},
		Naming: BackupNamingConfig{
			Prefix:                "5ive_Backup_",
			TimeFormat:            "2006-01-02-15-04-05-000",
			IncrementalModeSuffix: "current",
		},
		Paths: BackupPathConfig{
			Source:     "./src_backup",
			TargetBase: "./dest_backup_mirror",
		},
		RetentionPolicy: BackupRetentionPolicyConfig{
			Hours:  24, // N > 0: keep the N most recent hourly backups.
			Days:   7,  // N > 0: keep one backup for each of the last N days.
			Weeks:  4,  // N > 0: keep one backup for each of the last N weeks.
			Months: 12, // N > 0: keep one backup for each of the last N months.
		},
	}
}

// Load attempts to load a configuration from "ppBackup.conf".
// If the file doesn't exist, it returns the provided default config without an error.
// If the file exists but fails to parse, it returns an error and a zero-value config.
func Load() (Config, error) {
	exePath, err := os.Executable()
	if err != nil {
		// Cannot find exe path, proceed with defaults but log a warning.
		log.Printf("Warning: could not determine executable path: %v. Using default config.", err)
		return NewDefault(), nil
	}

	configPath := filepath.Join(filepath.Dir(exePath), "ppBackup.conf")

	file, err := os.Open(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return NewDefault(), nil // Config file doesn't exist, which is a normal case.
		}
		return Config{}, fmt.Errorf("error opening config file %s: %w", configPath, err)
	}
	defer file.Close()

	log.Printf("Loading configuration from %s", configPath)
	// Start with default values, then overwrite with the file's content.
	// This makes the config loading resilient to missing fields in the JSON file.
	config := NewDefault()
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&config); err != nil {
		return Config{}, fmt.Errorf("error parsing config file %s: %w", configPath, err)
	}
	return config, nil
}

// Generate creates a default ppBackup.conf file in the executable's
// directory. It will not overwrite an existing file.
func Generate() error {
	exePath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("could not determine executable path: %w", err)
	}

	configPath := filepath.Join(filepath.Dir(exePath), "ppBackup.conf")

	// Check if the file already exists to prevent overwriting.
	if _, err := os.Stat(configPath); err == nil {
		return fmt.Errorf("config file already exists at %s, will not overwrite", configPath)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("could not check for existing config file: %w", err)
	}

	// Marshal the default config into nicely formatted JSON.
	jsonData, err := json.MarshalIndent(NewDefault(), "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal default config to JSON: %w", err)
	}

	// Write the JSON data to the file.
	if err := os.WriteFile(configPath, jsonData, 0664); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	log.Printf("Successfully created default config file at: %s", configPath)
	return nil
}
