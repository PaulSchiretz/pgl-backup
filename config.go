package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

type backupNamingConfig struct {
	Prefix                string `json:"prefix"`
	TimeFormat            string `json:"timeFormat"`
	IncrementalModeSuffix string `json:"incrementalModeSuffix"`
}

type backupPathConfig struct {
	Source     string
	TargetBase string
	// CurrentTarget is the full, calculated path for this specific backup operation.
	CurrentTarget string
}

type backupRetentionPolicyConfig struct {
	Hours  int
	Days   int
	Weeks  int
	Months int
}

// backupMode represents the operational mode of the backup (incremental or snapshot).
type backupMode int

// Constants for backupMode, acting as an enum.
const (
	incrementalMode backupMode = iota // 0
	snapshotMode                      // 1
)

// String returns the string representation of a backupMode.
func (bm backupMode) String() string {
	switch bm {
	case incrementalMode:
		return "incremental"
	case snapshotMode:
		return "snapshot"
	default:
		return fmt.Sprintf("unknown_mode(%d)", bm)
	}
}

type backupConfig struct {
	Mode        backupMode                  `json:"mode"`
	UseRobocopy bool                        `json:"useRobocopy"`
	Quiet       bool                        `json:"quiet"`
	DryRun      bool                        `json:"dryRun"`
	Naming      backupNamingConfig          `json:"naming"`
	Paths       backupPathConfig            `json:"paths"`
	Retention   backupRetentionPolicyConfig `json:"retention"`
}

var defaultConfig = backupConfig{
	Mode:        incrementalMode, // Default mode
	UseRobocopy: true,            // Default to true; will only be active on Windows.
	Quiet:       false,
	DryRun:      false,
	Naming: backupNamingConfig{
		Prefix:                "5ive_Backup_",
		TimeFormat:            "2006-01-02-15-04-05-000",
		IncrementalModeSuffix: "current",
	},
	Paths: backupPathConfig{
		Source:     "./src_backup",
		TargetBase: "./dest_backup_mirror",
		// CurrentTarget is calculated at runtime.
	},
	Retention: backupRetentionPolicyConfig{
		Hours:  24, // N > 0: keep one backup for each of the last N hours of today.
		Days:   7,  // N > 0: keep one backup for each of the last N days.
		Weeks:  4,  // N > 0: keep one backup for each of the last N weeks.
		Months: 12, // N > 0: keep one backup for each of the last N months.
	},
}

// loadConfig attempts to load a configuration from "ppBackup.conf".
// If the file doesn't exist, it returns the provided default config without an error.
// If the file exists but fails to parse, it returns an error and a zero-value config.
func loadConfig() (backupConfig, error) {
	exePath, err := os.Executable()
	if err != nil {
		// Cannot find exe path, proceed with defaults but log a warning.
		log.Printf("Warning: could not determine executable path: %v. Using default config.", err)
		return defaultConfig, nil
	}

	configPath := filepath.Join(filepath.Dir(exePath), "ppBackup.conf")

	file, err := os.Open(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return defaultConfig, nil // Config file doesn't exist, which is a normal case.
		}
		return backupConfig{}, fmt.Errorf("error opening config file %s: %w", configPath, err)
	}
	defer file.Close()

	log.Printf("Loading configuration from %s", configPath)
	// Start with default values, then overwrite with the file's content.
	// This makes the config loading resilient to missing fields in the JSON file.
	config := defaultConfig
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&config); err != nil {
		return backupConfig{}, fmt.Errorf("error parsing config file %s: %w", configPath, err)
	}
	return config, nil
}

// generateConfig creates a default ppBackup.conf file in the executable's
// directory. It will not overwrite an existing file.
func generateConfig() error {
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
	jsonData, err := json.MarshalIndent(defaultConfig, "", "  ")
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
