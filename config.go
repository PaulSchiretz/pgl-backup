package main

import "fmt"

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
	Mode      backupMode                  `json:"mode"`
	Naming    backupNamingConfig          `json:"naming"`
	Paths     backupPathConfig            `json:"paths"`
	Retention backupRetentionPolicyConfig `json:"retention"`
}

var defaultConfig = backupConfig{
	Mode: incrementalMode, // Default mode
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
