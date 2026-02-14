package planner_test

import (
	"testing"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/config"
	"github.com/paulschiretz/pgl-backup/pkg/patharchive"
	"github.com/paulschiretz/pgl-backup/pkg/pathcompression"
	"github.com/paulschiretz/pgl-backup/pkg/pathsync"
	"github.com/paulschiretz/pgl-backup/pkg/planner"
)

func TestGenerateBackupPlan(t *testing.T) {
	tests := []struct {
		name         string
		configMod    func(*config.Config)
		expectedMode planner.Mode
		expectError  bool
		validate     func(*testing.T, *planner.BackupPlan)
	}{
		{
			name: "Incremental Mode - Default",
			configMod: func(c *config.Config) {
				c.Runtime.Mode = "incremental"
				// Ensure defaults are set as expected by NewDefault, but we can override specific ones to test mapping
				c.Sync.PreserveSourceDirName = true
			},
			expectedMode: planner.Incremental,
			validate: func(t *testing.T, p *planner.BackupPlan) {
				if !p.Sync.PreserveSourceDirName {
					t.Error("Expected PreserveSourceDirName to be true")
				}
				if p.Archive.IntervalMode != patharchive.Auto {
					t.Errorf("Expected Archive IntervalMode Auto, got %v", p.Archive.IntervalMode)
				}
				if p.Paths.RelCurrentPathKey != "PGL_Backup_Incremental_Current" {
					t.Errorf("Expected default incremental current path, got %s", p.Paths.RelCurrentPathKey)
				}
			},
		},
		{
			name: "Snapshot Mode",
			configMod: func(c *config.Config) {
				c.Runtime.Mode = "snapshot"
				c.Sync.PreserveSourceDirName = false
			},
			expectedMode: planner.Snapshot,
			validate: func(t *testing.T, p *planner.BackupPlan) {
				if p.Sync.PreserveSourceDirName {
					t.Error("Expected PreserveSourceDirName to be false")
				}
				if p.Paths.RelCurrentPathKey != "PGL_Backup_Snapshot_Current" {
					t.Errorf("Expected default snapshot current path, got %s", p.Paths.RelCurrentPathKey)
				}
			},
		},
		{
			name: "Invalid Mode",
			configMod: func(c *config.Config) {
				c.Runtime.Mode = "invalid"
			},
			expectError: true,
		},
		{
			name: "Invalid Archive Interval Mode",
			configMod: func(c *config.Config) {
				c.Runtime.Mode = "incremental"
				c.Archive.IntervalMode = "invalid"
			},
			expectError: true,
		},
		{
			name: "Invalid Sync Engine",
			configMod: func(c *config.Config) {
				c.Runtime.Mode = "incremental"
				c.Sync.Engine = "invalid"
			},
			expectError: true,
		},
		{
			name: "Invalid Compression Format",
			configMod: func(c *config.Config) {
				c.Runtime.Mode = "incremental"
				c.Compression.Format = "invalid"
			},
			expectError: true,
		},
		{
			name: "Retention Constraints Mapping",
			configMod: func(c *config.Config) {
				c.Runtime.Mode = "incremental"
				c.Retention.Incremental.Enabled = true
				c.Retention.Incremental.Days = 7
			},
			expectedMode: planner.Incremental,
			validate: func(t *testing.T, p *planner.BackupPlan) {
				if p.Archive.Constraints.Days != 7 {
					t.Errorf("Expected Archive Constraints Days to be 7, got %d", p.Archive.Constraints.Days)
				}
			},
		},
		{
			name: "Retention Disabled Constraints Mapping",
			configMod: func(c *config.Config) {
				c.Runtime.Mode = "incremental"
				c.Retention.Incremental.Enabled = false
				c.Retention.Incremental.Days = 7 // Should be ignored
			},
			expectedMode: planner.Incremental,
			validate: func(t *testing.T, p *planner.BackupPlan) {
				if p.Archive.Constraints.Days != 0 {
					t.Errorf("Expected Archive Constraints Days to be 0 when retention disabled, got %d", p.Archive.Constraints.Days)
				}
			},
		},
		{
			name: "Sync Engine Parsing",
			configMod: func(c *config.Config) {
				c.Runtime.Mode = "incremental"
				c.Sync.Engine = "robocopy"
			},
			expectedMode: planner.Incremental,
			validate: func(t *testing.T, p *planner.BackupPlan) {
				if p.Sync.Engine != pathsync.Robocopy {
					t.Errorf("Expected Sync Engine Robocopy, got %v", p.Sync.Engine)
				}
			},
		},
		{
			name: "Compression Format Parsing",
			configMod: func(c *config.Config) {
				c.Runtime.Mode = "incremental"
				c.Compression.Format = "zip"
			},
			expectedMode: planner.Incremental,
			validate: func(t *testing.T, p *planner.BackupPlan) {
				if p.Compression.Format != pathcompression.Zip {
					t.Errorf("Expected Compression Format Zip, got %v", p.Compression.Format)
				}
			},
		},
		{
			name: "Global Flags Mapping",
			configMod: func(c *config.Config) {
				c.Runtime.Mode = "incremental"
				c.Runtime.DryRun = true
				c.Engine.FailFast = true
				c.Engine.Metrics = false
				c.Sync.RetryCount = 5
				c.Sync.RetryWaitSeconds = 10
				c.Sync.ModTimeWindowSeconds = 2
			},
			expectedMode: planner.Incremental,
			validate: func(t *testing.T, p *planner.BackupPlan) {
				if !p.DryRun {
					t.Error("Expected DryRun to be true")
				}
				if !p.FailFast {
					t.Error("Expected FailFast to be true")
				}
				if p.Metrics {
					t.Error("Expected Metrics to be false")
				}
				// Check Sync specific mapping
				if p.Sync.RetryCount != 5 {
					t.Errorf("Expected RetryCount 5, got %d", p.Sync.RetryCount)
				}
				if p.Sync.RetryWait != 10*time.Second {
					t.Errorf("Expected RetryWait 10s, got %v", p.Sync.RetryWait)
				}
				if p.Sync.ModTimeWindow != 2*time.Second {
					t.Errorf("Expected ModTimeWindow 2s, got %v", p.Sync.ModTimeWindow)
				}
			},
		},
		{
			name: "Ignore Case Mismatch",
			configMod: func(c *config.Config) {
				c.Runtime.Mode = "incremental"
				c.Runtime.IgnoreCaseMismatch = true
			},
			expectedMode: planner.Incremental,
			validate: func(t *testing.T, p *planner.BackupPlan) {
				if p.Preflight.CaseMismatch {
					t.Error("Expected Preflight.CaseMismatch to be false when IgnoreCaseMismatch is true")
				}
			},
		},
		{
			name: "Retention Policy Mapping",
			configMod: func(c *config.Config) {
				c.Runtime.Mode = "incremental"
				c.Retention.Incremental.Enabled = true
				c.Retention.Incremental.Hours = 1
				c.Retention.Incremental.Days = 2
				c.Retention.Incremental.Weeks = 3
				c.Retention.Incremental.Months = 4
				c.Retention.Incremental.Years = 5
			},
			expectedMode: planner.Incremental,
			validate: func(t *testing.T, p *planner.BackupPlan) {
				if !p.Retention.Enabled {
					t.Error("Expected Retention Enabled")
				}
				if p.Retention.Hours != 1 {
					t.Errorf("Expected Retention Hours 1, got %d", p.Retention.Hours)
				}
				if p.Retention.Days != 2 {
					t.Errorf("Expected Retention Days 2, got %d", p.Retention.Days)
				}
				if p.Retention.Weeks != 3 {
					t.Errorf("Expected Retention Weeks 3, got %d", p.Retention.Weeks)
				}
				if p.Retention.Months != 4 {
					t.Errorf("Expected Retention Months 4, got %d", p.Retention.Months)
				}
				if p.Retention.Years != 5 {
					t.Errorf("Expected Retention Years 5, got %d", p.Retention.Years)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.NewDefault()
			if tc.configMod != nil {
				tc.configMod(&cfg)
			}

			plan, err := planner.GenerateBackupPlan(cfg)

			if tc.expectError {
				if err == nil {
					t.Error("Expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if plan.Mode != tc.expectedMode {
					t.Errorf("Expected mode %v, got %v", tc.expectedMode, plan.Mode)
				}
				if tc.validate != nil {
					tc.validate(t, plan)
				}
			}
		})
	}
}

func TestGenerateRestorePlan(t *testing.T) {
	tests := []struct {
		name         string
		configMod    func(*config.Config)
		expectedMode planner.Mode
		expectError  bool
		validate     func(*testing.T, *planner.RestorePlan)
	}{
		{
			name:         "Default Configuration",
			configMod:    nil,
			expectedMode: planner.Any,
		},
		{
			name: "Explicit Incremental Mode",
			configMod: func(c *config.Config) {
				c.Runtime.Mode = "incremental"
			},
			expectedMode: planner.Incremental,
		},
		{
			name: "Explicit Snapshot Mode",
			configMod: func(c *config.Config) {
				c.Runtime.Mode = "snapshot"
			},
			expectedMode: planner.Snapshot,
		},
		{
			name: "Invalid Mode",
			configMod: func(c *config.Config) {
				c.Runtime.Mode = "invalid"
			},
			expectError: true,
		},
		{
			name: "Overwrite Behavior Parsing",
			configMod: func(c *config.Config) {
				c.Runtime.RestoreOverwriteBehavior = "if-newer"
			},
			expectedMode: planner.Any,
			validate: func(t *testing.T, p *planner.RestorePlan) {
				if p.Sync.OverwriteBehavior != pathsync.OverwriteIfNewer {
					t.Errorf("Expected Sync OverwriteBehavior IfNewer, got %v", p.Sync.OverwriteBehavior)
				}
				if p.Extraction.OverwriteBehavior != pathcompression.OverwriteIfNewer {
					t.Errorf("Expected Extraction OverwriteBehavior IfNewer, got %v", p.Extraction.OverwriteBehavior)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.NewDefault()
			if tc.configMod != nil {
				tc.configMod(&cfg)
			}

			plan, err := planner.GenerateRestorePlan(cfg)

			if tc.expectError {
				if err == nil {
					t.Error("Expected error, got nil")
				}
			} else {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if plan.Mode != tc.expectedMode {
					t.Errorf("Expected mode %v, got %v", tc.expectedMode, plan.Mode)
				}
				if tc.validate != nil {
					tc.validate(t, plan)
				}
			}
		})
	}
}

func TestGenerateListPlan(t *testing.T) {
	tests := []struct {
		name         string
		configMod    func(*config.Config)
		expectedSort planner.SortOrder
		expectedMode planner.Mode
		expectError  bool
	}{
		{
			name:         "Default Configuration",
			configMod:    nil,
			expectedSort: planner.Desc,
			expectedMode: planner.Any,
		},
		{
			name: "Explicit Asc Sort Order",
			configMod: func(c *config.Config) {
				c.Runtime.ListSort = "asc"
			},
			expectedSort: planner.Asc,
			expectedMode: planner.Any,
		},
		{
			name: "Explicit Desc Sort Order",
			configMod: func(c *config.Config) {
				c.Runtime.ListSort = "desc"
			},
			expectedSort: planner.Desc,
			expectedMode: planner.Any,
		},
		{
			name: "Explicit Incremental Mode",
			configMod: func(c *config.Config) {
				c.Runtime.Mode = "incremental"
			},
			expectedSort: planner.Desc,
			expectedMode: planner.Incremental,
		},
		{
			name: "Explicit Snapshot Mode",
			configMod: func(c *config.Config) {
				c.Runtime.Mode = "snapshot"
			},
			expectedSort: planner.Desc,
			expectedMode: planner.Snapshot,
		},
		{
			name: "Invalid Sort Order",
			configMod: func(c *config.Config) {
				c.Runtime.ListSort = "invalid"
			},
			expectError: true,
		},
		{
			name: "Invalid Mode",
			configMod: func(c *config.Config) {
				c.Runtime.Mode = "invalid"
			},
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.NewDefault()
			if tc.configMod != nil {
				tc.configMod(&cfg)
			}

			plan, err := planner.GenerateListPlan(cfg)

			if tc.expectError {
				if err == nil {
					t.Error("Expected error, got nil")
				}
			} else {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if plan.SortOrder != tc.expectedSort {
					t.Errorf("Expected sort order %v, got %v", tc.expectedSort, plan.SortOrder)
				}
				if plan.Mode != tc.expectedMode {
					t.Errorf("Expected mode %v, got %v", tc.expectedMode, plan.Mode)
				}
			}
		})
	}
}

func TestGeneratePrunePlan(t *testing.T) {
	tests := []struct {
		name         string
		configMod    func(*config.Config)
		expectedMode planner.Mode
		expectError  bool
		validate     func(*testing.T, *planner.PrunePlan)
	}{
		{
			name: "Basic Mapping",
			configMod: func(c *config.Config) {
				c.Retention.Incremental.Enabled = true
				c.Retention.Incremental.Days = 5
				c.Retention.Snapshot.Enabled = true
				c.Retention.Snapshot.Weeks = 2
				c.Paths.Incremental.Archive = "inc_archive"
				c.Paths.Snapshot.Archive = "snap_archive"
				c.Runtime.DryRun = true
				c.Engine.FailFast = true
			},
			expectedMode: planner.Any,
			validate: func(t *testing.T, p *planner.PrunePlan) {
				if !p.RetentionIncremental.Enabled {
					t.Error("Expected Incremental Retention Enabled")
				}
				if p.RetentionIncremental.Days != 5 {
					t.Errorf("Expected Incremental Days 5, got %d", p.RetentionIncremental.Days)
				}
				if !p.RetentionSnapshot.Enabled {
					t.Error("Expected Snapshot Retention Enabled")
				}
				if p.RetentionSnapshot.Weeks != 2 {
					t.Errorf("Expected Snapshot Weeks 2, got %d", p.RetentionSnapshot.Weeks)
				}
				if p.PathsIncremental.RelArchivePathKey != "inc_archive" {
					t.Errorf("Expected Incremental Archive Path 'inc_archive', got %s", p.PathsIncremental.RelArchivePathKey)
				}
				if p.PathsSnapshot.RelArchivePathKey != "snap_archive" {
					t.Errorf("Expected Snapshot Archive Path 'snap_archive', got %s", p.PathsSnapshot.RelArchivePathKey)
				}
				if !p.DryRun {
					t.Error("Expected DryRun to be true")
				}
				if !p.FailFast {
					t.Error("Expected FailFast to be true")
				}
			},
		},
		{
			name: "Explicit Incremental Mode",
			configMod: func(c *config.Config) {
				c.Runtime.Mode = "incremental"
			},
			expectedMode: planner.Incremental,
		},
		{
			name: "Explicit Snapshot Mode",
			configMod: func(c *config.Config) {
				c.Runtime.Mode = "snapshot"
			},
			expectedMode: planner.Snapshot,
		},
		{
			name: "Invalid Mode",
			configMod: func(c *config.Config) {
				c.Runtime.Mode = "invalid"
			},
			expectError: true,
		},
		{
			name: "Retention Policy Mapping",
			configMod: func(c *config.Config) {
				c.Retention.Incremental.Enabled = true
				c.Retention.Incremental.Hours = 1
				c.Retention.Incremental.Days = 2
				c.Retention.Incremental.Weeks = 3
				c.Retention.Incremental.Months = 4
				c.Retention.Incremental.Years = 5

				c.Retention.Snapshot.Enabled = true
				c.Retention.Snapshot.Hours = 6
				c.Retention.Snapshot.Days = 7
				c.Retention.Snapshot.Weeks = 8
				c.Retention.Snapshot.Months = 9
				c.Retention.Snapshot.Years = 10
			},
			expectedMode: planner.Any,
			validate: func(t *testing.T, p *planner.PrunePlan) {
				// Incremental
				if !p.RetentionIncremental.Enabled {
					t.Error("Expected Incremental Retention Enabled")
				}
				if p.RetentionIncremental.Hours != 1 {
					t.Errorf("Expected Incremental Hours 1, got %d", p.RetentionIncremental.Hours)
				}
				if p.RetentionIncremental.Days != 2 {
					t.Errorf("Expected Incremental Days 2, got %d", p.RetentionIncremental.Days)
				}
				if p.RetentionIncremental.Weeks != 3 {
					t.Errorf("Expected Incremental Weeks 3, got %d", p.RetentionIncremental.Weeks)
				}
				if p.RetentionIncremental.Months != 4 {
					t.Errorf("Expected Incremental Months 4, got %d", p.RetentionIncremental.Months)
				}
				if p.RetentionIncremental.Years != 5 {
					t.Errorf("Expected Incremental Years 5, got %d", p.RetentionIncremental.Years)
				}

				// Snapshot
				if !p.RetentionSnapshot.Enabled {
					t.Error("Expected Snapshot Retention Enabled")
				}
				if p.RetentionSnapshot.Hours != 6 {
					t.Errorf("Expected Snapshot Hours 6, got %d", p.RetentionSnapshot.Hours)
				}
				if p.RetentionSnapshot.Days != 7 {
					t.Errorf("Expected Snapshot Days 7, got %d", p.RetentionSnapshot.Days)
				}
				if p.RetentionSnapshot.Weeks != 8 {
					t.Errorf("Expected Snapshot Weeks 8, got %d", p.RetentionSnapshot.Weeks)
				}
				if p.RetentionSnapshot.Months != 9 {
					t.Errorf("Expected Snapshot Months 9, got %d", p.RetentionSnapshot.Months)
				}
				if p.RetentionSnapshot.Years != 10 {
					t.Errorf("Expected Snapshot Years 10, got %d", p.RetentionSnapshot.Years)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.NewDefault()
			if tc.configMod != nil {
				tc.configMod(&cfg)
			}

			plan, err := planner.GeneratePrunePlan(cfg)

			if tc.expectError {
				if err == nil {
					t.Error("Expected error, got nil")
				}
			} else {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if plan.Mode != tc.expectedMode {
					t.Errorf("Expected mode %v, got %v", tc.expectedMode, plan.Mode)
				}
				if tc.validate != nil {
					tc.validate(t, plan)
				}
			}
		})
	}
}
