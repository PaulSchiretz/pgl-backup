package pathretention

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/config"
	"github.com/paulschiretz/pgl-backup/pkg/metafile"
)

// Helper to create a dummy PathRetentionManager for testing.
func newTestRetentionManager(cfg config.Config) *PathRetentionManager {
	return NewPathRetentionManager(cfg)
}

// Helper to create a temporary directory structure for a backup with a metafile.
func createTestBackup(t *testing.T, baseDir, name string, timestampUTC time.Time) {
	t.Helper()
	backupPath := filepath.Join(baseDir, name)
	if err := os.MkdirAll(backupPath, 0755); err != nil {
		t.Fatalf("failed to create test backup dir: %v", err)
	}

	metadata := metafile.MetafileContent{
		Version:      "test-version",
		TimestampUTC: timestampUTC,
		Source:       "/src",
	}

	if err := metafile.Write(backupPath, metadata); err != nil {
		t.Fatalf("failed to write metafile for test backup: %v", err)
	}
}

func TestDetermineBackupsToKeep(t *testing.T) {
	// Arrange: Create a series of backups over time
	now := time.Now()
	allBackups := []metafile.MetafileInfo{
		{RelPathKey: "backup_hourly_1", Metadata: metafile.MetafileContent{TimestampUTC: now.Add(-1 * time.Hour)}},
		{RelPathKey: "backup_hourly_2", Metadata: metafile.MetafileContent{TimestampUTC: now.Add(-2 * time.Hour)}},
		{RelPathKey: "backup_daily_1", Metadata: metafile.MetafileContent{TimestampUTC: now.Add(-25 * time.Hour)}},
		{RelPathKey: "backup_daily_2", Metadata: metafile.MetafileContent{TimestampUTC: now.Add(-50 * time.Hour)}},
		{RelPathKey: "backup_weekly_1", Metadata: metafile.MetafileContent{TimestampUTC: now.Add(-8 * 24 * time.Hour)}},
		{RelPathKey: "backup_weekly_2", Metadata: metafile.MetafileContent{TimestampUTC: now.Add(-16 * 24 * time.Hour)}},
		{RelPathKey: "backup_monthly_1", Metadata: metafile.MetafileContent{TimestampUTC: now.Add(-35 * 24 * time.Hour)}},
		{RelPathKey: "backup_monthly_2", Metadata: metafile.MetafileContent{TimestampUTC: now.Add(-70 * 24 * time.Hour)}},
		{RelPathKey: "backup_yearly_1", Metadata: metafile.MetafileContent{TimestampUTC: now.Add(-400 * 24 * time.Hour)}},
		{RelPathKey: "backup_yearly_2", Metadata: metafile.MetafileContent{TimestampUTC: now.Add(-800 * 24 * time.Hour)}},
		{RelPathKey: "backup_old_1", Metadata: metafile.MetafileContent{TimestampUTC: now.Add(-1200 * 24 * time.Hour)}},
		{RelPathKey: "backup_old_2", Metadata: metafile.MetafileContent{TimestampUTC: now.Add(-1600 * 24 * time.Hour)}},
	}

	policy := config.RetentionPolicyConfig{
		Enabled: true,
		Hours:   2,
		Days:    2,
		Weeks:   2,
		Months:  2,
		Years:   2,
	}

	run := &retentionRun{
		backups:         allBackups,
		retentionPolicy: policy,
	}

	// Act
	kept := run.determineBackupsToKeep()

	// Assert
	if len(kept) != 10 {
		t.Errorf("expected to keep 10 backups, but got %d", len(kept))
	}

	expectedToKeep := []string{
		"backup_hourly_1", "backup_hourly_2",
		"backup_daily_1", "backup_daily_2",
		"backup_weekly_1", "backup_weekly_2",
		"backup_monthly_1", "backup_monthly_2",
		"backup_yearly_1", "backup_yearly_2",
	}
	for _, name := range expectedToKeep {
		if !kept[name] {
			t.Errorf("expected backup %s to be kept, but it was not", name)
		}
	}

	expectedToDelete := []string{"backup_old_1", "backup_old_2"}
	for _, name := range expectedToDelete {
		if kept[name] {
			t.Errorf("expected backup %s to be deleted, but it was kept", name)
		}
	}
}

func TestDetermineBackupsToKeep_Promotion(t *testing.T) {
	// This test ensures that a single backup is "promoted" to fill the highest-priority slot.
	now := time.Now()
	allBackups := []metafile.MetafileInfo{
		{RelPathKey: "kept_hourly", Metadata: metafile.MetafileContent{TimestampUTC: now.Add(-1 * time.Hour)}},
		{RelPathKey: "kept_daily", Metadata: metafile.MetafileContent{TimestampUTC: now.Add(-25 * time.Hour)}},
		{RelPathKey: "kept_weekly", Metadata: metafile.MetafileContent{TimestampUTC: now.Add(-8 * 24 * time.Hour)}},
		{RelPathKey: "kept_monthly", Metadata: metafile.MetafileContent{TimestampUTC: now.Add(-35 * 24 * time.Hour)}},
		{RelPathKey: "kept_yearly", Metadata: metafile.MetafileContent{TimestampUTC: now.Add(-400 * 24 * time.Hour)}},
		{RelPathKey: "to_be_deleted", Metadata: metafile.MetafileContent{TimestampUTC: now.Add(-800 * 24 * time.Hour)}},
	}

	policy := config.RetentionPolicyConfig{
		Hours:  1,
		Days:   1,
		Weeks:  1,
		Months: 1,
		Years:  1,
	}

	run := &retentionRun{
		backups:         allBackups,
		retentionPolicy: policy,
	}

	// Act
	kept := run.determineBackupsToKeep()

	// Assert
	if len(kept) != 5 {
		t.Errorf("expected to keep 5 backups, but got %d", len(kept))
	}
	if !kept["kept_hourly"] || !kept["kept_daily"] || !kept["kept_weekly"] || !kept["kept_monthly"] || !kept["kept_yearly"] {
		t.Error("one of the expected backups was not kept")
	}
	if kept["to_be_deleted"] {
		t.Error("expected 'to_be_deleted' to be deleted, but it was kept")
	}
}

func TestApplyRetentionPolicy(t *testing.T) {
	// Arrange
	tempDir := t.TempDir()
	cfg := config.NewDefault()
	cfg.Paths.TargetBase = tempDir
	cfg.Naming.Prefix = "backup_"
	policy := config.RetentionPolicyConfig{
		Enabled: true,
		Days:    1, // Keep one daily backup
	}

	r := newTestRetentionManager(cfg)
	now := time.Now()

	// Create backups to be kept and deleted
	createTestBackup(t, tempDir, "backup_kept", now.Add(-1*24*time.Hour))
	createTestBackup(t, tempDir, "backup_to_delete", now.Add(-5*24*time.Hour))

	// Create a non-backup directory that should be ignored
	if err := os.Mkdir(filepath.Join(tempDir, "not_a_backup"), 0755); err != nil {
		t.Fatalf("failed to create non-backup dir: %v", err)
	}

	// Act
	err := r.Apply(context.Background(), "test", tempDir, policy, "")
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// Assert
	if _, err := os.Stat(filepath.Join(tempDir, "backup_kept")); err != nil {
		t.Errorf("expected kept backup to exist, but it does not: %v", err)
	}
	if _, err := os.Stat(filepath.Join(tempDir, "backup_to_delete")); !os.IsNotExist(err) {
		t.Errorf("expected old backup to be deleted, but it still exists or another error occurred: %v", err)
	}
	if _, err := os.Stat(filepath.Join(tempDir, "not_a_backup")); err != nil {
		t.Errorf("expected non-backup directory to be ignored, but it was deleted or an error occurred: %v", err)
	}
}

func TestApplyRetentionPolicy_DryRun(t *testing.T) {
	// Arrange
	tempDir := t.TempDir()
	cfg := config.NewDefault()
	cfg.Paths.TargetBase = tempDir
	cfg.Naming.Prefix = "backup_"
	cfg.DryRun = true // Enable Dry Run
	policy := config.RetentionPolicyConfig{
		Enabled: true,
		Days:    1, // Keep one daily backup
	}

	r := newTestRetentionManager(cfg)
	now := time.Now()

	// Create backups to be kept and deleted
	createTestBackup(t, tempDir, "backup_kept", now.Add(-1*24*time.Hour))
	createTestBackup(t, tempDir, "backup_to_delete", now.Add(-5*24*time.Hour))

	// Act
	err := r.Apply(context.Background(), "test", tempDir, policy, "")
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// Assert
	if _, err := os.Stat(filepath.Join(tempDir, "backup_kept")); err != nil {
		t.Errorf("expected kept backup to exist, but it does not: %v", err)
	}
	if _, err := os.Stat(filepath.Join(tempDir, "backup_to_delete")); os.IsNotExist(err) {
		t.Errorf("expected backup_to_delete to still exist in dry run mode, but it was deleted")
	}
}

func TestApplyRetentionPolicy_WorkerCancellation(t *testing.T) {
	// Arrange
	tempDir := t.TempDir()
	cfg := config.NewDefault()
	cfg.Paths.TargetBase = tempDir
	cfg.Naming.Prefix = "backup_"
	// Use 1 worker to serialize execution for predictable cancellation testing
	cfg.Engine.Performance.DeleteWorkers = 1

	policy := config.RetentionPolicyConfig{
		Enabled: true,
		Days:    1, // Keep one daily backup
	}

	r := newTestRetentionManager(cfg)
	now := time.Now()

	// Create 1 backup to keep
	createTestBackup(t, tempDir, "backup_kept", now.Add(-1*24*time.Hour))

	// Create many backups to delete to ensure the loop runs long enough to catch cancellation
	for i := 0; i < 100; i++ {
		createTestBackup(t, tempDir, fmt.Sprintf("backup_delete_%d", i), now.Add(time.Duration(-5*24*time.Hour-time.Duration(i)*time.Hour)))
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel shortly after starting
	go func() {
		time.Sleep(1 * time.Millisecond)
		cancel()
	}()

	// Act
	_ = r.Apply(ctx, "test", tempDir, policy, "")

	// Assert
	if _, err := os.Stat(filepath.Join(tempDir, "backup_kept")); err != nil {
		t.Errorf("expected kept backup to exist, but it does not: %v", err)
	}
}

func TestApplyRetentionPolicy_DisabledOptimization(t *testing.T) {
	// Arrange
	tempDir := t.TempDir()
	// Create a file where a directory is expected.
	// If the manager tries to scan this, it will fail.
	filePath := filepath.Join(tempDir, "not_a_dir")
	if err := os.WriteFile(filePath, []byte("content"), 0644); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	cfg := config.NewDefault()
	r := newTestRetentionManager(cfg)

	// Disabled policy (all zeros)
	policy := config.RetentionPolicyConfig{}

	// Act
	err := r.Apply(context.Background(), "test", filePath, policy, "")

	// Assert
	if err != nil {
		t.Errorf("expected no error due to disabled policy skipping scan, but got: %v", err)
	}
}

func TestFetchSortedBackups(t *testing.T) {
	// Arrange
	tempDir := t.TempDir()
	cfg := config.NewDefault()
	cfg.Naming.Prefix = "backup_"

	now := time.Now()
	backup1Time := now.Add(-10 * time.Hour)
	backup2Time := now.Add(-5 * time.Hour) // Newest
	backup3Time := now.Add(-20 * time.Hour)

	createTestBackup(t, tempDir, "backup_1", backup1Time)
	createTestBackup(t, tempDir, "backup_2_newest", backup2Time)
	createTestBackup(t, tempDir, "backup_3", backup3Time)

	// Create a directory without a metafile that should be ignored
	if err := os.Mkdir(filepath.Join(tempDir, "backup_no_meta"), 0755); err != nil {
		t.Fatalf("failed to create dir without metafile: %v", err)
	}

	r := newTestRetentionManager(cfg)

	// Act
	backups, err := r.fetchSortedBackups(context.Background(), tempDir, "non_existent_current_dir", "test_policy")
	if err != nil {
		t.Fatalf("fetchSortedBackups failed: %v", err)
	}

	// Assert
	if len(backups) != 3 {
		t.Fatalf("expected to find 3 valid backups, but got %d", len(backups))
	}

	// Check that they are sorted newest to oldest
	expectedOrder := []string{"backup_2_newest", "backup_1", "backup_3"}
	for i, relPathKey := range expectedOrder {
		if backups[i].RelPathKey != relPathKey {
			t.Errorf("expected backup at index %d to be %s, but got %s", i, relPathKey, backups[i].RelPathKey)
		}
	}

	expectedTimes := []time.Time{backup2Time, backup1Time, backup3Time}
	for i, ti := range expectedTimes {
		// Truncate to millisecond because some filesystems have lower precision.
		if !backups[i].Metadata.TimestampUTC.Truncate(time.Millisecond).Equal(ti.Truncate(time.Millisecond)) {
			t.Errorf("expected backup time at index %d to be %v, but got %v", i, ti, backups[i].Metadata.TimestampUTC)
		}
	}
}
