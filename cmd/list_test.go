package cmd_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/paulschiretz/pgl-backup/cmd"
	"github.com/paulschiretz/pgl-backup/pkg/config"
	"github.com/paulschiretz/pgl-backup/pkg/metafile"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
)

func TestRunList_Sort(t *testing.T) {
	// Setup directories
	baseDir := t.TempDir()

	// 1. Create Config
	cfg := config.Default()
	if err := config.Generate(baseDir, cfg); err != nil {
		t.Fatalf("Failed to generate config: %v", err)
	}

	// 2. Create Dummy Backups with distinct timestamps
	// Backup A: Oldest
	pathA := filepath.Join(baseDir, "PGL_Backup_Incremental_Archive", "PGL_Backup_2023-01-01")
	if err := os.MkdirAll(filepath.Join(pathA, "PGL_Backup_Content"), 0755); err != nil {
		t.Fatal(err)
	}
	metafile.Write(pathA, &metafile.MetafileContent{
		TimestampUTC: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
		Mode:         "incremental",
		UUID:         "uuid-oldest",
	})

	// Backup B: Middle
	pathB := filepath.Join(baseDir, "PGL_Backup_Incremental_Archive", "PGL_Backup_2023-02-01")
	if err := os.MkdirAll(filepath.Join(pathB, "PGL_Backup_Content"), 0755); err != nil {
		t.Fatal(err)
	}
	metafile.Write(pathB, &metafile.MetafileContent{
		TimestampUTC: time.Date(2023, 2, 1, 0, 0, 0, 0, time.UTC),
		Mode:         "incremental",
		UUID:         "uuid-middle",
	})

	// Backup C: Newest
	pathC := filepath.Join(baseDir, "PGL_Backup_Incremental_Archive", "PGL_Backup_2023-03-01")
	if err := os.MkdirAll(filepath.Join(pathC, "PGL_Backup_Content"), 0755); err != nil {
		t.Fatal(err)
	}
	metafile.Write(pathC, &metafile.MetafileContent{
		TimestampUTC: time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC),
		Mode:         "incremental",
		UUID:         "uuid-newest",
	})

	tests := []struct {
		name          string
		sortFlag      string
		expectedOrder []string // UUIDs in expected order
	}{
		{
			name:          "Sort Desc (Default)",
			sortFlag:      "desc",
			expectedOrder: []string{"uuid-newest", "uuid-middle", "uuid-oldest"},
		},
		{
			name:          "Sort Asc",
			sortFlag:      "asc",
			expectedOrder: []string{"uuid-oldest", "uuid-middle", "uuid-newest"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Capture logs
			var logBuf bytes.Buffer
			plog.SetOutput(&logBuf)
			defer plog.SetOutput(os.Stderr)

			flags := map[string]any{
				"base": baseDir,
				"sort": tc.sortFlag,
			}

			err := cmd.RunList(context.Background(), flags)
			if err != nil {
				t.Fatalf("RunList failed: %v", err)
			}

			output := logBuf.String()

			// Verify order by finding indices of UUIDs in the output string
			lastIndex := -1
			for _, uuid := range tc.expectedOrder {
				idx := strings.Index(output, uuid)
				if idx == -1 {
					t.Errorf("Expected UUID %s not found in output", uuid)
				}
				if idx < lastIndex {
					t.Errorf("UUID %s appeared out of order (index %d < %d)", uuid, idx, lastIndex)
				}
				lastIndex = idx
			}
		})
	}
}
