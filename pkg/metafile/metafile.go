package metafile

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"pixelgardenlabs.io/pgl-backup/pkg/config"
	"pixelgardenlabs.io/pgl-backup/pkg/util"
)

// MetafileContent holds the contents of the metadatafile.
type MetafileContent struct {
	Version             string    `json:"version"`
	TimestampUTC        time.Time `json:"timestampUTC"`
	Mode                string    `json:"mode"`
	Source              string    `json:"source"`
	IsCompressed        bool      `json:"isCompressed,omitempty"`
	CompressionAttempts int       `json:"compressionAttempts,omitempty"`
}

// Write creates and writes the .pgl-backup.meta.json file into a given directory.
func Write(dirPath string, content MetafileContent) error {
	metaFilePath := filepath.Join(dirPath, config.MetaFileName)
	jsonData, err := json.MarshalIndent(content, "", "  ")
	if err != nil {
		return fmt.Errorf("could not marshal meta data: %w", err)
	}

	// Use group-writable permissions for the metafile. Unlike the top-level config and lock files,
	// the metafile is part of the backup data itself. In multi-user environments, allowing
	// group members to write to backup contents is a common and useful scenario.
	// The config/lock files remain user-only writable for security.
	if err := os.WriteFile(metaFilePath, jsonData, util.UserGroupWritableFilePerms); err != nil {
		return fmt.Errorf("could not write meta file %s: %w", metaFilePath, err)
	}

	return nil
}

// Read opens and parses the .pgl-backup.meta.json file in a given directory.
// It returns the parsed metadata or an error if the file cannot be read or parsed.
func Read(dirPath string) (MetafileContent, error) {
	metaFilePath := filepath.Join(dirPath, config.MetaFileName)
	metaFile, err := os.Open(metaFilePath)
	if err != nil {
		// Note: os.IsNotExist errors are handled by the caller.
		return MetafileContent{}, err // Return the original error so os.IsNotExist works.
	}
	defer metaFile.Close()

	var content MetafileContent
	decoder := json.NewDecoder(metaFile)
	if err := decoder.Decode(&content); err != nil {
		return MetafileContent{}, fmt.Errorf("could not parse metafile %s: %w. It may be corrupt", metaFilePath, err)
	}

	return content, nil
}
