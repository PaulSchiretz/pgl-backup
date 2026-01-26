package metafile

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// MetaFileName is the name of the backup metadata file.
const MetaFileName = ".pgl-backup.meta.json"

// MetafileInfo holds the parsed metadata and rel directory path of a backup found on disk.
type MetafileInfo struct {
	RelPathKey string // Normalized, forward-slash and maybe otherwise modified key. NOT for direct FS access.
	Metadata   MetafileContent
}

// MetafileContent holds the contents of the metadatafile.
type MetafileContent struct {
	Version           string    `json:"version"`
	UUID              string    `json:"uuid"`
	TimestampUTC      time.Time `json:"timestampUTC"`
	Mode              string    `json:"mode"`
	IsCompressed      bool      `json:"isCompressed,omitempty"`
	CompressionFormat string    `json:"compressionFormat,omitempty"`
}

// Write creates and writes the .pgl-backup.meta.json file into a given directory.
func Write(dirPath string, content *MetafileContent) error {
	metaFilePath := filepath.Join(dirPath, MetaFileName)
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
	metaFilePath := filepath.Join(dirPath, MetaFileName)
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
