package buildinfo

// Version holds the application's version string.
// It's a `var` so it can be set at compile time using ldflags.
// Example: go build -ldflags="-X github.com/paulschiretz/pgl-backup/pkg/buildinfo.Version=1.0.0"
var Version = "dev"

// Name is the canonical name of the application used for logging.
var Name = "PGL-Backup"
