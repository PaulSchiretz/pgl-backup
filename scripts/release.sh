#!/bin/bash

# exit immediately if a command exits with a non-zero status.
set -e

# --- Configuration ---
# The path to your main package
PROJECT_ROOT=$(dirname "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")")
MAIN_PACKAGE_PATH="github.com/paulschiretz/pgl-backup/cmd/pgl-backup"
# The name of your application binary
BINARY_NAME="pgl-backup"
# The directory to output release artifacts
RELEASE_DIR="$PROJECT_ROOT/releases/$VERSION"

# --- Helper Functions ---
function write_usage() {
  echo "Usage: ./release.sh <version> [--dry-run]"
  echo "Example: ./release.sh v1.0.0"
  echo "  --dry-run: Performs the build and packaging but skips the final git tag push."
  exit 1
}

function write_header() {
  echo "---"
  echo "🚀 $1"
  echo "---"
}

# --- Script Start ---

# 1. Validate arguments
VERSION=""
DRY_RUN=false

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true ;;
    -h|--help) write_usage; exit 0 ;;
    -*) echo "Unknown option: $1"; write_usage; exit 1 ;;
    *)
      if [ -z "$VERSION" ]; then
        VERSION=$1
      else
        echo "Unknown argument: $1"; write_usage; exit 1
      fi ;;
  esac
  shift # Consume the argument for the next iteration
done

if [ -z "$VERSION" ]; then
  echo "Error: Version argument is required."
  write_usage
fi

# Now that we have the version, we can define the release directory.
# This is placed here because the VERSION variable is needed.
RELEASE_DIR="$PROJECT_ROOT/releases/$VERSION"

# Regex to validate semantic versioning with a 'v' prefix (e.g., vX.Y.Z or vX.Y.Z-rc.N)
if ! [[ "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?$ ]]; then
  echo "Error: Invalid version format. Expected format: vX.Y.Z or vX.Y.Z-rc.N"
  write_usage
fi

write_header "Starting release process for pgl-backup version $VERSION"

# 2. Pre-flight checks
write_header "Running pre-flight checks"

# Check for required tools
if ! command -v git &> /dev/null; then
    echo "Error: Git is not found in your PATH. Please install Git and try again."
    exit 1
fi
if ! command -v go &> /dev/null; then
    echo "Error: Go is not found in your PATH. Please install Go and try again."
    exit 1
fi
if ! command -v zip &> /dev/null; then
    echo "Error: zip is not found in your PATH. Please install zip and try again."
    exit 1
fi
if ! command -v tar &> /dev/null; then
    echo "Error: tar is not found in your PATH. Please install tar and try again."
    exit 1
fi

# Check for uncommitted changes
if ! git diff-index --quiet HEAD --; then
  echo "Error: You have uncommitted changes. Please commit or stash them before releasing."
  exit 1
fi

# Check if on main/master branch
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [[ "$CURRENT_BRANCH" != "main" && "$CURRENT_BRANCH" != "master" ]]; then
  echo "Warning: You are not on the 'main' or 'master' branch. Current branch is '$CURRENT_BRANCH'."
  read -p "Continue anyway? (y/N): " -n 1 -r
  echo
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Release cancelled."
    exit 1
  fi
fi

echo "✅ Pre-flight checks passed."

# 3. Clean and prepare release directory
write_header "Preparing release directory"
if [ "$DRY_RUN" = true ]; then
    echo "[DRY RUN] Would remove and recreate the '$RELEASE_DIR' directory."
else
    rm -rf "$RELEASE_DIR"
    mkdir -p "$RELEASE_DIR"
fi
echo "✅ Cleaned and created '$RELEASE_DIR' directory."

# 4. Cross-compile for target platforms
write_header "Cross-compiling binaries"

# Define target platforms: GOOS/GOARCH
PLATFORMS=("windows/amd64" "linux/amd64" "linux/arm64" "darwin/amd64" "darwin/arm64" "freebsd/amd64")

# The ldflags variable for injecting the version
LDFLAGS="-s -w -X main.version=$VERSION"

for platform in "${PLATFORMS[@]}"; do
  # Split the platform string into OS and architecture
  GOOS=${platform%/*}
  GOARCH=${platform#*/}
  
  # Set the output binary name, adding .exe for Windows
  OUTPUT_NAME="$BINARY_NAME"
  if [ "$GOOS" = "windows" ]; then
    OUTPUT_NAME+=".exe"
  fi

  echo "Building for $GOOS/$GOARCH..."
  
  if [ "$DRY_RUN" = true ]; then
    echo "[DRY RUN] Would build and archive for $GOOS/$GOARCH."
  else
    # Execute the build command
    env CGO_ENABLED=0 GOOS=$GOOS GOARCH=$GOARCH go build -trimpath -ldflags="$LDFLAGS" -o "$RELEASE_DIR/$OUTPUT_NAME" "$MAIN_PACKAGE_PATH"

    # Create an archive for the binary
    ARCHIVE_PATH="$RELEASE_DIR/${BINARY_NAME}_${VERSION}_${GOOS}_${GOARCH}"

    # Create and populate a temporary staging directory for robust path handling.
    STAGING_DIR="$RELEASE_DIR/staging_${GOOS}_${GOARCH}"
    mkdir -p "$STAGING_DIR"
    cp "$RELEASE_DIR/$OUTPUT_NAME" "$STAGING_DIR/"
    cp "$PROJECT_ROOT/LICENSE" "$STAGING_DIR/LICENSE.txt"
    cp "$PROJECT_ROOT/README.md" "$STAGING_DIR/README.txt"
    cp "$PROJECT_ROOT/NOTICE" "$STAGING_DIR/NOTICE.txt"

    # Archive the contents of the staging directory.
    if [ "$GOOS" = "windows" ] || [ "$GOOS" = "darwin" ]; then
      (cd "$STAGING_DIR" && zip -r "${ARCHIVE_PATH}.zip" . > /dev/null)
    else
      tar -czf "${ARCHIVE_PATH}.tar.gz" -C "$STAGING_DIR" .
    fi
    # Clean up the staging directory and the original binary.
    rm -rf "$STAGING_DIR"
    rm "$RELEASE_DIR/$OUTPUT_NAME"
  fi
done

echo "✅ All platforms built and archived successfully."

# 5. Create Source Code Archives
write_header "Creating source code archives"
if [ "$DRY_RUN" = true ]; then
    echo "[DRY RUN] Would create source code archives."
else
    SOURCE_ZIP_PATH="$RELEASE_DIR/${BINARY_NAME}_${VERSION}_source.zip"
    SOURCE_TAR_PATH="$RELEASE_DIR/${BINARY_NAME}_${VERSION}_source.tar.gz"
    git archive --format=zip --output="$SOURCE_ZIP_PATH" HEAD
    git archive --format=tar.gz --output="$SOURCE_TAR_PATH" HEAD
    echo "✅ Source code archives created."
fi

# 6. Generate Checksums
write_header "Generating checksums"
pushd "$RELEASE_DIR" > /dev/null

# Use the appropriate command for the OS (sha256sum on Linux, shasum on macOS)
if command -v sha256sum &> /dev/null; then
    # The output format `checksum  filename` is standard and verifiable.
    sha256sum *.zip *.tar.gz > "checksums.txt"
elif command -v shasum &> /dev/null; then
    shasum -a 256 *.zip *.tar.gz > "checksums.txt"
else
    echo "⚠️ Warning: Could not find sha256sum or shasum. Checksum file will not be generated."
fi

echo "✅ Checksums generated in '$RELEASE_DIR/checksums.txt'."
popd > /dev/null

# 7. Create and push git tag
write_header "Tagging release in git"
if [ "$DRY_RUN" = true ]; then
    echo "✅ [DRY RUN] Skipping git tag creation and push."
    # Ensure local tag doesn't exist from a previous dry run
    git tag --delete "$VERSION" 2>/dev/null || true
else
    echo "Creating git tag '$VERSION'..."
    git tag "$VERSION"
    echo "Pushing tag to remote..."
    git push origin "$VERSION"
    echo "✅ Git tag '$VERSION' created and pushed."
fi

write_header "Release $VERSION is complete! Artifacts are in the '$RELEASE_DIR' directory."
