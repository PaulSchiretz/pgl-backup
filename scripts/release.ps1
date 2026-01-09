<#
.SYNOPSIS
  Builds and packages the pgl-backup application for multiple platforms.

.DESCRIPTION
  This script automates the release process. It performs the following steps:
  1. Validates the version string.
  2. Performs pre-flight checks (clean git state, correct branch).
  3. Cleans and creates a 'release' directory.
  4. Cross-compiles the Go application for Windows, Linux, macOS, and FreeBSD.
  5. Archives the binaries into .zip (for Windows) and .tar.gz (for others) files.
  6. Generates a SHA256 checksums.txt file for all artifacts.
  7. Creates and pushes a git tag for the release.

.PARAMETER Version
  The semantic version for the release, prefixed with 'v' (e.g., vX.Y.Z or vX.Y.Z-rc.N).

.EXAMPLE
  .\release.ps1 -Version v1.0.0
  Runs the full release process for version 1.0.0.

.NOTES
  Requires Git and Go to be in the system's PATH.
  The 'tar' command is available by default on modern Windows 10/11.

  How to Use:
  1. Open a PowerShell terminal.

  2. You may need to adjust your execution policy to run local scripts.
     You can do this for the current session by running:
     Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

  3. Navigate to the root of your project and run the script with a version number:
     .\scripts\release.ps1 -Version v1.0.0

  The script will then run natively on Windows, producing the release artifacts.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, HelpMessage = "The semantic version for the release (e.g., v1.0.0).")]
    [ValidatePattern('^v\d+\.\d+\.\d+(-[a-zA-Z0-9.]+)?$')]
    [string]$Version,

    [Parameter(Mandatory = $false, HelpMessage = "If specified, the script will show what it would do without executing.")]
    [switch]$DryRun
)

# Stop script on any error
$ErrorActionPreference = 'Stop'

# --- Configuration ---
# Get the project root directory (the parent of the script's directory)
$ProjectRoot = Split-Path -Path $PSScriptRoot -Parent
$MainPackagePath = "github.com/paulschiretz/pgl-backup/cmd/pgl-backup"
$BinaryName = "pgl-backup"
$ReleaseDir = Join-Path -Path $ProjectRoot -ChildPath "releases\$Version"

# --- Helper Functions ---
function Write-Header {
    param([string]$Message)
    Write-Host "---" -ForegroundColor Cyan
    Write-Host "🚀 $($Message.ToUpper())" -ForegroundColor Cyan
    Write-Host "---" -ForegroundColor Cyan
}

# --- Script Start ---

Write-Header "Starting release process for pgl-backup version $Version"

# 1. Pre-flight checks
Write-Header "Running pre-flight checks"

# Check for required tools
if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
    Write-Error "Git is not found in your PATH. Please install Git and try again."
    exit 1
}
if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
    Write-Error "Go is not found in your PATH. Please install Go and try again."
    exit 1
}
if (-not (Get-Command tar -ErrorAction SilentlyContinue)) {
    Write-Error "The 'tar' command is not found in your PATH. It is required for packaging non-Windows binaries."
    exit 1
}

# Check for uncommitted changes
$gitStatus = git status --porcelain
if ($gitStatus) {
    Write-Error "You have uncommitted changes. Please commit or stash them before releasing."
    exit 1
}

# Check if on main/master branch
$currentBranch = git rev-parse --abbrev-ref HEAD
if ($currentBranch -ne "main" -and $currentBranch -ne "master") {
    Write-Warning "You are not on the 'main' or 'master' branch. Current branch is '$currentBranch'."
    $response = Read-Host "Continue anyway? (y/N)"
    if ($response -ne 'y') {
        Write-Host "Release cancelled."
        exit 1
    }
}

Write-Host "✅ Pre-flight checks passed." -ForegroundColor Green

# 2. Clean and prepare release directory
Write-Header "Preparing release directory"
if (Test-Path $ReleaseDir) {
    if ($DryRun) {
        Write-Host "[DRY RUN] Would remove directory: $ReleaseDir"
    } else {
        Remove-Item -Path $ReleaseDir -Recurse -Force
    }
}
if ($DryRun) { Write-Host "[DRY RUN] Would create directory: $ReleaseDir" }
else { New-Item -Path $ReleaseDir -ItemType Directory | Out-Null }
Write-Host "✅ Cleaned and created '$ReleaseDir' directory." -ForegroundColor Green

# 3. Cross-compile for target platforms
Write-Header "Cross-compiling binaries"

# Define target platforms: GOOS/GOARCH
$platforms = @(
    "windows/amd64",
    "windows/arm64",
    "linux/amd64",
    "linux/arm64",
    "darwin/amd64",
    "darwin/arm64",
    "freebsd/amd64"
)

# The ldflags variable for injecting the version
$ldflags = "-s -w -X main.appVersion=$Version"

foreach ($platform in $platforms) {
    $parts = $platform.Split('/')
    $GOOS = $parts[0]
    $GOARCH = $parts[1]

    # Set the output binary name, adding .exe for Windows
    $outputName = $BinaryName
    if ($GOOS -eq "windows") {
        $outputName += ".exe"
    }

    Write-Host "Building for $GOOS/$GOARCH..."

    # Execute the build command with temporary environment variables
    if ($DryRun) {
        Write-Host "[DRY RUN] Would build for $GOOS/$GOARCH with command: go build -trimpath -ldflags=`"$ldflags`" -o `"$ReleaseDir/$outputName`" `"$MainPackagePath`""
    } else {
        & {
            $env:GOOS = $GOOS
            $env:GOARCH = $GOARCH
            $env:CGO_ENABLED = "0"
            go build -trimpath -ldflags="$ldflags" -o "$ReleaseDir/$outputName" "$MainPackagePath"
        }
    }

    # Create an archive for the binary
    $archiveName = "${BinaryName}_${Version}_${GOOS}_${GOARCH}"
    if ($DryRun) {
        Write-Host "[DRY RUN] Would create archive for $outputName"
    } else {
        $binaryPath = Join-Path -Path $ReleaseDir -ChildPath $outputName
        $licensePath = Join-Path -Path $ProjectRoot -ChildPath "LICENSE"
        $readmePath = Join-Path -Path $ProjectRoot -ChildPath "README.md"
        $noticePath = Join-Path -Path $ProjectRoot -ChildPath "NOTICE"
        $archivePath = Join-Path -Path $ReleaseDir -ChildPath "$archiveName"

        # Create and populate a temporary staging directory for robust path handling.
        $stagingDirName = "staging_${GOOS}_${GOARCH}"
        $stagingDir = New-Item -Path (Join-Path $ReleaseDir $stagingDirName) -ItemType Directory
        Copy-Item -Path $binaryPath -Destination $stagingDir.FullName
        Copy-Item -Path $licensePath -Destination (Join-Path $stagingDir.FullName "LICENSE.txt")
        Copy-Item -Path $readmePath -Destination (Join-Path $stagingDir.FullName "README.txt")
        Copy-Item -Path $noticePath -Destination (Join-Path $stagingDir.FullName "NOTICE.txt")

        # Archive the contents of the staging directory.
        if ($GOOS -eq "windows") {
            $compressPath = Join-Path -Path $stagingDir.FullName -ChildPath "*"
            Compress-Archive -Path $compressPath -DestinationPath "$archivePath.zip" -Force
        } else {
            tar -czf "$archivePath.tar.gz" -C "$($stagingDir.FullName)" .
        }
        # Clean up the staging directory and the original binary.
        Remove-Item -Path $stagingDir.FullName -Recurse -Force
        Remove-Item -Path $binaryPath
    }
}

Write-Host "✅ All platforms built and archived successfully." -ForegroundColor Green

# 4. Create Source Code Archives
Write-Header "Creating source code archives"
if ($DryRun) {
    Write-Host "[DRY RUN] Would create source code archives."
} else {
    $sourceZipPath = Join-Path -Path $ReleaseDir -ChildPath "${BinaryName}_${Version}_source.zip"
    $sourceTarPath = Join-Path -Path $ReleaseDir -ChildPath "${BinaryName}_${Version}_source.tar.gz"
    git archive --format=zip --output="$sourceZipPath" HEAD
    git archive --format=tar.gz --output="$sourceTarPath" HEAD
    Write-Host "✅ Source code archives created." -ForegroundColor Green
}

# 5. Generate Checksums
Write-Header "Generating checksums"
if ($DryRun) {
    Write-Host "[DRY RUN] Would generate checksums file: $ReleaseDir\checksums.txt"
} else {
    Get-FileHash -Path "$ReleaseDir\*" -Algorithm SHA256 | ForEach-Object { "$($_.Hash.ToLower())  $($_.Path | Split-Path -Leaf)" } | Set-Content "$ReleaseDir\checksums.txt"
    Write-Host "✅ Checksums generated in '$ReleaseDir\checksums.txt'." -ForegroundColor Green
}

# 6. Create and push git tag
Write-Header "Tagging release in git"
if ($DryRun) {
    Write-Host "[DRY RUN] Would create git tag: $Version"
    Write-Host "[DRY RUN] Would push tag to remote with: git push origin $Version"
} else {
    Write-Host "Creating git tag '$Version'..."
    git tag $Version
    Write-Host "Pushing tag to remote..."
    git push origin $Version
}
Write-Host "✅ Git tag '$Version' created and pushed." -ForegroundColor Green

Write-Header "Release $Version is complete! Artifacts are in the '$ReleaseDir' directory."
