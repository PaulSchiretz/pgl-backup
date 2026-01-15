package pathcompression

import (
	"archive/tar"
	"archive/zip"
	"fmt"
	"io"
	"os"
)

// archiveWriter defines an interface for a generic archive creation utility.
// This allows the main compression logic to be format-agnostic.
type archiveWriter interface {
	// AddFile adds a file from the filesystem to the archive using a pre-calculated relative path.
	AddFile(absPath, relPath string, info os.FileInfo, buf []byte) error
	// AddSymlink adds a symbolic link to the archive.
	AddSymlink(absPath, relPath string, info os.FileInfo) error
	// Close finalizes and closes the archive writer.
	Close() error
}

// zipArchiveWriter implements archiveWriter for .zip files.
type zipArchiveWriter struct {
	zipWriter *zip.Writer
}

func (zw *zipArchiveWriter) AddFile(absSrcPath, relPath string, info os.FileInfo, buf []byte) error {
	// Create a zip header from the file info.
	// This is crucial: zip.Create() uses default permissions and the current time.
	// By using FileInfoHeader, we preserve the original file's permissions (Mode) and modification time.
	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return fmt.Errorf("failed to create zip header for %s: %w", relPath, err)
	}
	header.Name = relPath
	header.Method = zip.Deflate

	writer, err := zw.zipWriter.CreateHeader(header)
	if err != nil {
		return fmt.Errorf("failed to create entry for %s in zip: %w", relPath, err)
	}

	// The file header is created, now open the file on disk to copy its contents.
	fileToZip, err := os.Open(absSrcPath)
	if err != nil {
		return fmt.Errorf("failed to open file %s for zipping: %w", absSrcPath, err)
	}
	defer fileToZip.Close()

	// Copy the file content into the archive writer.
	_, err = io.CopyBuffer(writer, fileToZip, buf)
	if err != nil {
		return fmt.Errorf("failed to copy file %s to zip: %w", absSrcPath, err)
	}
	return nil
}

func (zw *zipArchiveWriter) AddSymlink(absSrcPath, relPath string, info os.FileInfo) error {
	target, err := os.Readlink(absSrcPath)
	if err != nil {
		return fmt.Errorf("failed to read link target for %s: %w", absSrcPath, err)
	}

	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return fmt.Errorf("failed to create zip header for %s: %w", relPath, err)
	}
	header.Name = relPath
	header.Method = zip.Store

	writer, err := zw.zipWriter.CreateHeader(header)
	if err != nil {
		return fmt.Errorf("failed to create entry for %s in zip: %w", relPath, err)
	}
	_, err = writer.Write([]byte(target))
	return err
}

func (zw *zipArchiveWriter) Close() error {
	if err := zw.zipWriter.Close(); err != nil {
		return fmt.Errorf("failed to close zip writer: %w", err)
	}

	return nil
}

// tarArchiveWriter implements archiveWriter for .tar.gz or .tar.zst files.
type tarArchiveWriter struct {
	tarWriter        *tar.Writer
	compressedWriter io.WriteCloser
}

func (tw *tarArchiveWriter) AddFile(absSrcPath, relPath string, info os.FileInfo, buf []byte) error {
	// Create a tar header from the file's info.
	// FileInfoHeader automatically preserves file permissions (Mode) and modification time.
	header, err := tar.FileInfoHeader(info, relPath)
	if err != nil {
		return fmt.Errorf("failed to create tar header for %s: %w", absSrcPath, err)
	}
	// The FileInfoHeader uses the second argument for the link name.
	// We must explicitly set the Name field to the normalized relative path.
	header.Name = relPath

	if err := tw.tarWriter.WriteHeader(header); err != nil {
		return fmt.Errorf("failed to write tar header for %s: %w", relPath, err)
	}

	fileToTar, err := os.Open(absSrcPath)
	if err != nil {
		return fmt.Errorf("failed to open file %s for taring: %w", absSrcPath, err)
	}
	defer fileToTar.Close()

	// Copy the file content into the tar writer.
	_, err = io.CopyBuffer(tw.tarWriter, fileToTar, buf)
	if err != nil {
		return fmt.Errorf("failed to copy file %s to tar: %w", absSrcPath, err)
	}
	return nil
}

func (tw *tarArchiveWriter) AddSymlink(absSrcPath, relPath string, info os.FileInfo) error {
	target, err := os.Readlink(absSrcPath)
	if err != nil {
		return fmt.Errorf("failed to read link target for %s: %w", absSrcPath, err)
	}

	header, err := tar.FileInfoHeader(info, target)
	if err != nil {
		return fmt.Errorf("failed to create tar header for %s: %w", absSrcPath, err)
	}
	header.Name = relPath

	if err := tw.tarWriter.WriteHeader(header); err != nil {
		return fmt.Errorf("failed to write tar header for %s: %w", relPath, err)
	}
	return nil
}

// Close finalizes and closes the tar and underlying compressors in the correct order.
func (tw *tarArchiveWriter) Close() error {
	// Writers must be closed in the correct order: tar first, then compressor.
	if err := tw.tarWriter.Close(); err != nil {
		return err
	}
	return tw.compressedWriter.Close()
}
