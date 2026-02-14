package pathcompression

import (
	"archive/tar"
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/klauspost/compress/flate"
	"github.com/klauspost/compress/zip"
	"github.com/klauspost/compress/zstd"
	"github.com/klauspost/pgzip"
	"github.com/paulschiretz/pgl-backup/pkg/pathcompressionmetrics"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

// maxPreReadSize defines the threshold (4MB). Files smaller than this
// will be read fully into memory to parallelize IO.
const maxPreReadSize = 4 * 1024 * 1024 // TODO add to compression plan and config!!!

// compressor defines the interface for compressing a directory into an archive file.
type compressor interface {
	Compress(ctx context.Context, sourceDir, archivePath string) error
}

// compressMetricWriter wraps an io.Writer and updates metrics on every write.
type compressMetricWriter struct {
	w       io.Writer
	metrics pathcompressionmetrics.Metrics
}

func (mw *compressMetricWriter) Write(p []byte) (n int, err error) {
	n, err = mw.w.Write(p)
	if n > 0 {
		mw.metrics.AddBytesWritten(int64(n))
	}
	return
}

func (mw *compressMetricWriter) Reset(w io.Writer) {
	mw.w = w
}

// newCompressor returns the correct implementation based on the format.
func newCompressor(format Format, level Level, writerPool, bufferPool *sync.Pool, numWorkers int, metrics pathcompressionmetrics.Metrics) (compressor, error) {
	switch format {
	case Zip:
		return &zipCompressor{
			level:         level,
			ioWriterPool:  writerPool,
			ioBufferPool:  bufferPool,
			numZipWorkers: numWorkers,
			metrics:       metrics,
			zipTasksChan:  make(chan *zipTask, numWorkers*4),
			zipErrsChan:   make(chan error, 1),
		}, nil
	case TarGz:
		return &tarCompressor{
			compression:   TarGz,
			level:         level,
			ioWriterPool:  writerPool,
			ioBufferPool:  bufferPool,
			numTarWorkers: numWorkers,
			metrics:       metrics,
			tarTasksChan:  make(chan *tarTask, numWorkers*4),
			tarErrsChan:   make(chan error, 1),
		}, nil
	case TarZst:
		return &tarCompressor{
			compression:   TarZst,
			level:         level,
			ioWriterPool:  writerPool,
			ioBufferPool:  bufferPool,
			numTarWorkers: numWorkers,
			metrics:       metrics,
			tarTasksChan:  make(chan *tarTask, numWorkers*4),
			tarErrsChan:   make(chan error, 1),
		}, nil
	default:
		return nil, fmt.Errorf("unsupported format: %s", format)
	}
}

type zipCompressor struct {
	ioWriterPool *sync.Pool
	ioBufferPool *sync.Pool

	level Level
	mu    sync.Mutex
	zw    *zip.Writer

	metrics pathcompressionmetrics.Metrics

	// ctx is the cancellable context for the entire run.
	ctx context.Context

	src  string
	trgF *os.File

	numZipWorkers int

	// zipWg waits for the zipTaskProducer and zipWorkers to finish processing all zip tasks.
	zipWg sync.WaitGroup

	// zipTasksChan is the channel where the Walker sends pre-processed tasks.
	zipTasksChan chan *zipTask

	// zipErrsChan captures the first critical, unrecoverable error (e.g., walker failure)
	zipErrsChan chan error

	// Pool for flate writers to reduce GC pressure
	zipFlatePool sync.Pool
}

// Wrapper to return flate writer to pool on close
type pooledFlateWriter struct {
	*flate.Writer
	pool *sync.Pool
}

func (w *pooledFlateWriter) Close() error {
	err := w.Writer.Close()
	w.pool.Put(w.Writer)
	return err
}

// ZipTask struct for zip workers
type zipTask struct {
	absSrcPath string
	relPathKey string
	info       os.FileInfo
}

func (c *zipCompressor) Compress(ctx context.Context, absSourcePath, absArchiveFilePath string) (retErr error) {
	// 1. Create Temp File
	// We create it in the same directory as the target to ensure atomic rename.
	var err error

	c.ctx = ctx

	// store the source path
	c.src = absSourcePath

	c.trgF, err = os.CreateTemp(filepath.Dir(absArchiveFilePath), "pgl-backup-*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temp archive: %w", err)
	}
	tempTrgPath := c.trgF.Name()

	// Ensure cleanup on error
	defer func() {
		if retErr != nil {
			c.trgF.Close()         // Ensure closed
			os.Remove(tempTrgPath) // Delete temp file
		}
	}()

	// 2. Write Archive Content
	if err := c.handleZip(); err != nil {
		return err
	}

	// 3. Close explicitly to flush to disk before rename
	if err := c.trgF.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	// 4. Atomic Rename
	if err := os.Rename(tempTrgPath, absArchiveFilePath); err != nil {
		return fmt.Errorf("failed to rename temp archive to final path: %w", err)
	}

	return nil
}

func (c *zipCompressor) handleZip() (retErr error) {

	// Get a Buffer from the pool
	mw := &compressMetricWriter{w: c.trgF, metrics: c.metrics}
	bufWriter := c.ioWriterPool.Get().(*bufio.Writer)
	bufWriter.Reset(mw)
	defer func() {
		bufWriter.Reset(io.Discard)
		c.ioWriterPool.Put(bufWriter)
	}()

	var lvl int
	switch c.level {
	case Fastest:
		lvl = flate.BestSpeed
	case Better:
		lvl = 6 // Good balance
	case Best:
		lvl = flate.BestCompression
	default:
		lvl = flate.DefaultCompression
	}

	// Init the flate pool
	c.zipFlatePool = sync.Pool{
		New: func() interface{} {
			fw, _ := flate.NewWriter(io.Discard, lvl)
			return fw
		},
	}

	c.zw = zip.NewWriter(bufWriter)

	// Optimization: Register compressor using the Pool
	c.zw.RegisterCompressor(zip.Deflate, func(out io.Writer) (io.WriteCloser, error) {
		fw := c.zipFlatePool.Get().(*flate.Writer)
		fw.Reset(out)
		return &pooledFlateWriter{Writer: fw, pool: &c.zipFlatePool}, nil
	})

	// Robust cleanup
	defer func() {
		if err := c.zw.Close(); err != nil && retErr == nil {
			retErr = fmt.Errorf("zip writer close failed: %w", err)
		}
		if err := bufWriter.Flush(); err != nil && retErr == nil {
			retErr = fmt.Errorf("buffer flush failed: %w", err)
		}
	}()

	// 1. Start zipWorkers (Consumers).
	for range c.numZipWorkers {
		c.zipWg.Add(1)
		go c.zipWorker()
	}

	// 2. Start the zipTaskProducer (Producer)
	// This goroutine walks the file tree and feeds paths into 'zipTasks'.
	go c.zipTaskProducer()

	// 3. Wait for all workers to finish processing all tasks.
	c.zipWg.Wait()

	// 4. Check for any errors captured by workers.
	select {
	case err := <-c.zipErrsChan:
		return err
	default:
	}
	return nil
}

func (c *zipCompressor) zipTaskProducer() {
	defer close(c.zipTasksChan)
	walkErr := filepath.WalkDir(c.src, func(absSrcPath string, d os.DirEntry, walkErr error) error {
		select {
		case <-c.ctx.Done():
			return context.Canceled
		default:
		}

		if walkErr != nil {
			return walkErr
		}

		if d.IsDir() {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("failed to get file info for %s: %w", absSrcPath, err)
		}

		relPathKey, err := filepath.Rel(c.src, absSrcPath)
		if err != nil {
			return fmt.Errorf("failed to get relative path for %s: %w", absSrcPath, err)
		}
		relPathKey = util.NormalizePath(relPathKey)

		plog.Notice("ADD", "source", c.src, "file", relPathKey)
		c.metrics.AddEntriesProcessed(1)

		select {
		case c.zipTasksChan <- &zipTask{absSrcPath: absSrcPath, relPathKey: relPathKey, info: info}:
			return nil
		case <-c.ctx.Done():
			return c.ctx.Err()
		}
	})

	if walkErr != nil {
		select {
		case c.zipErrsChan <- walkErr:
		default:
		}
	}
}

func (c *zipCompressor) zipWorker() {
	defer c.zipWg.Done()

	// Use buffer from pool for large file copying
	bufPtr := c.ioBufferPool.Get().(*[]byte)
	defer c.ioBufferPool.Put(bufPtr)

	for t := range c.zipTasksChan {
		select {
		case <-c.ctx.Done():
			return
		case <-c.zipErrsChan:
			return
		default:
		}

		var err error
		if t.info.Mode()&os.ModeSymlink != 0 {
			err = c.writeSymlink(t.absSrcPath, t.relPathKey, t.info)
		} else if t.info.Size() <= maxPreReadSize {
			err = c.writeFilePreRead(t.absSrcPath, t.relPathKey, t.info)
		} else {
			err = c.writeFileStream(t.absSrcPath, t.relPathKey, t.info, *bufPtr)
		}

		if err != nil {
			c.sendErr(err)
			return
		}
	}
}

// Internal helpers
func (c *zipCompressor) writeSymlink(absSrcPath, relPathKey string, info os.FileInfo) error {

	// 1. Parallel: Read the link target
	linkTarget, err := os.Readlink(absSrcPath)
	if err != nil {
		return fmt.Errorf("failed to read link %s: %w", absSrcPath, err)
	}

	// 2. Serial: Lock and Write
	c.mu.Lock()
	defer c.mu.Unlock()

	c.metrics.AddBytesRead(int64(len(linkTarget)))
	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return fmt.Errorf("failed to create zip header for %s: %w", relPathKey, err)
	}
	header.Name = relPathKey
	header.Method = zip.Store // Symplincs are stored not compressed!

	w, err := c.zw.CreateHeader(header)
	if err != nil {
		return err
	}
	_, err = w.Write([]byte(linkTarget))
	return err
}

func (c *zipCompressor) writeFilePreRead(absSrcPath, relPathKey string, info os.FileInfo) error {

	// 1. Parallel: Read file into memory (the expensive part)
	// Security: TOCTOU check
	fileToZip, err := secureFileOpen(absSrcPath, info)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", absSrcPath, err)
	}
	defer fileToZip.Close()

	// Read All data
	data, err := io.ReadAll(fileToZip)
	fileToZip.Close() // Close explicitly! Do not use defer in a loop.
	if err != nil {
		return fmt.Errorf("failed to read file %s: %w", absSrcPath, err)
	}

	// 2. Serial: Lock and Write
	c.mu.Lock()
	defer c.mu.Unlock()

	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return fmt.Errorf("failed to create zip header for %s: %w", relPathKey, err)
	}
	header.Name = relPathKey
	header.Method = zip.Deflate

	w, err := c.zw.CreateHeader(header)
	if err != nil {
		return fmt.Errorf("failed to write zip header for %s: %w", relPathKey, err)
	}
	_, err = w.Write(data)
	if err == nil {
		c.metrics.AddBytesRead(int64(len(data)))
	}
	return err
}

func (c *zipCompressor) writeFileStream(absSrcPath, relPathKey string, info os.FileInfo, buf []byte) error {
	// 1. Parallel Prep: Open the file (pre-lock)
	// Security: TOCTOU check
	fileToZip, err := secureFileOpen(absSrcPath, info)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", absSrcPath, err)
	}
	defer fileToZip.Close()

	// 2. Serial: Lock and Write
	c.mu.Lock()
	defer c.mu.Unlock()

	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return fmt.Errorf("failed to create zip header for %s: %w", relPathKey, err)
	}
	header.Name = relPathKey
	header.Method = zip.Deflate

	w, err := c.zw.CreateHeader(header)
	if err != nil {
		return fmt.Errorf("failed to write zip header for %s: %w", relPathKey, err)
	}

	n, err := io.CopyBuffer(w, fileToZip, buf)
	c.metrics.AddBytesRead(int64(n))
	return err
}

func (c *zipCompressor) sendErr(err error) {
	select {
	case c.zipErrsChan <- err:
	default:
	}
}

type tarCompressor struct {
	ioWriterPool *sync.Pool
	ioBufferPool *sync.Pool
	compression  Format
	level        Level
	mu           sync.Mutex
	tw           *tar.Writer
	metrics      pathcompressionmetrics.Metrics

	// ctx is the cancellable context for the entire run.
	ctx context.Context

	src  string
	trgF *os.File

	numTarWorkers int

	// tarWg waits for the tarTaskProducer and tarWorkers to finish processing all tar tasks.
	tarWg sync.WaitGroup

	// tarTasksChan is the channel where the Walker sends pre-processed tasks.
	tarTasksChan chan *tarTask

	// tarErrsChan captures the first critical, unrecoverable error (e.g., walker failure)
	tarErrsChan chan error
}

// tarTask struct for tar workers
type tarTask struct {
	absSrcPath string
	relPathKey string
	info       os.FileInfo
}

func (c *tarCompressor) Compress(ctx context.Context, absSourcePath, absArchiveFilePath string) (retErr error) {
	// 1. Create Temp File
	var err error

	c.ctx = ctx
	c.src = absSourcePath

	c.trgF, err = os.CreateTemp(filepath.Dir(absArchiveFilePath), "pgl-backup-*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temp archive: %w", err)
	}
	tempTrgPath := c.trgF.Name()

	// Ensure cleanup on error
	defer func() {
		if retErr != nil {
			c.trgF.Close()
			os.Remove(tempTrgPath)
		}
	}()

	// 2. Write Archive Content
	if err := c.handleTar(); err != nil {
		return err
	}

	// 3. Close explicitly
	if err := c.trgF.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	// 4. Atomic Rename
	if err := os.Rename(tempTrgPath, absArchiveFilePath); err != nil {
		return fmt.Errorf("failed to rename temp archive to final path: %w", err)
	}

	return nil
}

func (c *tarCompressor) handleTar() (retErr error) {

	mw := &compressMetricWriter{w: c.trgF, metrics: c.metrics}
	bufWriter := c.ioWriterPool.Get().(*bufio.Writer)
	bufWriter.Reset(mw)
	defer func() {
		bufWriter.Reset(io.Discard)
		c.ioWriterPool.Put(bufWriter)
	}()

	var compressedWriter io.WriteCloser
	if c.compression == TarZst {
		opts := []zstd.EOption{}
		var encoderLevel zstd.EncoderLevel
		switch c.level {
		case Fastest:
			encoderLevel = zstd.SpeedFastest
		case Better:
			encoderLevel = zstd.SpeedBetterCompression
		case Best:
			encoderLevel = zstd.SpeedBestCompression
		default:
			encoderLevel = zstd.SpeedDefault
		}
		opts = append(opts, zstd.WithEncoderLevel(encoderLevel))

		zstdWriter, err := zstd.NewWriter(bufWriter, opts...)
		if err != nil {
			return fmt.Errorf("failed to create zstd writer: %w", err)
		}
		compressedWriter = zstdWriter
	} else {
		var lvl int
		switch c.level {
		case Fastest:
			lvl = pgzip.BestSpeed
		case Better:
			lvl = 6 // Good balance
		case Best:
			lvl = pgzip.BestCompression
		default:
			lvl = pgzip.DefaultCompression
		}
		pgzipWriter, err := pgzip.NewWriterLevel(bufWriter, lvl)
		if err != nil {
			return fmt.Errorf("failed to create gzip writer: %w", err)
		}
		compressedWriter = pgzipWriter
	}

	tarWriter := tar.NewWriter(compressedWriter)
	c.tw = tarWriter

	// Robust cleanup
	defer func() {
		if err := tarWriter.Close(); err != nil && retErr == nil {
			retErr = fmt.Errorf("tar writer close failed: %w", err)
		}
		if err := compressedWriter.Close(); err != nil && retErr == nil {
			retErr = fmt.Errorf("compressed writer close failed: %w", err)
		}
		if err := bufWriter.Flush(); err != nil && retErr == nil {
			retErr = fmt.Errorf("buffer flush failed: %w", err)
		}
	}()

	// 1. Start tarWorkers (Consumers).
	for range c.numTarWorkers {
		c.tarWg.Add(1)
		go c.tarWorker()
	}

	// 2. Start the tarTaskProducer (Producer)
	// This goroutine walks the file tree and feeds paths into 'tarTasks'.
	go c.tarTaskProducer()

	// 3. Wait for all workers to finish processing all tasks.
	c.tarWg.Wait()

	// 4. Check for any errors captured by workers.
	select {
	case err := <-c.tarErrsChan:
		return err
	default:
	}
	return nil
}

func (c *tarCompressor) tarTaskProducer() {
	defer close(c.tarTasksChan)
	walkErr := filepath.WalkDir(c.src, func(absSrcPath string, d os.DirEntry, walkErr error) error {
		select {
		case <-c.ctx.Done():
			return context.Canceled
		default:
		}

		if walkErr != nil {
			return walkErr
		}

		if d.IsDir() {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("failed to get file info for %s: %w", absSrcPath, err)
		}

		relPathKey, err := filepath.Rel(c.src, absSrcPath)
		if err != nil {
			return fmt.Errorf("failed to get relative path for %s: %w", absSrcPath, err)
		}
		relPathKey = util.NormalizePath(relPathKey)

		plog.Notice("ADD", "source", c.src, "file", relPathKey)
		c.metrics.AddEntriesProcessed(1)

		select {
		case c.tarTasksChan <- &tarTask{absSrcPath: absSrcPath, relPathKey: relPathKey, info: info}:
			return nil
		case <-c.ctx.Done():
			return c.ctx.Err()
		}
	})

	if walkErr != nil {
		select {
		case c.tarErrsChan <- walkErr:
		default:
		}
	}
}

func (c *tarCompressor) tarWorker() {
	defer c.tarWg.Done()
	bufPtr := c.ioBufferPool.Get().(*[]byte)
	defer c.ioBufferPool.Put(bufPtr)

	for t := range c.tarTasksChan {
		select {
		case <-c.ctx.Done():
			return
		case <-c.tarErrsChan:
			return
		default:
		}

		var err error
		if t.info.Mode()&os.ModeSymlink != 0 {
			err = c.writeSymlink(t.absSrcPath, t.relPathKey, t.info)
		} else if t.info.Size() <= maxPreReadSize {
			err = c.writeFilePreRead(t.absSrcPath, t.relPathKey, t.info)
		} else {
			err = c.writeFileStream(t.absSrcPath, t.relPathKey, t.info, *bufPtr)
		}

		if err != nil {
			c.sendErr(err)
			return
		}
	}
}

// Internal helpers
func (c *tarCompressor) writeSymlink(absSrcPath, relPathKey string, info os.FileInfo) error {

	// 1. Parallel: Read the link target
	linkTarget, err := os.Readlink(absSrcPath)
	if err != nil {
		return fmt.Errorf("failed to read link %s: %w", absSrcPath, err)
	}

	// 2. Serial: Lock and Write
	c.mu.Lock()
	defer c.mu.Unlock()

	c.metrics.AddBytesRead(int64(len(linkTarget)))
	header, err := tar.FileInfoHeader(info, linkTarget)
	if err != nil {
		return fmt.Errorf("failed to create tar header for %s: %w", relPathKey, err)
	}
	header.Name = relPathKey
	return c.tw.WriteHeader(header)
}

func (c *tarCompressor) writeFilePreRead(absSrcPath, relPathKey string, info os.FileInfo) error {

	// 1. Parallel: Read file into memory (the expensive part)
	// Security: TOCTOU check
	fileToTar, err := secureFileOpen(absSrcPath, info)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", absSrcPath, err)
	}
	defer fileToTar.Close()

	// Read All data
	data, err := io.ReadAll(fileToTar)
	fileToTar.Close() // Close explicitly! Do not use defer in a loop.
	if err != nil {
		return fmt.Errorf("failed to read file %s: %w", absSrcPath, err)
	}

	// 2. Serial: Lock and Write
	c.mu.Lock()
	defer c.mu.Unlock()

	header, err := tar.FileInfoHeader(info, "")
	if err != nil {
		return fmt.Errorf("failed to create tar header for %s: %w", relPathKey, err)
	}
	header.Name = relPathKey

	if err := c.tw.WriteHeader(header); err != nil {
		return fmt.Errorf("failed to write tar header for %s: %w", relPathKey, err)
	}
	_, err = c.tw.Write(data)
	if err == nil {
		c.metrics.AddBytesRead(int64(len(data)))
	}
	return err
}

func (c *tarCompressor) writeFileStream(absSrcPath, relPathKey string, info os.FileInfo, buf []byte) error {

	// 1. Parallel Prep: Open the file (pre-lock)
	// Security: TOCTOU check
	fileToTar, err := secureFileOpen(absSrcPath, info)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", absSrcPath, err)
	}
	defer fileToTar.Close()

	// 2. Serial: Lock and Write
	c.mu.Lock()
	defer c.mu.Unlock()

	header, err := tar.FileInfoHeader(info, "")
	if err != nil {
		return fmt.Errorf("failed to create tar header for %s: %w", relPathKey, err)
	}
	header.Name = relPathKey

	if err := c.tw.WriteHeader(header); err != nil {
		return fmt.Errorf("failed to write tar header for %s: %w", relPathKey, err)
	}

	n, err := io.CopyBuffer(c.tw, fileToTar, buf)
	c.metrics.AddBytesRead(int64(n))
	return err
}

func (c *tarCompressor) sendErr(err error) {
	select {
	case c.tarErrsChan <- err:
	default:
	}
}

// secureFileOpen verifies that the file at path is the same one we expected(TOCTOU check).
// Ensure the file we opened is the same one we discovered in the walk.
// This prevents attacks where a file is swapped for a symlink after discovery.
func secureFileOpen(absFilePath string, expected os.FileInfo) (*os.File, error) {
	f, err := os.Open(absFilePath)
	if err != nil {
		return nil, err
	}

	openedInfo, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to stat opened file: %w", err)
	}

	if !os.SameFile(expected, openedInfo) {
		f.Close()
		return nil, fmt.Errorf("file changed during backup (TOCTOU): %s", absFilePath)
	}

	return f, nil
}
