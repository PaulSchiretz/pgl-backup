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
	"github.com/paulschiretz/pgl-backup/pkg/limiter"
	"github.com/paulschiretz/pgl-backup/pkg/pathcompressionmetrics"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

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
func newCompressor(format Format, level Level, ioBufferPool *sync.Pool, ioBufferSize int64, readAheadLimiter *limiter.Memory, readAheadLimitSize int64, numWorkers int, metrics pathcompressionmetrics.Metrics) (compressor, error) {
	switch format {
	case Zip:
		return &zipCompressor{
			level:              level,
			numZipWorkers:      numWorkers,
			metrics:            metrics,
			zipTasksChan:       make(chan *zipTask, numWorkers*4),
			zipErrsChan:        make(chan error, 1),
			ioBufferPool:       ioBufferPool,
			ioBufferSize:       ioBufferSize,
			readAheadLimiter:   readAheadLimiter,
			readAheadLimitSize: readAheadLimitSize,
			zipTaskPool: &sync.Pool{
				New: func() any {
					return new(zipTask)
				},
			},
		}, nil
	case TarGz:
		return &tarCompressor{
			compression:        TarGz,
			level:              level,
			numTarWorkers:      numWorkers,
			metrics:            metrics,
			tarTasksChan:       make(chan *tarTask, numWorkers*4),
			tarErrsChan:        make(chan error, 1),
			ioBufferPool:       ioBufferPool,
			ioBufferSize:       ioBufferSize,
			readAheadLimiter:   readAheadLimiter,
			readAheadLimitSize: readAheadLimitSize,
			tarTaskPool: &sync.Pool{
				New: func() any {
					return new(tarTask)
				},
			},
		}, nil
	case TarZst:
		return &tarCompressor{
			compression:        TarZst,
			level:              level,
			numTarWorkers:      numWorkers,
			metrics:            metrics,
			tarTasksChan:       make(chan *tarTask, numWorkers*4),
			tarErrsChan:        make(chan error, 1),
			ioBufferPool:       ioBufferPool,
			ioBufferSize:       ioBufferSize,
			readAheadLimiter:   readAheadLimiter,
			readAheadLimitSize: readAheadLimitSize,
			tarTaskPool: &sync.Pool{
				New: func() any {
					return new(tarTask)
				},
			},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported format: %s", format)
	}
}

type zipCompressor struct {
	// Read buffer pool
	ioBufferPool *sync.Pool
	ioBufferSize int64

	// Memory limiter for file readahead
	readAheadLimiter   *limiter.Memory
	readAheadLimitSize int64

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

	// Pool for zipTask structs to reduce GC pressure
	zipTaskPool *sync.Pool
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
	bufWriter := bufio.NewWriterSize(mw, int(c.ioBufferSize))

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

		task := c.zipTaskPool.Get().(*zipTask)
		task.absSrcPath = absSrcPath
		task.relPathKey = relPathKey
		task.info = info

		select {
		case c.zipTasksChan <- task:
			return nil
		case <-c.ctx.Done():
			c.zipTaskPool.Put(task) // Return to pool on cancellation
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

	// Dereference the bufPtr to get the actual slice
	buf := *bufPtr
	// IMPORTANT: Ensure length is maxed out before use
	// In case someone messed with it, always reset len to cap
	// strictly for io.Read/Copy purposes.
	buf = buf[:cap(buf)]

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.zipErrsChan:
			return
		case t, ok := <-c.zipTasksChan:
			if !ok {
				return // Channel closed
			}

			// Use an IIFE to ensure the task is always returned to the pool.
			func() {
				defer c.zipTaskPool.Put(t)

				var err error
				if t.info.Mode()&os.ModeSymlink != 0 {
					err = c.writeSymlink(t.absSrcPath, t.relPathKey, t.info)
				} else {
					fSize := t.info.Size()
					// Attempt to acquire readahead budget for this file
					if c.readAheadLimiter.TryAcquire(fSize) {
						// Happy Path: We have budget. Read fully to RAM.
						// We wrap this in a func to ensure Release happens immediately after processing
						err = func() error {
							defer c.readAheadLimiter.Release(fSize)
							return c.writeFileBuffered(t.absSrcPath, t.relPathKey, t.info)
						}()
					} else {
						// Fallback: Budget full or file too big. Stream serially.
						// This holds the lock longer but uses minimal RAM.
						err = c.writeFileStreamed(t.absSrcPath, t.relPathKey, t.info, buf)
					}
				}

				if err != nil {
					c.sendErr(err)
					// The return here is from the anonymous func, not zipWorker.
					// The worker will continue to the next loop iteration unless
					// the error channel is read, which will break the outer loop.
				}
			}()
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

func (c *zipCompressor) writeFileBuffered(absSrcPath, relPathKey string, info os.FileInfo) error {

	// 1. Parallel: Read file into memory (the expensive part)
	// Security: TOCTOU check
	fileToZip, err := secureFileOpen(absSrcPath, info)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", absSrcPath, err)
	}
	// We only defer if we plan to return early.
	// If we close manually later, we can rely on the deferred call
	// to be a "safety net" (no-op if already closed).
	defer fileToZip.Close()

	// Read All data
	fSize := info.Size()
	data := make([]byte, fSize)
	_, err = io.ReadFull(fileToZip, data)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %w", absSrcPath, err)
	}

	// Check the error on the explicit close
	if err := fileToZip.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	// Prepare the header data
	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return fmt.Errorf("failed to create zip header for %s: %w", relPathKey, err)
	}
	header.Name = relPathKey
	header.Method = zip.Deflate

	// 2. Serial: Lock and Write
	c.mu.Lock()
	defer c.mu.Unlock()

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

func (c *zipCompressor) writeFileStreamed(absSrcPath, relPathKey string, info os.FileInfo, buf []byte) error {
	// 1. Parallel Prep: Open the file (pre-lock)
	// Security: TOCTOU check
	fileToZip, err := secureFileOpen(absSrcPath, info)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", absSrcPath, err)
	}
	defer fileToZip.Close()

	// Prepare the header data
	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return fmt.Errorf("failed to create zip header for %s: %w", relPathKey, err)
	}
	header.Name = relPathKey
	header.Method = zip.Deflate

	// 2. Serial: Lock and Write
	c.mu.Lock()
	defer c.mu.Unlock()

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
	// Read buffer pool
	ioBufferPool *sync.Pool
	ioBufferSize int64

	// Memory limiter for file readahead
	readAheadLimiter   *limiter.Memory
	readAheadLimitSize int64

	compression Format
	level       Level
	mu          sync.Mutex
	tw          *tar.Writer
	metrics     pathcompressionmetrics.Metrics

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

	// Pool for tarTask structs to reduce GC pressure
	tarTaskPool *sync.Pool
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
	bufWriter := bufio.NewWriterSize(mw, int(c.ioBufferSize))

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

		task := c.tarTaskPool.Get().(*tarTask)
		task.absSrcPath = absSrcPath
		task.relPathKey = relPathKey
		task.info = info

		select {
		case c.tarTasksChan <- task:
			return nil
		case <-c.ctx.Done():
			c.tarTaskPool.Put(task) // Return to pool on cancellation
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

	// Dereference the bufPtr to get the actual slice
	buf := *bufPtr
	// IMPORTANT: Ensure length is maxed out before use
	// In case someone messed with it, always reset len to cap
	// strictly for io.Read/Copy purposes.
	buf = buf[:cap(buf)]

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.tarErrsChan:
			return
		case t, ok := <-c.tarTasksChan:
			if !ok {
				return // Channel closed
			}

			// Use an IIFE to ensure the task is always returned to the pool.
			func() {
				defer c.tarTaskPool.Put(t)

				var err error
				if t.info.Mode()&os.ModeSymlink != 0 {
					err = c.writeSymlink(t.absSrcPath, t.relPathKey, t.info)
				} else {
					fSize := t.info.Size()
					// Attempt to acquire readahead budget for this file
					if c.readAheadLimiter.TryAcquire(fSize) {
						// Happy Path: We have budget. Read fully to RAM.
						// We wrap this in a func to ensure Release happens immediately after processing
						err = func() error {
							defer c.readAheadLimiter.Release(fSize)
							return c.writeFileBuffered(t.absSrcPath, t.relPathKey, t.info)
						}()
					} else {
						// Fallback: Budget full or file too big. Stream serially.
						// This holds the lock longer but uses minimal RAM.
						err = c.writeFileStreamed(t.absSrcPath, t.relPathKey, t.info, buf)
					}
				}
				if err != nil {
					c.sendErr(err)
					// The return here is from the anonymous func, not tarWorker.
					// The worker will continue to the next loop iteration unless
					// the error channel is read, which will break the outer loop.
				}
			}()
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

func (c *tarCompressor) writeFileBuffered(absSrcPath, relPathKey string, info os.FileInfo) error {

	// 1. Parallel: Read file into memory (the expensive part)
	// Security: TOCTOU check
	fileToTar, err := secureFileOpen(absSrcPath, info)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", absSrcPath, err)
	}
	// We only defer if we plan to return early.
	// If we close manually later, we can rely on the deferred call
	// to be a "safety net" (no-op if already closed).
	defer fileToTar.Close()

	// Read All data
	fSize := info.Size()
	data := make([]byte, fSize)
	_, err = io.ReadFull(fileToTar, data)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %w", absSrcPath, err)
	}

	// Check the error on the explicit close
	if err := fileToTar.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	// Prepare the header data
	header, err := tar.FileInfoHeader(info, "")
	if err != nil {
		return fmt.Errorf("failed to create tar header for %s: %w", relPathKey, err)
	}
	header.Name = relPathKey

	// 2. Serial: Lock and Write
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.tw.WriteHeader(header); err != nil {
		return fmt.Errorf("failed to write tar header for %s: %w", relPathKey, err)
	}
	_, err = c.tw.Write(data)
	if err == nil {
		c.metrics.AddBytesRead(int64(len(data)))
	}
	return err
}

func (c *tarCompressor) writeFileStreamed(absSrcPath, relPathKey string, info os.FileInfo, buf []byte) error {

	// 1. Parallel Prep: Open the file (pre-lock)
	// Security: TOCTOU check
	fileToTar, err := secureFileOpen(absSrcPath, info)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", absSrcPath, err)
	}
	defer fileToTar.Close()

	// Prepare the header data
	header, err := tar.FileInfoHeader(info, "")
	if err != nil {
		return fmt.Errorf("failed to create tar header for %s: %w", relPathKey, err)
	}
	header.Name = relPathKey

	// 2. Serial: Lock and Write
	c.mu.Lock()
	defer c.mu.Unlock()

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

	// 1. Check if it's the same physical file (Inode check)
	if !os.SameFile(expected, openedInfo) {
		f.Close()
		return nil, fmt.Errorf("file changed during backup (TOCTOU): %s", absFilePath)
	}

	// 2. Check if the size changed (Tar Header Integrity check)
	// If you already calculated the Tar header based on 'expected',
	// a size change will corrupt the archive.
	if openedInfo.Size() != expected.Size() {
		f.Close()
		return nil, fmt.Errorf("file size changed during backup: %s", absFilePath)
	}

	return f, nil
}
