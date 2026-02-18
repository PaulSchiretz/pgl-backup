package pathcompression

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/klauspost/compress/flate"
	"github.com/klauspost/compress/zip"
	"github.com/paulschiretz/pgl-backup/pkg/limiter"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

type zipCompressor struct {
	// Read buffer pool
	ioBufferPool *sync.Pool
	ioBufferSize int64

	// Memory limiter for file readahead
	readAheadLimiter   *limiter.Memory
	readAheadLimitSize int64

	format Format
	level  Level
	mu     sync.Mutex
	zw     *zip.Writer

	metrics Metrics

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
	zipFlatePool *sync.Pool

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

func newZipCompressor(format Format, level Level, ioBufferPool *sync.Pool, ioBufferSize int64, readAheadLimiter *limiter.Memory, readAheadLimitSize int64, numWorkers int, metrics Metrics) *zipCompressor {

	// Map the compression level
	var lvl int
	switch level {
	case Fastest:
		lvl = flate.BestSpeed
	case Better:
		lvl = 6 // Good balance
	case Best:
		lvl = flate.BestCompression
	default:
		lvl = flate.DefaultCompression
	}

	return &zipCompressor{
		format:             format,
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
		zipFlatePool: &sync.Pool{
			New: func() interface{} {
				fw, _ := flate.NewWriter(io.Discard, lvl)
				return fw
			},
		},
	}
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

	c.zw = zip.NewWriter(bufWriter)

	// Optimization: Register compressor using the Pool
	c.zw.RegisterCompressor(zip.Deflate, func(out io.Writer) (io.WriteCloser, error) {
		fw := c.zipFlatePool.Get().(*flate.Writer)
		fw.Reset(out)
		return &pooledFlateWriter{Writer: fw, pool: c.zipFlatePool}, nil
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
							return c.writeFileBuffered(t.absSrcPath, t.relPathKey, t.info, buf)
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

func (c *zipCompressor) writeFileBuffered(absSrcPath, relPathKey string, info os.FileInfo, buf []byte) error {

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
	var data []byte
	// Optimization: Reuse the worker's buffer if the file fits.
	// This significantly reduces GC pressure for small files in the "fast path".
	if fSize <= int64(len(buf)) {
		data = buf[:fSize]
	} else {
		data = make([]byte, fSize)
	}
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
