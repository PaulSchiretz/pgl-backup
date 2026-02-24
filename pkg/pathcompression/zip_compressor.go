package pathcompression

import (
	"bufio"
	"bytes"
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
	"github.com/paulschiretz/pgl-backup/pkg/pool"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

type zipCompressor struct {
	// Read buffer pool
	ioBufferPool *pool.FixedBufferPool
	ioBufferSize int64

	// Limiter for sequential writes
	readAheadLimiter *limiter.Memory
	readAheadLimit   int64
	readAheadPool    *pool.BucketedBufferPool

	format Format
	level  Level
	dryRun bool
	mu     sync.Mutex
	zw     *zip.Writer

	metrics Metrics

	// ctx is the cancellable context for the entire run.
	ctx context.Context

	src  string
	trgF *os.File

	numZipWorkers int

	// zipWg waits for the zipItemProducer and zipWorkers to finish processing all zip tasks.
	zipWg sync.WaitGroup

	// zipItemsChan is the channel where the Walker sends pre-processed tasks.
	zipItemsChan chan *zipItem

	// zipErrsChan captures the first critical, unrecoverable error (e.g., walker failure)
	zipErrsChan chan error

	// Pool for flate writers to reduce GC pressure
	zipFlatePool *sync.Pool

	// Pool for zipItem structs to reduce GC pressure
	zipItemPool *sync.Pool

	// Pool for compressMetricWriter to reduce GC pressure
	compressMetricWriterPool *sync.Pool

	// Pool for compressMetricReader to reduce GC pressure
	compressMetricReaderPool *sync.Pool
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

// zipItem struct for zip workers
type zipItem struct {
	absSrcPath string
	relPathKey string
	info       os.FileInfo
}

func newZipCompressor(dryRun bool, format Format, level Level, ioBufferPool *pool.FixedBufferPool, ioBufferSize int64, readAheadLimiter *limiter.Memory, readAheadLimit int64, readAheadPool *pool.BucketedBufferPool, numWorkers int, metrics Metrics) *zipCompressor {

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
		dryRun:           dryRun,
		format:           format,
		level:            level,
		numZipWorkers:    numWorkers,
		metrics:          metrics,
		zipItemsChan:     make(chan *zipItem, numWorkers*4),
		zipErrsChan:      make(chan error, 1),
		ioBufferPool:     ioBufferPool,
		ioBufferSize:     ioBufferSize,
		readAheadLimiter: readAheadLimiter,
		readAheadLimit:   readAheadLimit,
		readAheadPool:    readAheadPool,
		zipItemPool: &sync.Pool{
			New: func() any {
				return new(zipItem)
			},
		},
		zipFlatePool: &sync.Pool{
			New: func() interface{} {
				fw, _ := flate.NewWriter(io.Discard, lvl)
				return fw
			},
		},
		compressMetricWriterPool: &sync.Pool{
			New: func() any {
				return &compressMetricWriter{metrics: metrics}
			},
		},
		compressMetricReaderPool: &sync.Pool{
			New: func() any {
				return &compressMetricReader{metrics: metrics}
			},
		},
	}
}

func (c *zipCompressor) Compress(ctx context.Context, absSourcePath, absArchiveFilePath string) (retErr error) {

	if c.dryRun {
		plog.Notice("[DRY RUN] COMPRESS", "source", absSourcePath)
		return nil
	}

	plog.Notice("COMPRESS", "source", absSourcePath)

	var err error
	// Create a cancellable context to ensure the producer goroutine exits
	// if we return early due to an error.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c.ctx = ctx

	// store the source path
	c.src = absSourcePath

	// 1. Create Temp File
	// We create it in the same directory as the target to ensure atomic rename.
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
	mw := c.compressMetricWriterPool.Get().(*compressMetricWriter)
	mw.Reset(c.trgF)
	defer c.compressMetricWriterPool.Put(mw)

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

	// 2. Start the zipItemProducer (Producer)
	// This goroutine walks the file tree and feeds paths into 'zipItems'.
	go c.zipItemProducer()

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

func (c *zipCompressor) zipItemProducer() {
	defer close(c.zipItemsChan)
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

		task := c.zipItemPool.Get().(*zipItem)
		task.absSrcPath = absSrcPath
		task.relPathKey = relPathKey
		task.info = info

		select {
		case c.zipItemsChan <- task:
			return nil
		case <-c.ctx.Done():
			c.zipItemPool.Put(task) // Return to pool on cancellation
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
	bufPtr := c.ioBufferPool.Get()
	defer c.ioBufferPool.Put(bufPtr)

	mr := c.compressMetricReaderPool.Get().(*compressMetricReader)
	defer c.compressMetricReaderPool.Put(mr)

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.zipErrsChan:
			return
		case t, ok := <-c.zipItemsChan:
			if !ok {
				return // Channel closed
			}

			// Use an IIFE to ensure the task is always returned to the pool.
			func() {
				defer c.zipItemPool.Put(t)

				var err error
				if t.info.Mode()&os.ModeSymlink != 0 {
					err = c.writeSymlink(t.absSrcPath, t.relPathKey, t.info)
				} else {
					err = c.writeFile(t.absSrcPath, t.relPathKey, t.info, bufPtr, mr)
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

func (c *zipCompressor) writeFile(absSrcPath, relPathKey string, info os.FileInfo, bufPtr *[]byte, mr *compressMetricReader) error {

	// 1. Open File (Securely)
	// Security: TOCTOU check
	fileToZip, err := secureFileOpen(absSrcPath, info)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", absSrcPath, err)
	}
	// We only defer if we plan to return early.
	// If we close manually later, we can rely on the deferred call
	// to be a "safety net" (no-op if already closed).
	defer fileToZip.Close()

	mr.Reset(fileToZip)

	// 2. Prepare Header
	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return fmt.Errorf("failed to create zip header for %s: %w", relPathKey, err)
	}
	header.Name = relPathKey
	header.Method = zip.Deflate

	fSize := info.Size()

	// 3. Buffered Path
	if c.readAheadLimiter.TryAcquire(fSize) {
		defer c.readAheadLimiter.Release(fSize)

		// Use the pool for the read-ahead buffer
		readAheadDataPtr := c.readAheadPool.Get(fSize)
		// Put the buffer back regardless of whether the read succeeds
		defer c.readAheadPool.Put(readAheadDataPtr)

		if _, err := io.ReadFull(mr, *readAheadDataPtr); err != nil {
			return fmt.Errorf("failed to read file %s: %w", absSrcPath, err)
		}
		// Close early to free FD
		fileToZip.Close()

		c.mu.Lock()
		defer c.mu.Unlock()

		w, err := c.zw.CreateHeader(header)
		if err != nil {
			return fmt.Errorf("failed to write zip header for %s: %w", relPathKey, err)
		}
		_, err = io.CopyBuffer(w, bytes.NewReader(*readAheadDataPtr), *bufPtr)
		return err
	}

	// 4. Streamed Path
	c.mu.Lock()
	defer c.mu.Unlock()

	w, err := c.zw.CreateHeader(header)
	if err != nil {
		return fmt.Errorf("failed to write zip header for %s: %w", relPathKey, err)
	}

	_, err = io.CopyBuffer(w, mr, *bufPtr)
	return err
}

func (c *zipCompressor) sendErr(err error) {
	select {
	case c.zipErrsChan <- err:
	default:
	}
}
