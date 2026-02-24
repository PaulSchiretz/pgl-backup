package pathcompression

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/klauspost/pgzip"
	"github.com/paulschiretz/pgl-backup/pkg/limiter"
	"github.com/paulschiretz/pgl-backup/pkg/plog"
	"github.com/paulschiretz/pgl-backup/pkg/pool"
	"github.com/paulschiretz/pgl-backup/pkg/util"
)

type tarCompressor struct {
	// Read buffer pool
	ioBufferPool *pool.FixedBufferPool
	ioBufferSize int64

	// Limiter for sequential writes
	readAheadLimiter *limiter.Memory
	readAheadLimit   int64
	readAheadPool    *pool.BucketedBufferPool

	format  Format
	level   Level
	dryRun  bool
	mu      sync.Mutex
	tw      *tar.Writer
	metrics Metrics

	// ctx is the cancellable context for the entire run.
	ctx context.Context

	src  string
	trgF *os.File

	numTarWorkers int

	// tarWg waits for the tarItemProducer and tarWorkers to finish processing all tar tasks.
	tarWg sync.WaitGroup

	// tarItemsChan is the channel where the Walker sends pre-processed tasks.
	tarItemsChan chan *tarItem

	// tarErrsChan captures the first critical, unrecoverable error (e.g., walker failure)
	tarErrsChan chan error

	// Pool for tarItem structs to reduce GC pressure
	tarItemPool *sync.Pool

	// Pool for compressMetricWriter to reduce GC pressure
	compressMetricWriterPool *sync.Pool

	// Pool for compressMetricReader to reduce GC pressure
	compressMetricReaderPool *sync.Pool
}

// tarItem struct for tar workers
type tarItem struct {
	absSrcPath string
	relPathKey string
	info       os.FileInfo
}

func newTarCompressor(dryRun bool, format Format, level Level, ioBufferPool *pool.FixedBufferPool, ioBufferSize int64, readAheadLimiter *limiter.Memory, readAheadLimit int64, readAheadPool *pool.BucketedBufferPool, numWorkers int, metrics Metrics) *tarCompressor {
	return &tarCompressor{
		dryRun:           dryRun,
		format:           format,
		level:            level,
		numTarWorkers:    numWorkers,
		metrics:          metrics,
		tarItemsChan:     make(chan *tarItem, numWorkers*4),
		tarErrsChan:      make(chan error, 1),
		ioBufferPool:     ioBufferPool,
		ioBufferSize:     ioBufferSize,
		readAheadLimiter: readAheadLimiter,
		readAheadLimit:   readAheadLimit,
		readAheadPool:    readAheadPool,
		tarItemPool: &sync.Pool{
			New: func() any {
				return new(tarItem)
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

func (c *tarCompressor) Compress(ctx context.Context, absSourcePath, absArchiveFilePath string) (retErr error) {

	plog.Notice("COMPRESS", "source", absSourcePath)

	if c.dryRun {
		plog.Notice("[DRY RUN] COMPRESS", "source", absSourcePath)
		return nil
	}

	var err error

	// Create a cancellable context to ensure the producer goroutine exits
	// if we return early due to an error.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c.ctx = ctx

	// store the source path
	c.src = absSourcePath

	// 1. Create Temp File
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

	mw := c.compressMetricWriterPool.Get().(*compressMetricWriter)
	mw.Reset(c.trgF)
	defer c.compressMetricWriterPool.Put(mw)

	bufWriter := bufio.NewWriterSize(mw, int(c.ioBufferSize))

	var compressedWriter io.WriteCloser
	if c.format == TarZst {
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

	// 2. Start the tarItemProducer (Producer)
	// This goroutine walks the file tree and feeds paths into 'tarItems'.
	go c.tarItemProducer()

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

func (c *tarCompressor) tarItemProducer() {
	defer close(c.tarItemsChan)
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

		task := c.tarItemPool.Get().(*tarItem)
		task.absSrcPath = absSrcPath
		task.relPathKey = relPathKey
		task.info = info

		select {
		case c.tarItemsChan <- task:
			return nil
		case <-c.ctx.Done():
			c.tarItemPool.Put(task) // Return to pool on cancellation
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
	bufPtr := c.ioBufferPool.Get()
	defer c.ioBufferPool.Put(bufPtr)

	mr := c.compressMetricReaderPool.Get().(*compressMetricReader)
	defer c.compressMetricReaderPool.Put(mr)

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.tarErrsChan:
			return
		case t, ok := <-c.tarItemsChan:
			if !ok {
				return // Channel closed
			}

			// Use an IIFE to ensure the task is always returned to the pool.
			func() {
				defer c.tarItemPool.Put(t)

				var err error
				if t.info.Mode()&os.ModeSymlink != 0 {
					err = c.writeSymlink(t.absSrcPath, t.relPathKey, t.info)
				} else {
					err = c.writeFile(t.absSrcPath, t.relPathKey, t.info, bufPtr, mr)
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

func (c *tarCompressor) writeFile(absSrcPath, relPathKey string, info os.FileInfo, bufPtr *[]byte, mr *compressMetricReader) error {

	// 1. Open File (Securely)
	// Security: TOCTOU check
	fileToTar, err := secureFileOpen(absSrcPath, info)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", absSrcPath, err)
	}
	// We only defer if we plan to return early.
	// If we close manually later, we can rely on the deferred call
	// to be a "safety net" (no-op if already closed).
	defer fileToTar.Close()

	mr.Reset(fileToTar)

	// 2. Prepare Header
	header, err := tar.FileInfoHeader(info, "")
	if err != nil {
		return fmt.Errorf("failed to create tar header for %s: %w", relPathKey, err)
	}
	header.Name = relPathKey

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
		fileToTar.Close()

		c.mu.Lock()
		defer c.mu.Unlock()

		if err := c.tw.WriteHeader(header); err != nil {
			return fmt.Errorf("failed to write tar header for %s: %w", relPathKey, err)
		}
		_, err = io.CopyBuffer(c.tw, bytes.NewReader(*readAheadDataPtr), *bufPtr)
		return err
	}

	// 4. Streamed Path
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.tw.WriteHeader(header); err != nil {
		return fmt.Errorf("failed to write tar header for %s: %w", relPathKey, err)
	}

	_, err = io.CopyBuffer(c.tw, mr, *bufPtr)
	return err
}

func (c *tarCompressor) sendErr(err error) {
	select {
	case c.tarErrsChan <- err:
	default:
	}
}
