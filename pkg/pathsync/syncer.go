package pathsync

import (
	"context"
	"io"
)

// syncer defines the interface for syncing directories
type syncer interface {
	Sync(ctx context.Context, absSourcePath, absTargetPath string) error
}

// syncMetricWriter wraps an io.Writer and updates metrics on every write.
type syncMetricWriter struct {
	w       io.Writer
	metrics Metrics
}

func (mw *syncMetricWriter) Write(p []byte) (n int, err error) {
	n, err = mw.w.Write(p)
	if n > 0 {
		mw.metrics.AddBytesWritten(int64(n))
	}
	return
}

func (mw *syncMetricWriter) Reset(w io.Writer) {
	mw.w = w
}

// syncMetricReader wraps an io.Reader and updates metrics on every read.
type syncMetricReader struct {
	r       io.Reader
	metrics Metrics
}

func (mr *syncMetricReader) Read(p []byte) (n int, err error) {
	n, err = mr.r.Read(p)
	if n > 0 {
		mr.metrics.AddBytesRead(int64(n))
	}
	return
}

func (mr *syncMetricReader) Reset(r io.Reader) {
	mr.r = r
}
