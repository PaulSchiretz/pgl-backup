package pool

import (
	"testing"
)

func TestBucketedBufferPool_New(t *testing.T) {
	// Valid
	_ = NewBucketedBufferPool(1024, 4096)

	// Invalid Min (not power of two)
	func() {
		defer func() {
			if recover() == nil {
				t.Error("expected panic for non-power-of-two minSize")
			}
		}()
		NewBucketedBufferPool(1000, 4096)
	}()

	// Invalid Max (not power of two)
	func() {
		defer func() {
			if recover() == nil {
				t.Error("expected panic for non-power-of-two maxSize")
			}
		}()
		NewBucketedBufferPool(1024, 4097)
	}()

	// Invalid Range (max <= min)
	func() {
		defer func() {
			if recover() == nil {
				t.Error("expected panic for minSize >= maxSize")
			}
		}()
		NewBucketedBufferPool(4096, 1024)
	}()
}

func TestBucketedBufferPool_Get(t *testing.T) {
	minSize := int64(1024)
	maxSize := int64(16384) // 16KB
	bp := NewBucketedBufferPool(minSize, maxSize)

	tests := []struct {
		name    string
		reqSize int64
		wantLen int
		wantCap int // Minimum expected capacity
	}{
		{"Zero", 0, 0, 0},
		{"Negative", -1, 0, 0},
		{"Tiny", 10, 10, int(minSize)}, // Promoted to min bucket
		{"ExactMin", 1024, 1024, 1024},
		{"Between", 2000, 2000, 2048}, // Next power of 2 is 2048
		{"ExactMax", 16384, 16384, 16384},
		{"TooBig", 20000, 20000, 20000}, // Not pooled, just allocated
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bufPtr := bp.Get(tt.reqSize)
			if bufPtr == nil {
				t.Fatal("Get returned nil")
			}
			b := *bufPtr

			if len(b) != tt.wantLen {
				t.Errorf("got len %d, want %d", len(b), tt.wantLen)
			}
			if cap(b) < tt.wantCap {
				t.Errorf("got cap %d, want >= %d", cap(b), tt.wantCap)
			}

			// Basic Put check (should not panic and should handle the buffer)
			bp.Put(bufPtr)
		})
	}
}

func TestBucketedBufferPool_Put_Logic(t *testing.T) {
	// This test verifies that Put accepts valid buffers and rejects invalid ones
	// based on the pool configuration.
	bp := NewBucketedBufferPool(1024, 4096)

	// 1. Valid buffer (1024)
	b1 := make([]byte, 1024)
	bp.Put(&b1) // Should be accepted (no panic, no easy way to verify internal state without race)

	// 2. Invalid buffer (too small)
	b2 := make([]byte, 512)
	bp.Put(&b2) // Should be rejected/ignored

	// 3. Invalid buffer (too big)
	b3 := make([]byte, 8192)
	bp.Put(&b3) // Should be rejected/ignored

	// 4. Invalid buffer (not power of two)
	b4 := make([]byte, 2000)
	bp.Put(&b4) // Should be rejected/ignored
}

func TestFixedBufferPool(t *testing.T) {
	size := int64(1024)
	fp := NewFixedBuffer(size)

	// Get
	ptr := fp.Get()
	if len(*ptr) != int(size) {
		t.Errorf("got len %d, want %d", len(*ptr), size)
	}
	if cap(*ptr) != int(size) {
		t.Errorf("got cap %d, want %d", cap(*ptr), size)
	}

	// Put
	fp.Put(ptr)

	// Put invalid size (should be ignored)
	small := make([]byte, 10)
	fp.Put(&small)

	// Put nil
	fp.Put(nil)
}
