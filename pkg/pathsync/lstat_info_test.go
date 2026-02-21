package pathsync

import (
	"os"
	"testing"
	"time"
)

// mockFileInfo implements os.FileInfo for testing purposes.
type mockFileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
}

func (m mockFileInfo) Name() string       { return m.name }
func (m mockFileInfo) Size() int64        { return m.size }
func (m mockFileInfo) Mode() os.FileMode  { return m.mode }
func (m mockFileInfo) ModTime() time.Time { return m.modTime }
func (m mockFileInfo) IsDir() bool        { return m.isDir }
func (m mockFileInfo) Sys() any           { return nil }

func TestMakeLstatInfo(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name     string
		fileInfo os.FileInfo
		want     lstatInfo
	}{
		{
			name: "Regular File",
			fileInfo: mockFileInfo{
				name:    "test.txt",
				size:    12345,
				mode:    0644,
				modTime: now,
				isDir:   false,
			},
			want: lstatInfo{
				ModTime:   now.UnixNano(),
				Size:      12345,
				Mode:      0644,
				IsDir:     false,
				IsRegular: true,
				IsSymlink: false,
			},
		},
		{
			name: "Directory",
			fileInfo: mockFileInfo{
				name:    "testdir",
				size:    4096,
				mode:    os.ModeDir | 0755,
				modTime: now,
				isDir:   true,
			},
			want: lstatInfo{
				ModTime:   now.UnixNano(),
				Size:      4096,
				Mode:      os.ModeDir | 0755,
				IsDir:     true,
				IsRegular: false,
				IsSymlink: false,
			},
		},
		{
			name: "Symlink",
			fileInfo: mockFileInfo{
				name:    "link",
				size:    12,
				mode:    os.ModeSymlink | 0777,
				modTime: now,
				isDir:   false,
			},
			want: lstatInfo{
				ModTime:   now.UnixNano(),
				Size:      12,
				Mode:      os.ModeSymlink | 0777,
				IsDir:     false,
				IsRegular: false,
				IsSymlink: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := makeLstatInfo(tt.fileInfo)

			if got.ModTime != tt.want.ModTime {
				t.Errorf("ModTime = %v, want %v", got.ModTime, tt.want.ModTime)
			}
			if got.Size != tt.want.Size {
				t.Errorf("Size = %v, want %v", got.Size, tt.want.Size)
			}
			if got.Mode != tt.want.Mode {
				t.Errorf("Mode = %v, want %v", got.Mode, tt.want.Mode)
			}
			if got.IsDir != tt.want.IsDir {
				t.Errorf("IsDir = %v, want %v", got.IsDir, tt.want.IsDir)
			}
			if got.IsRegular != tt.want.IsRegular {
				t.Errorf("IsRegular = %v, want %v", got.IsRegular, tt.want.IsRegular)
			}
			if got.IsSymlink != tt.want.IsSymlink {
				t.Errorf("IsSymlink = %v, want %v", got.IsSymlink, tt.want.IsSymlink)
			}
		})
	}
}
