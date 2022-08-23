// Code generated by vfsgen; DO NOT EDIT.

package db

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	pathpkg "path"
	"time"
)

// DBMigrationFileSystem statically implements the virtual filesystem provided to vfsgen.
var DBMigrationFileSystem = func() http.FileSystem {
	fs := vfsgen۰FS{
		"/": &vfsgen۰DirInfo{
			name:    "/",
			modTime: time.Date(2022, 8, 23, 10, 51, 53, 124223012, time.UTC),
		},
		"/000001_create_predator_tables.down.sql": &vfsgen۰CompressedFileInfo{
			name:             "000001_create_predator_tables.down.sql",
			modTime:          time.Date(2022, 8, 23, 10, 51, 53, 118605127, time.UTC),
			uncompressedSize: 221,

			compressedContent: []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x02\xff\x72\x09\xf2\x0f\x50\x08\x71\x74\xf2\x71\x55\xf0\x74\x53\x70\x8d\xf0\x0c\x0e\x09\x56\x28\x2e\x49\x2c\x29\x2d\xb6\xe6\xe2\xc2\x2a\x9b\x9b\x5a\x52\x94\x99\x8c\x4b\x36\x29\x33\xbd\xb0\x34\xb5\xa8\x32\x3e\x2b\x3f\x09\x97\x9a\xc4\xd2\x94\xcc\x92\xf8\xa2\xd4\xe2\xd2\x9c\x12\xbc\x6a\x70\x49\x16\x14\xe5\xa7\x65\xe6\xa4\xe2\x92\x4e\xcd\x2b\xc9\x2c\xa9\xb4\xe6\x02\x04\x00\x00\xff\xff\x7e\xb5\x58\xdd\xdd\x00\x00\x00"),
		},
		"/000001_create_predator_tables.up.sql": &vfsgen۰CompressedFileInfo{
			name:             "000001_create_predator_tables.up.sql",
			modTime:          time.Date(2022, 8, 23, 10, 51, 53, 123797318, time.UTC),
			uncompressedSize: 3069,

			compressedContent: []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x02\xff\xbc\x55\x4b\x6f\xda\x4e\x10\xbf\xf3\x29\x46\x39\x81\x94\x48\xf9\xeb\xaf\xf6\x92\x93\x09\x4e\xeb\x96\x98\x08\x4c\x45\x4e\xab\xc5\x1e\xe8\x22\x7b\xd7\xd9\x07\x2d\xdf\xbe\xb2\xd7\xc6\x2f\x5e\x49\xd3\x70\x9c\xf9\x8d\x3d\xfe\x3d\x86\x9b\x1b\x08\x25\x52\x8d\x90\x4a\xb1\x62\x31\xf6\xee\xa7\xae\x13\xb8\xe0\x2e\x02\xd7\x9f\x79\x13\x1f\xbc\x07\xf0\x27\x01\xb8\x0b\x6f\x16\xcc\xe0\xca\x18\x16\xdd\x08\xa5\xd2\xab\xbb\x12\x1b\x38\xc3\xb1\xdb\xc2\x15\x8f\xeb\xf7\x00\x00\x58\x04\xf3\xb9\x37\x82\xa7\xa9\xf7\xe8\x4c\x9f\xe1\xbb\xfb\x9c\x63\xfd\xf9\x78\x0c\x23\xf7\xc1\x99\x8f\x03\xc8\x1e\x4c\xd6\xc8\x51\x52\x8d\x64\xfb\x5f\x7f\x70\x9d\x0f\x1b\xc9\x21\x70\x17\xc1\x7e\xc2\x96\xd7\x52\x98\x94\x70\x9a\x20\xfc\x70\xa6\xf7\x5f\x9d\xa9\xad\xaf\x58\xac\x51\xe6\x13\xb6\x90\x88\xa8\x05\xd1\x42\xd3\x98\x48\x0c\x85\x8c\x14\x0c\xbd\x2f\x9e\x1f\x74\xf7\xb9\xb5\x60\x6a\x22\xa6\x89\x66\x09\x42\xe0\x3d\xba\xb3\xc0\x79\x7c\xb2\x1d\xdc\x22\xb7\x1d\xa5\x69\x92\x56\xed\xbc\x3b\xb8\xeb\xf5\x2a\x76\x35\x5d\xc6\x08\x4b\xb6\x7e\x31\x28\x77\xb0\x11\xcb\x53\xe4\x95\x38\xb2\x11\x4b\xcb\xe0\x85\x3f\x16\xc1\xcc\x9d\x7a\xce\xb8\x4e\xf5\xf5\x6b\x9e\x50\xe8\x46\x4a\xc9\xf6\xb4\x48\x5c\xa1\x44\x1e\xa2\xda\x6b\xcb\xa2\x42\xa2\xe5\x4b\x86\x3f\x20\x92\xfd\xfa\x88\x50\x5d\xb1\xb3\x87\xec\x69\x2a\x98\xf0\xfc\x91\xbb\x80\xe5\x86\x54\x3b\x10\x16\xfd\x86\x89\xdf\x20\x04\xfa\x55\x7f\x70\xd7\x19\xce\x77\x39\x3c\x97\xb7\x9a\xba\x24\xa8\x25\x0b\xad\x3c\xa7\x14\xb1\xb8\x0b\xb5\xf8\x1b\x15\x6a\xfc\x67\x81\xb8\x8c\x7f\x9b\x85\x2d\x8d\x0d\xd6\x8c\xbf\x62\x18\x47\xa5\x30\xb6\x24\x7e\x71\x94\x44\xef\xd2\x7d\x22\xa0\xff\xff\xed\xa0\xa5\x9a\xfd\xd8\x46\xb6\xa0\xff\xe9\x18\xcc\xbe\x76\x34\x99\x67\xac\x3d\x4d\xdd\x7b\x2f\x3f\x1b\x2d\x23\x08\x1e\x31\xcd\x04\xaf\x2d\x13\x52\x8d\x6b\x21\x77\xcd\x70\x26\xa8\x69\x44\x35\x85\x6f\xb3\x89\x3f\x7c\xb3\x8d\x92\x03\x2e\x2a\xc4\x3e\xe1\x9f\x84\x94\xa4\xb5\x67\xca\x7a\x77\xa2\x75\x06\xda\x83\xd5\xee\x4d\xe7\xe5\x77\xe5\xbc\xf1\x72\xd8\x3b\x5c\xd1\x37\xe5\xfa\x55\x87\xf2\xe8\x39\x3c\xa3\x14\x35\x07\xa4\xb2\xec\x34\x95\xea\x90\x27\x51\x99\xb8\xe4\xf0\x2c\x89\xc4\xc2\x3f\xf8\x9e\xda\x57\x9f\x62\xdd\x2a\xfc\xfa\x2c\xff\x83\x94\x6a\x11\xa3\xa4\x3c\x44\x22\x4d\x8c\x2a\x8f\xe0\x05\x41\x4e\xa9\x52\x64\x15\xd3\x35\x0c\x27\x93\xb1\xeb\xf8\xdd\x15\xde\x23\xd2\x54\x92\x92\xcd\x86\x4d\x0a\x61\xa1\x5f\x76\xdb\x19\xa5\xb2\x13\xeb\xe6\x60\x2d\xdc\x35\x93\x29\x4d\xb5\x51\x17\xd8\xcb\x02\x3f\xe0\xcf\x61\x23\x96\x99\x07\xaa\xd3\xfd\xb9\xad\x76\x86\x38\x77\xde\x8b\xef\x3a\xe9\x19\xa5\xe8\xba\xee\xbf\x37\xc8\xa5\x88\x5d\xb7\xa4\xbc\x78\x6d\xdf\x56\xdb\x1a\x59\x74\xb6\xfa\x21\x7c\x56\xef\x4e\x1c\xb9\xbc\xe5\xe0\xb1\xcb\x8b\x5c\x33\xbd\xbb\x40\x57\x0b\xcc\x75\x65\x11\x6c\xa9\x0c\x7f\x52\xd9\x91\x2d\x0f\x60\xd9\xe4\x42\x03\x37\x71\x9c\x77\x90\x6f\x99\x14\x3c\x41\xae\x0f\x03\xd6\x4c\x13\x23\xe3\x23\xcd\x30\xcd\x0e\xe3\x06\xc3\xcc\xd3\xea\x30\xa8\xa6\x4b\x75\x79\x1b\x08\x93\x46\x1d\x44\xcf\xea\xf5\x27\x00\x00\xff\xff\xfb\xaa\x66\x98\xfd\x0b\x00\x00"),
		},
	}
	fs["/"].(*vfsgen۰DirInfo).entries = []os.FileInfo{
		fs["/000001_create_predator_tables.down.sql"].(os.FileInfo),
		fs["/000001_create_predator_tables.up.sql"].(os.FileInfo),
	}

	return fs
}()

type vfsgen۰FS map[string]interface{}

func (fs vfsgen۰FS) Open(path string) (http.File, error) {
	path = pathpkg.Clean("/" + path)
	f, ok := fs[path]
	if !ok {
		return nil, &os.PathError{Op: "open", Path: path, Err: os.ErrNotExist}
	}

	switch f := f.(type) {
	case *vfsgen۰CompressedFileInfo:
		gr, err := gzip.NewReader(bytes.NewReader(f.compressedContent))
		if err != nil {
			// This should never happen because we generate the gzip bytes such that they are always valid.
			panic("unexpected error reading own gzip compressed bytes: " + err.Error())
		}
		return &vfsgen۰CompressedFile{
			vfsgen۰CompressedFileInfo: f,
			gr:                        gr,
		}, nil
	case *vfsgen۰DirInfo:
		return &vfsgen۰Dir{
			vfsgen۰DirInfo: f,
		}, nil
	default:
		// This should never happen because we generate only the above types.
		panic(fmt.Sprintf("unexpected type %T", f))
	}
}

// vfsgen۰CompressedFileInfo is a static definition of a gzip compressed file.
type vfsgen۰CompressedFileInfo struct {
	name              string
	modTime           time.Time
	compressedContent []byte
	uncompressedSize  int64
}

func (f *vfsgen۰CompressedFileInfo) Readdir(count int) ([]os.FileInfo, error) {
	return nil, fmt.Errorf("cannot Readdir from file %s", f.name)
}
func (f *vfsgen۰CompressedFileInfo) Stat() (os.FileInfo, error) { return f, nil }

func (f *vfsgen۰CompressedFileInfo) GzipBytes() []byte {
	return f.compressedContent
}

func (f *vfsgen۰CompressedFileInfo) Name() string       { return f.name }
func (f *vfsgen۰CompressedFileInfo) Size() int64        { return f.uncompressedSize }
func (f *vfsgen۰CompressedFileInfo) Mode() os.FileMode  { return 0444 }
func (f *vfsgen۰CompressedFileInfo) ModTime() time.Time { return f.modTime }
func (f *vfsgen۰CompressedFileInfo) IsDir() bool        { return false }
func (f *vfsgen۰CompressedFileInfo) Sys() interface{}   { return nil }

// vfsgen۰CompressedFile is an opened compressedFile instance.
type vfsgen۰CompressedFile struct {
	*vfsgen۰CompressedFileInfo
	gr      *gzip.Reader
	grPos   int64 // Actual gr uncompressed position.
	seekPos int64 // Seek uncompressed position.
}

func (f *vfsgen۰CompressedFile) Read(p []byte) (n int, err error) {
	if f.grPos > f.seekPos {
		// Rewind to beginning.
		err = f.gr.Reset(bytes.NewReader(f.compressedContent))
		if err != nil {
			return 0, err
		}
		f.grPos = 0
	}
	if f.grPos < f.seekPos {
		// Fast-forward.
		_, err = io.CopyN(ioutil.Discard, f.gr, f.seekPos-f.grPos)
		if err != nil {
			return 0, err
		}
		f.grPos = f.seekPos
	}
	n, err = f.gr.Read(p)
	f.grPos += int64(n)
	f.seekPos = f.grPos
	return n, err
}
func (f *vfsgen۰CompressedFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		f.seekPos = 0 + offset
	case io.SeekCurrent:
		f.seekPos += offset
	case io.SeekEnd:
		f.seekPos = f.uncompressedSize + offset
	default:
		panic(fmt.Errorf("invalid whence value: %v", whence))
	}
	return f.seekPos, nil
}
func (f *vfsgen۰CompressedFile) Close() error {
	return f.gr.Close()
}

// vfsgen۰DirInfo is a static definition of a directory.
type vfsgen۰DirInfo struct {
	name    string
	modTime time.Time
	entries []os.FileInfo
}

func (d *vfsgen۰DirInfo) Read([]byte) (int, error) {
	return 0, fmt.Errorf("cannot Read from directory %s", d.name)
}
func (d *vfsgen۰DirInfo) Close() error               { return nil }
func (d *vfsgen۰DirInfo) Stat() (os.FileInfo, error) { return d, nil }

func (d *vfsgen۰DirInfo) Name() string       { return d.name }
func (d *vfsgen۰DirInfo) Size() int64        { return 0 }
func (d *vfsgen۰DirInfo) Mode() os.FileMode  { return 0755 | os.ModeDir }
func (d *vfsgen۰DirInfo) ModTime() time.Time { return d.modTime }
func (d *vfsgen۰DirInfo) IsDir() bool        { return true }
func (d *vfsgen۰DirInfo) Sys() interface{}   { return nil }

// vfsgen۰Dir is an opened dir instance.
type vfsgen۰Dir struct {
	*vfsgen۰DirInfo
	pos int // Position within entries for Seek and Readdir.
}

func (d *vfsgen۰Dir) Seek(offset int64, whence int) (int64, error) {
	if offset == 0 && whence == io.SeekStart {
		d.pos = 0
		return 0, nil
	}
	return 0, fmt.Errorf("unsupported Seek in directory %s", d.name)
}

func (d *vfsgen۰Dir) Readdir(count int) ([]os.FileInfo, error) {
	if d.pos >= len(d.entries) && count > 0 {
		return nil, io.EOF
	}
	if count <= 0 || count > len(d.entries)-d.pos {
		count = len(d.entries) - d.pos
	}
	e := d.entries[d.pos : d.pos+count]
	d.pos += count
	return e, nil
}
