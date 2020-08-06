package decompress

import (
	"errors"
	"io"
	"os"
	"path/filepath"
)

var (
	ErrNoFound = errors.New("not found decompresser")
)

type FormatType byte

const (
	NoDecompressType FormatType = iota
	Lz4Type
)

type DeCompress struct {
	dec Decompresser
}

type Decompresser interface {
	io.ReaderAt
	io.ReadCloser
	//ReadAt([]byte, int64) (int, error)
	//Close() error
}

type Option func() Decompresser

func LocalFile(fname string) Option {

	return func() Decompresser {
		_, t := GetFormat(fname)
		switch t {
		case NoDecompressType:
			dec := NoDecompress{}
			dec.f, dec.e = os.Open(fname)
			return dec
		case Lz4Type:
			dec := NewLz4(fname)
			return dec
		}
		return nil
	}

}

func GetFormat(fname string) (string, FormatType) {
	ext := filepath.Ext(fname)
	if ext == ".lz4" {
		ext = filepath.Ext(fname[:len(fname)-len(ext)])
		return ext, Lz4Type
	}
	return ext, NoDecompressType
}

func Open(opt Option) (reader Decompresser, e error) {
	reader = opt()
	if reader == nil {
		e = ErrNoFound
		return
	}
	return
}

type NoDecompress struct {
	f *os.File
	e error
}

func (nd NoDecompress) ReadAt(b []byte, o int64) (n int, e error) {
	if nd.e != nil {
		return -1, nd.e
	}
	n, e = nd.f.ReadAt(b, o)
	//defer nd.f.Close()
	return
}

func (nd NoDecompress) Read(b []byte) (n int, e error) {
	if nd.e != nil {
		return -1, nd.e
	}
	n, e = nd.f.Read(b)
	return
}

func (nd NoDecompress) Close() error {
	return nd.f.Close()
}
