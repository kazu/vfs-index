package decompress

import (
	"io"
	"os"

	"github.com/pierrec/lz4"
)

type Lz4 struct {
	f *os.File
	r *lz4.Reader
	e error
}

func NewLz4(fname string) Lz4 {
	l := Lz4{}
	l.f, l.e = os.Open(fname)
	if l.e == nil {
		l.r = lz4.NewReader(l.f)
	}
	return l
}

func (l Lz4) Close() error {
	return l.f.Close()
}

func (l Lz4) ReadAt(b []byte, o int64) (n int, e error) {
	if l.e != nil {
		return -1, l.e
	}
	c, se := l.r.Seek(0, io.SeekCurrent)
	if se != nil {
		o -= c
	}
	sn, se := l.r.Seek(o, io.SeekCurrent)
	l.e = se
	if sn != o {
		return -1, l.e
	}

	if l.e != nil {
		return -1, l.e
	}
	n, e = l.r.Read(b)
	c, _ = l.r.Seek(0, io.SeekCurrent)
	//defer l.f.Close()
	return
}

func (l Lz4) Read(b []byte) (n int, e error) {
	if l.e != nil {
		return -1, l.e
	}
	n, e = l.f.Read(b)
	return
}
