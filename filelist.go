package vfsindex

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"github.com/kazu/fbshelper/query/base"

	"github.com/kazu/loncha"
	"github.com/kazu/vfs-index/decompress"
	"github.com/kazu/vfs-index/query"
	"github.com/kazu/vfs-index/vfs_schema"
)

var ZERO_TIME time.Time = time.Time{}

//time.Now().Add(-10 * 365 * 24 * time.Duration.Hours()

type FileList struct {
	Dir       string
	IndexedAt time.Time
	Files     []*File
}

type File struct {
	id       uint64
	name     string
	index_at int64
}

func NewFile(id uint64, name string, index_at int64) *File {
	return &File{id: id, name: name, index_at: index_at}
}

func CreateFileList(tdir string) (flist *FileList, err error) {

	err = os.MkdirAll(filepath.Dir(FileListPath(tdir)), os.ModePerm)
	if err != nil {
		return
	}
	/*
		file, err := os.Create(FileListPath(tdir))
		if err != nil {
			return nil, err
		}
	*/
	flist = &FileList{Dir: tdir, IndexedAt: ZERO_TIME}
	return
}

func OpenFileList(tdir string) (flist *FileList) {
	return &FileList{Dir: tdir, IndexedAt: ZERO_TIME}
}

func GetInode(info os.FileInfo) uint64 {

	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0
	}
	return stat.Ino
}

func FileListPath(tabledir string) string {

	return filepath.Join(Opt.rootDir, filepath.Base(tabledir), "files.idx")
}

func FileListPathWithAdding(tabledir string, s, e uint64, usePid bool) string {

	var b strings.Builder
	if b.Cap() < 64 {
		b.Grow(64)
	}
	if usePid {
		fmt.Fprintf(&b, "%s.adding.%d.%08x-%08x", FileListPath(tabledir), os.Getgid(), s, e)
	} else {
		fmt.Fprintf(&b, "%s.%08x-%08x", FileListPath(tabledir), s, e)
	}
	return b.String()
}

func (flist *FileList) Update() {
	info, e := os.Stat(flist.Dir)

	if e != nil {
		return
	}

	if info.ModTime().Before(flist.IndexedAt) {
		return
	}

	infos, err := ioutil.ReadDir(flist.Dir)
	if err != nil {
		return
	}
	if len(flist.Files) < 1 {
		flist.Files = make([]*File, 0, len(infos))
	}

	for i, info := range infos {
		_, e := GetDecoder(info.Name())
		if e != nil {
			Log(LOG_WARN, "FileList.Update(): %s is skip( decoder not found)", info.Name())
			continue
		}
		flist.Files = append(flist.Files, &File{
			id:       GetInode(info),
			name:     filepath.Base(info.Name()),
			index_at: info.ModTime().UnixNano(), // ? ZERO_TIME?
		})
		Log(LOG_DEBUG, "loaded file list %+v from data \n", flist.Files[i])
	}

	sort.Slice(flist.Files, func(i, j int) bool { return flist.Files[i].id < flist.Files[j].id })

	flist.Store()
}

func (l *FileList) Reload() error {
	return l.load()
}

func (l *FileList) load() error {
	olen := len(l.Files)
	//l.Files = nil
	_ = olen

	files, e := filepath.Glob(fmt.Sprintf("%s.*-*", FileListPath(l.Dir)))
	if e != nil {
		Log(LOG_ERROR, "glob fail %s\n", e.Error())
		return e
	}

	nFiles := make([]*File, 0, olen)
	for _, fname := range files {
		r, e := os.Open(fname)
		if e != nil {
			Log(LOG_ERROR, "F: read %s %s\n", fname, e.Error())
			continue
		}
		f := FileFromFbs(r)
		Log(LOG_DEBUG, "loaded file list %+v from %s \n", f, fname)
		nFiles = append(nFiles, f)
		r.Close()
	}
	l.Files = nFiles
	sort.Slice(l.Files, func(i, j int) bool { return l.Files[i].id < l.Files[j].id })

	return nil
}

func (l *FileList) Store() {
	for _, f := range l.Files {
		f.Write(l)
	}
}

func (l *FileList) FPath(id uint64) (path string, e error) {

	//FIXME: shoud support incremental load
	//if l.Files
	if len(l.Files) < 1 {
		l.load()
	}

	inf, e := loncha.Find(&l.Files, func(i int) bool { return l.Files[i].id == id })
	if e != nil {
		Log(LOG_ERROR, "loncha error %s\n", e)
		return
	}
	if file, ok := inf.(*File); ok {
		path = filepath.Join(l.Dir, file.name)
		return
	}
	a := unsafe.Offsetof(l.Files)
	_ = a
	return path, ErrNotFoundFile
}

func FileFromFbs(r io.Reader) *File {
	root := query.Open(r, 512)

	file := root.Index().File()
	return &File{id: file.Id().Uint64(), name: string(file.Name().Bytes()), index_at: file.IndexAt().Int64()}
}

func (f *File) ToFbs(l *FileList) []byte {

	var e error
	root := query.NewRoot()
	root.SetVersion(query.FromInt32(1))
	root.WithHeader()

	file := query.NewFile()
	file.SetId(query.FromUint64(f.id))
	file.SetName(base.FromByteList([]byte(f.name)))
	if f.index_at == 0 {
		file.SetIndexAt(query.FromInt64(time.Now().UnixNano()))
	} else {
		file.SetIndexAt(query.FromInt64(f.index_at))
	}

	root.SetIndexType(query.FromByte(byte(vfs_schema.IndexFile)))
	e = root.SetIndex(&query.Index{CommonNode: file.CommonNode})
	if e != nil {
		return nil
	}
	root.Merge()
	return root.R(0)

}

func SafeRename(src, dst string) error {

	if err := os.Link(src, dst); err != nil {
		return err
	}
	return os.Remove(src)

}

func (f *File) finishWrite(l *FileList) error {
	return SafeRename(FileListPathWithAdding(l.Dir, f.id, f.id, true), FileListPathWithAdding(l.Dir, f.id, f.id, false))
}

func (f *File) Write(l *FileList) error {

	Log(LOG_DEBUG, "writting... %s\n", FileListPathWithAdding(l.Dir, f.id, f.id, true))
	io, e := os.Create(FileListPathWithAdding(l.Dir, f.id, f.id, true))
	if e != nil {
		Log(LOG_WARN, "F: writing... %s\n", FileListPathWithAdding(l.Dir, f.id, f.id, true))
		return e
	}
	io.Write(f.ToFbs(l))
	io.Close()

	Log(LOG_DEBUG, "renaming... %s -> %s \n",
		FileListPathWithAdding(l.Dir, f.id, f.id, true),
		FileListPathWithAdding(l.Dir, f.id, f.id, false),
	)
	e = f.finishWrite(l)
	if e != nil {
		os.Remove(FileListPathWithAdding(l.Dir, f.id, f.id, true))
		Log(LOG_WARN, "F: rename %s -> %s \n",
			FileListPathWithAdding(l.Dir, f.id, f.id, true),
			FileListPathWithAdding(l.Dir, f.id, f.id, false),
		)
		return e
	}
	Log(LOG_DEBUG, "S: renamed %s -> %s \n", FileListPathWithAdding(l.Dir, f.id, f.id, true), FileListPathWithAdding(l.Dir, f.id, f.id, false))

	return nil

}

// FIXME: support other format
func (f *File) Records(ctx context.Context, dir string) <-chan *Record {
	ch := make(chan *Record, 5)

	path := filepath.Join(dir, f.name)
	rio, e := decompress.Open(decompress.LocalFile(path))
	if e != nil {
		close(ch)
		rio.Close()
		return ch
	}

	ext := filepath.Ext(f.name)

	dec, err := GetDecoder(f.name)

	if err != nil {
		Log(LOG_ERROR, "File.Records(): cannot find %s decoder\n", ext)
		rio.Close()
		defer close(ch)
		return ch
	}

	go func(ctx context.Context, f io.Closer) {
		defer f.Close()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}(ctx, rio)

	return dec.Tokenizer(ctx, rio, f)

}
