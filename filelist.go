package vfsindex

import (
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

	"encoding/json"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/kazu/loncha"
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

	return filepath.Join(Opt.RootDir, filepath.Base(tabledir), "files.idx")
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
		flist.Files = make([]*File, len(infos))
	}

	for i, info := range infos {
		flist.Files[i] = &File{
			id:       GetInode(info),
			name:     filepath.Base(info.Name()),
			index_at: info.ModTime().UnixNano(), // ? ZERO_TIME?
		}
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
	raws, e := ioutil.ReadAll(r)
	if e != nil {
		return nil
	}
	vRoot := vfs_schema.GetRootAsRoot(raws, 0)

	uTable := new(flatbuffers.Table)
	vRoot.Index(uTable)
	fbsFile := new(vfs_schema.File)
	fbsFile.Init(uTable.Bytes, uTable.Pos)

	return &File{id: fbsFile.Id(), name: string(fbsFile.Name()), index_at: fbsFile.IndexAt()}
}

func (f *File) ToFbs(l *FileList) []byte {

	b := flatbuffers.NewBuilder(0)
	fname := b.CreateString(f.name)

	vfs_schema.FileStart(b)
	vfs_schema.FileAddId(b, f.id)
	vfs_schema.FileAddName(b, fname)
	if f.index_at == 0 {
		vfs_schema.FileAddIndexAt(b, time.Now().UnixNano())
	} else {
		vfs_schema.FileAddIndexAt(b, f.index_at)
	}
	fbFile := vfs_schema.FileEnd(b)

	vfs_schema.RootStart(b)
	vfs_schema.RootAddVersion(b, 1)
	vfs_schema.RootAddIndexType(b, vfs_schema.IndexFile)
	vfs_schema.RootAddIndex(b, fbFile)
	b.Finish(vfs_schema.RootEnd(b))
	return b.FinishedBytes()

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
func (f *File) Records(dir string) <-chan *Record {
	ch := make(chan *Record, 5)
	/*
		b, _ := ioutil.ReadFile(filepath.Join(dir, f.name))
		buf := string(b)
	*/
	rio, e := os.Open(filepath.Join(dir, f.name))
	if e != nil {
		close(ch)
		rio.Close()
		return ch
	}

	go func() {
		dec := json.NewDecoder(rio)

		var rec *Record

		nest := int(0)
		for {

			token, err := dec.Token()
			if err == io.EOF {
				break
			}
			switch token {
			case json.Delim('{'):
				nest++
				if nest == 1 {
					rec = &Record{fileID: f.id, offset: dec.InputOffset() - 1}
				}

			case json.Delim('}'):
				nest--
				if nest == 0 {
					rec.size = dec.InputOffset() - rec.offset
					ch <- rec
					//Log(LOG_DEBUG, "rec=%+v\n", rec)
					//Log(LOG_DEBUG, "1: raw='%s'\n", strings.ReplaceAll(buf[rec.offset:rec.offset+rec.size], "\n", " "))
				}
			}
		}

		close(ch)
		rio.Close()
	}()

	return ch

}
