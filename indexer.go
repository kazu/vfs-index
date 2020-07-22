package vfsindex

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/kazu/vfs-index/vfs_schema"

	//"/github.com/kazu/loncha"
	"time"
)

func Hoge() bool {
	b := flatbuffers.NewBuilder(0)
	vfs_schema.RootStart(b)
	return true
}

type LogLevel int

const (
	LOG_ERROR LogLevel = iota
	LOG_WARN
	LOG_DEBUG
)

var CurrentLogLoevel LogLevel = LOG_DEBUG
var LogWriter io.StringWriter = os.Stderr

func Log(l LogLevel, f string, args ...interface{}) {

	if CurrentLogLoevel < l {
		return
	}

	var b strings.Builder
	_ = b
	switch l {
	case LOG_DEBUG:
		b.WriteString("D: ")
	case LOG_WARN:
		b.WriteString("W: ")
	case LOG_ERROR:
		b.WriteString("E: ")
	}
	fmt.Fprintf(&b, f, args...)
	LogWriter.WriteString(b.String())
	return
}

type Indexer struct {
	Root string
	Cols map[string]*Column
	opt  optionState
}

func TrimFilePathSuffix(path string) string {

	return path[0 : len(path)-len(filepath.Ext(path))]

}
func Open(dpath string, opts ...Option) (*Indexer, error) {
	mergeOpt(&Opt, opts...)
	//mergeOpt(&Opt, RootDir(dpath))

	idx := &Indexer{Root: dpath, Cols: make(map[string]*Column)}
	return idx, nil
}

func (idx *Indexer) Regist(table, col string) error {

	if len(table) == 0 || table[0] == '.' {
		return ErrInvalidTableName
	}

	flist, err := idx.OpenFileList(table)
	flist.Update()
	flist.Reload()

	idxCol := idx.OpenCol(flist, table, col)
	_ = idxCol
	idxCol.Update(1 * time.Minute)
	idx.Cols[col] = idxCol

	return err
}

func (idx *Indexer) On(table string, opts ...Option) *SearchCond {
	flist, err := idx.OpenFileList(table)
	if err != nil {
		return &SearchCond{idx: idx, Err: err}
	}
	mergeOpt(&idx.opt, opts...)
	cond := &SearchCond{idx: idx, flist: flist, table: table, column: ""}
	if len(idx.opt.column) > 0 {
		opt := &idx.opt
		cond.StartCol(opt.column)
	}

	return cond
}

func (idx *Indexer) OpenFileList(table string) (flist *FileList, err error) {

	dir := filepath.Dir(table)
	tableDir := filepath.Join(idx.Root, dir)

	if _, e := os.Stat(FileListPath(tableDir)); os.IsNotExist(e) {
		flist, err = CreateFileList(tableDir)
		if err != nil {
			return nil, err
		}
	}
	flist = OpenFileList(tableDir)
	return
}
