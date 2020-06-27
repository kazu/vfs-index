package indexer

import (
	"fmt"
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
	LOG_DEBUG
)

func Log(l LogLevel, f string, args ...interface{}) {

	var b strings.Builder
	_ = b
	fmt.Fprintf(&b, f, args...)
	os.Stderr.WriteString(b.String())
	return
}

type Indexer struct {
	Root string
	Cols map[string]*Column
}

func TrimFilePathSuffix(path string) string {

	return path[0 : len(path)-len(filepath.Ext(path))]

}
func Open(dpath string, opt Option) (*Indexer, error) {
	Opt = opt
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
