package indexer

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	RECORDS_INIT = 64
)

const (
	RECORD_WRITING = iota
)

type Column struct {
	Table   string
	Name    string
	Dir     string
	Flist   *FileList
	IsNum   bool
	Dirties Records
}

type Record struct {
	fileID uint64
	offset int64
	size   int64
	cache  map[string]interface{}
}

type Records []*Record

func NewRecords(n int) Records {
	return make(Records, 0, n)
}

func (recs Records) Add(r *Record) Records {
	return append(recs, r)
}

func ColumnPath(tdir, col string, isNum bool) string {
	if isNum {
		return filepath.Join(Opt.RootDir, filepath.Base(tdir), col+"num.idx")

	}

	return filepath.Join(Opt.RootDir, filepath.Base(tdir), col+"gram.idx")
}

func JoinExt(s ...string) string {

	return strings.Join(s, ".")

}

func ColumnPathWithStatus(tdir, col string, isNum bool, s, e string, status byte) string {
	if status == 0 {
		// base.adding.process id.start-end
		return fmt.Sprintf("%s.adding.%d.%s-%s",
			ColumnPath(tdir, col, isNum),
			os.Getgid(),
			s, e)

	}
	return ""

}

func (idx *Indexer) OpenCol(flist *FileList, table, col string) *Column {

	return NewColumn(flist, table, col)
}

func NewColumn(flist *FileList, table, col string) *Column {
	//if _, e := os.Stat(ColumnPath(tableDir)); os.IsNotExist(e) {
	return &Column{
		Table:   table,
		Name:    col,
		Flist:   flist,
		Dirties: NewRecords(RECORDS_INIT),
	}
}

func (c *Column) Update(d time.Duration) error {

	for _, f := range c.Flist.Files {
		if f.name[0:len(filepath.Base(c.Table))] == c.Table {
			c.updateFile(f)
		}
	}

	return nil
}

func (c *Column) updateFile(f *File) {

	for r := range f.Records(c.Flist.Dir) {
		c.Dirties = c.Dirties.Add(r)
	}
}

func (c *Column) WriteDirties() {

}

func (r *Record) Write(w io.Writer, c *Column) error {

	//os.Create(ColumnPathWithStatus(c.Table, c.Name, true, 0, r.

	return nil
}

func (r *Record) caching(c *Column) {
	return
}

func (r *Record) IntValue(c *Column) int {
	if r.cache == nil {
		r.caching(c)
	}
	return 0
}
