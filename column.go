package vfsindex

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/davecgh/go-spew/spew"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/kazu/loncha"
	"github.com/kazu/vfs-index/vfs_schema"
)

const (
	RECORDS_INIT = 64
)

const (
	RECORD_WRITING byte = iota
	RECORD_WRITTEN
)

type Column struct {
	Table   string
	Name    string
	Dir     string
	Flist   *FileList
	IsNum   bool
	Dirties Records

	cache *IdxCaches
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

func NewRecord(id uint64, offset, size int64) *Record {
	return &Record{fileID: id, offset: offset, size: size}
}

func (recs Records) Add(r *Record) Records {
	return append(recs, r)
}

func ColumnPath(tdir, col string, isNum bool) string {
	if isNum {
		return filepath.Join(Opt.RootDir, tdir, col+".num.idx")

	}

	return filepath.Join(Opt.RootDir, tdir, col+".gram.idx")
}

func JoinExt(s ...string) string {

	return strings.Join(s, ".")

}

func ColumnPathWithStatus(tdir, col string, isNum bool, s, e string, status byte) string {
	if status == RECORD_WRITING {

	}
	switch status {
	case RECORD_WRITING:
		// base.adding.process id.start-end
		// <column name>.<index type>.idx.adding.<pid>.<inode number>.<start>-<end>
		//   index type  num or tri ?
		return fmt.Sprintf("%s.adding.%d.%s-%s", ColumnPath(tdir, col, isNum), os.Getgid(), s, e)
	case RECORD_WRITTEN:
		// <column name>.<index type>.idx.adding.<pid>.<start>-<end>
		return fmt.Sprintf("%s.%s-%s", ColumnPath(tdir, col, isNum), s, e)
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
		cache:   NewIdxCaches(),
	}
}

func (c *Column) Update(d time.Duration) error {

	idxDir := filepath.Join(Opt.RootDir, c.Table)
	err := os.MkdirAll(idxDir, os.ModePerm)
	if err != nil {
		return err
	}

	for _, f := range c.Flist.Files {
		if f.name[0:len(filepath.Base(c.Table))] == c.Table {
			c.updateFile(f)
		}
	}
	c.WriteDirties()

	return nil
}

func (c *Column) updateFile(f *File) {

	for r := range f.Records(c.Flist.Dir) {
		c.Dirties = c.Dirties.Add(r)
	}
}

func (c *Column) WriteDirties() {
	for _, r := range c.Dirties {
		if r.Write(c) == nil {
			loncha.Delete(c.Dirties, func(i int) bool {
				return c.Dirties[i].fileID == r.fileID &&
					c.Dirties[i].offset == r.offset
			})
		}
	}
}

func (c *Column) RecordEqInt(v int) (record *Record) {

	// files, e := filepath.Glob(fmt.Sprintf("%s.*-*", FileListPath(l.Dir)))

	path := ColumnPathWithStatus(c.TableDir(), c.Name, true, toFname(uint64(v)), toFname(uint64(v)), RECORD_WRITTEN)
	rio, e := os.Open(path)
	if e == nil {
		record = RecordFromFbs(rio)
		record.caching(c)
		rio.Close()
		return
	}

	return nil
}

func toFname(i interface{}) string {
	return fmt.Sprintf("%010x", i)
}

func (c *Column) TableDir() string {

	return filepath.Join(c.Dir, c.Table)
}

func (r *Record) Write(c *Column) error {

	r.caching(c)

	wPath := ColumnPathWithStatus(c.TableDir(), c.Name, true, toFname(r.Uint64Value(c)), toFname(r.Uint64Value(c)), RECORD_WRITING)
	path := ColumnPathWithStatus(c.TableDir(), c.Name, true, toFname(r.Uint64Value(c)), toFname(r.Uint64Value(c)), RECORD_WRITTEN)
	path = fmt.Sprintf("%s.%010x.%010x", path, r.fileID, r.offset)

	//Log(LOG_DEBUG, "path=%s %s \n", wPath, c.TableDir(), c)

	io, e := os.Create(wPath)
	if e != nil {
		Log(LOG_WARN, "F: open... %s\n", wPath)
		return e
	}
	io.Write(r.ToFbs(r.Uint64Value(c)))
	io.Close()
	Log(LOG_DEBUG, "S: written %s \n", wPath)
	if r.Uint64Value(c) == 0 {
		spew.Dump(r)
	}

	e = SafeRename(wPath, path)
	if e != nil {
		os.Remove(wPath)
		Log(LOG_WARN, "F: rename %s -> %s \n", wPath, path)
		return e
	}
	Log(LOG_DEBUG, "S: renamed%s -> %s \n", wPath, path)

	return nil
}

func (r *Record) parse(raw []byte) {

	e := json.Unmarshal(raw, &r.cache)
	if e != nil {
		Log(LOG_ERROR, "e=%s Raw='%s' Record=%+v\n", e, strings.ReplaceAll(string(raw), "\n", ""), r.cache)
		return
	}

	for key, v := range r.cache {
		if f64, ok := v.(float64); ok {
			r.cache[key] = uint64(f64)
		}
	}

}

func (r *Record) caching(c *Column) {

	if r.cache != nil {
		return
	}

	data := r.Raw(c)
	if data == nil {
		Log(LOG_WARN, "fail got data r=%+v\n", r)
		return
	}
	r.parse(data)

}

func (r *Record) Raw(c *Column) (data []byte) {

	path, err := c.Flist.FPath(r.fileID)
	if err != nil {
		Log(LOG_ERROR, "Flist.FPath fail e=%s r=%+v\n", err.Error(), r)
		return nil
	}

	f, err := os.Open(path)
	if err != nil {
		Log(LOG_WARN, "%s should remove from Column.Dirties \n", path)
		// FIXME: remove from Column.Dirties
		return nil
	}
	data = make([]byte, r.size)
	if n, e := f.ReadAt(data, r.offset); e != nil || int64(n) != r.size {
		Log(LOG_WARN, "%s is nvalid data: should remove from Column.Dirties \n", path)
		return nil
	}

	return data

}

func (r *Record) Uint64Value(c *Column) uint64 {
	v, ok := r.cache[c.Name].(uint64)
	_ = ok
	return uint64(v)
}

func (r *Record) ToFbs(inf interface{}) []byte {

	key, ok := inf.(uint64)
	if !ok {
		return nil
	}

	b := flatbuffers.NewBuilder(0)

	vfs_schema.InvertedMapNumStart(b)
	vfs_schema.InvertedMapNumAddKey(b, int64(key))
	vfs_schema.InvertedMapNumAddValue(b, vfs_schema.CreateRecord(b, r.fileID, r.offset, r.size, 0, 0))
	iMapNum := vfs_schema.InvertedMapNumEnd(b)

	vfs_schema.RootStart(b)
	vfs_schema.RootAddVersion(b, 1)
	vfs_schema.RootAddIndexType(b, vfs_schema.IndexInvertedMapNum)
	vfs_schema.RootAddIndex(b, iMapNum)
	b.Finish(vfs_schema.RootEnd(b))
	//vfs_schema.IndexNumStart(b)
	//vfs_schema.IndexIndexNum

	return b.FinishedBytes()

}

func RecordFromFbs(r io.Reader) *Record {
	raws, e := ioutil.ReadAll(r)
	if e != nil {
		return nil
	}
	vRoot := vfs_schema.GetRootAsRoot(raws, 0)
	uTable := new(flatbuffers.Table)
	version := vRoot.Version()
	idxType := vRoot.IndexType()
	_, _ = version, idxType
	vRoot.Index(uTable)

	fbsImap := new(vfs_schema.InvertedMapNum)
	fbsImap.Init(uTable.Bytes, uTable.Pos)
	imapRaw := uTable.Bytes[uTable.Pos:]
	_ = imapRaw
	fbsRecord := fbsImap.Value(nil)

	return &Record{fileID: fbsRecord.FileId(),
		offset: fbsRecord.Offset(),
		size:   fbsRecord.Size(),
	}

}

func (c *Column) caching() {

	path := ColumnPathWithStatus(c.TableDir(), c.Name, true, "*", "*", RECORD_WRITTEN)
	pat := fmt.Sprintf("%s.*.*", path)

	idxfiles, err := filepath.Glob(pat)
	if err != nil {
		Log(LOG_WARN, "%s is not found\n", pat)
	}

	for _, idxpath := range idxfiles {
		strs := strings.Split(filepath.Base(idxpath), ".")
		if len(strs) != 6 {
			continue
		}
		sRange := strs[3]
		fileID, _ := strconv.ParseUint(strs[4], 16, 64)
		offset, _ := strconv.ParseInt(strs[5], 16, 64)

		strs = strings.Split(sRange, "-")
		first, _ := strconv.ParseUint(strs[0], 16, 64)
		last, _ := strconv.ParseUint(strs[1], 16, 64)

		c.cache.caches = append(c.cache.caches,
			&IdxCache{FirstEnd: Range{first: first, last: last},
				Pos: RecordPos{fileID: fileID, offset: offset}})

	}
	sort.Slice(c.cache.caches, func(i, j int) bool {
		return c.cache.caches[i].FirstEnd.first < c.cache.caches[j].FirstEnd.first
	})
}

func (c *Column) cacheToRecord(n int) *Record {
	first := c.cache.caches[n].FirstEnd.first
	last := c.cache.caches[n].FirstEnd.last
	path := ColumnPathWithStatus(c.TableDir(), c.Name, true, toFname(first), toFname(last), RECORD_WRITTEN)
	path = fmt.Sprintf("%s.%010x.%010x", path, c.cache.caches[n].Pos.fileID, c.cache.caches[n].Pos.offset)

	rio, e := os.Open(path)
	if e != nil {
		spew.Dump(c.cache.caches)
		Log(LOG_WARN, "%s column index file not found\n", path)
		return nil
	}
	record := RecordFromFbs(rio)
	rio.Close()
	return record
}

type RecordPos struct {
	fileID uint64
	offset int64
}

type IdxCache struct {
	FirstEnd Range
	Pos      RecordPos
}

type IdxCaches struct {
	caches    []*IdxCache
	negatives []*Range
}

func InitIdxCaches(i *IdxCaches) {
	i.caches = make([]*IdxCache, 0, MAX_IDX_CACHE)
	i.negatives = make([]*Range, 0, MIN_NEGATIVE_CACHE)
}

func NewIdxCaches() *IdxCaches {
	i := &IdxCaches{}
	InitIdxCaches(i)
	return i
}

type SearchMode byte

const (
	SEARCH_INIT SearchMode = iota
	SEARCH_START
	SEARCH_ASC
	SEARCH_DESC
	SEARCH_ALL
	SEARCH_FINISH
)

func (c *Column) Searcher() *Searcher {
	if len(c.cache.caches) == 0 {
		c.caching()
	}
	return &Searcher{
		c:    c,
		low:  0,
		high: len(c.cache.caches) - 1,
		mode: SEARCH_INIT,
	}

}

type Searcher struct {
	c    *Column
	low  int
	high int
	cur  int
	mode SearchMode
}

func (s *Searcher) Do() <-chan *Record {

	ch := make(chan *Record, 10)
	go func() {
		for s.low <= s.high {
			s.cur = (s.low + s.high) / 2

		}

	}()
	return ch

}

func (s *Searcher) Start(fn func(*Record) bool) {

	first := fn(s.c.cacheToRecord(s.low))
	last := fn(s.c.cacheToRecord(s.high))

	if first && !last {
		s.mode = SEARCH_DESC
		s.cur = s.low
		return
	}
	if !first && last {
		s.mode = SEARCH_ASC
		s.cur = s.high
		return
	}
	s.mode = SEARCH_ALL
	return

}

type SearchResult map[string]interface{}

func (s *Searcher) First(fn func(Match) bool) SearchResult {

	if s.mode == SEARCH_INIT {
		s.mode = SEARCH_START
		s.Start(func(r *Record) bool {
			if r == nil {
				Log(LOG_ERROR, "cannot load record!!!\n")
				return false
			}
			r.caching(s.c)
			return fn(Match{mapInf: r.cache})
		})
	}
	if s.mode == SEARCH_ALL {
		for r := range s.searchOnNormal() {
			r.caching(s.c)
			if fn(Match{mapInf: r.cache}) {
				s.mode = SEARCH_FINISH
				return r.cache
			}
		}
	}
	if s.mode == SEARCH_DESC {
		r := s.c.cacheToRecord(s.low)
		r.caching(s.c)
		return r.cache
	}
	if s.mode == SEARCH_ASC {
		r := s.c.cacheToRecord(s.high)
		r.caching(s.c)
		return r.cache
	}

	return nil
}

func (s *Searcher) FindAll(fn func(Match) bool) (results []SearchResult) {

	results = make([]SearchResult, 0, 100)

	if s.mode == SEARCH_INIT {
		s.mode = SEARCH_START
		s.Start(func(r *Record) bool {
			if r == nil {
				Log(LOG_ERROR, "cannot load record!!!\n")
				return false
			}
			r.caching(s.c)
			return fn(Match{mapInf: r.cache})
		})
	}
	switch s.mode {
	case SEARCH_ALL:
		for r := range s.searchOnNormal() {
			r.caching(s.c)
			if fn(Match{mapInf: r.cache}) {
				results = append(results, r.cache)
			}
		}
	case SEARCH_ASC, SEARCH_DESC:
		for r := range s.bsearch(fn) {
			results = append(results, r.cache)
		}
	}

	return
}

func (s *Searcher) searchOnNormal() <-chan *Record {
	ch := make(chan *Record, 10)

	if s.mode != SEARCH_ALL {
		close(ch)
		return ch
	}
	go func() {
		for s.cur = s.low; s.cur <= s.high; s.cur++ {
			if s.mode == SEARCH_FINISH {
				break
			}
			ch <- s.c.cacheToRecord(s.cur)
		}
		s.mode = SEARCH_FINISH
		close(ch)
	}()
	return ch
}

func (s *Searcher) bsearch(fn func(Match) bool) <-chan *Record {
	ch := make(chan *Record, 10)

	if s.mode != SEARCH_ASC && s.mode != SEARCH_DESC {
		close(ch)
		return ch
	}
	go func() {
		checked := map[int]bool{}

		checked[s.cur] = true
		r := s.c.cacheToRecord(s.cur)
		r.caching(s.c)
		ch <- r

		for {
			if s.mode == SEARCH_FINISH {
				break
			}

			s.cur = (s.low + s.high) / 2
			//Log(LOG_DEBUG, "ASC low=%d cur=%d high=%d\n", s.low, s.cur, s.high)
			r = s.c.cacheToRecord(s.cur)
			r.caching(s.c)
			if fn(Match{mapInf: r.cache}) {
				ch <- r
				checked[s.cur] = true
				if s.mode == SEARCH_ASC {
					s.high = s.cur
				}
				if s.mode == SEARCH_DESC {
					s.low = s.cur
				}
			} else {
				checked[s.cur] = false
				if s.mode == SEARCH_ASC {
					s.low = s.cur
				}
				if s.mode == SEARCH_DESC {
					s.high = s.cur
				}
			}
			if s.high <= s.low+1 {
				//s.mode = SEARCH_FINISH
				break
			}
		}

		//Log(LOG_DEBUG, "!ASC low=%d cur=%d high=%d\n", s.low, s.cur, s.high)
		//for cur := s.high; cur < len(s.c.cache.caches); cur++ {
		var cur int
		if s.mode == SEARCH_ASC {
			cur = s.high
		}
		if s.mode == SEARCH_DESC {
			cur = s.low
		}
		for {
			if s.mode == SEARCH_ASC && cur >= len(s.c.cache.caches) {
				break
			}
			if s.mode == SEARCH_DESC && cur <= 0 {
				break
			}
			//Log(LOG_DEBUG, "cur=%d\n", cur)

			if checked[cur] {
				goto NEXT
			}
			r = s.c.cacheToRecord(cur)
			r.caching(s.c)
			ch <- r
		NEXT:
			if s.mode == SEARCH_ASC {
				cur++
			}
			if s.mode == SEARCH_DESC {
				cur--
			}
		}

		s.mode = SEARCH_FINISH
		close(ch)
	}()
	return ch
}

/*
func (c *Column) FirstOnBsearch() {
	s := &Searcher{c: c, low: 0, high: len(c.cache.caches) - 1}

	s.Next()
}
*/
