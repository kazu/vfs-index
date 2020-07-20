package vfsindex

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	query "github.com/kazu/vfs-index/qeury"

	"github.com/davecgh/go-spew/spew"
	"github.com/kazu/loncha"
	"github.com/kazu/vfs-index/vfs_schema"
	"github.com/schollz/progressbar/v3"
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
		// <column name>.<index type>.idx.adding.<pid>.<start>-<end>
		//   <index type> ...  num or tri ?
		//   <start>,<end>  ... value
		return fmt.Sprintf("%s.adding.%d.%s-%s", ColumnPath(tdir, col, isNum), os.Getgid(), s, e)
	case RECORD_WRITTEN:
		// <column name>.<index type>.<start>-<end>.<inode number>.<offset>
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
	//Log(LOG_WARN, "Called WriteDirtues \n")

	return nil
}

func (c *Column) updateFile(f *File) {

	for r := range f.Records(c.Flist.Dir) {
		c.Dirties = c.Dirties.Add(r)
	}
}

func (c *Column) WriteDirties() {
	// if CurrentLogLoevel < LOG_DEBUG {
	// 	fmt.Fprint(os.Stderr, "writing index...")
	// }
	bar := progressbar.Default(int64(len(c.Dirties)))

	for _, r := range c.Dirties {
		if r.Write(c) == nil {
			loncha.Delete(c.Dirties, func(i int) bool {
				return c.Dirties[i].fileID == r.fileID &&
					c.Dirties[i].offset == r.offset
			})
		}
		bar.Add(1)
	}
	// if CurrentLogLoevel < LOG_DEBUG {
	// 	fmt.Fprint(os.Stderr, "done\n")
	// }
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

func toFnameTri(i interface{}) string {
	return fmt.Sprintf("%012x", i)
}

func (c *Column) TableDir() string {

	return filepath.Join(c.Dir, c.Table)
}

type IdxWriter struct {
	IsNum        bool
	ValueEncoder func(r *Record) []string
}

func EncodeTri(s string) (result []string) {

	runes := []rune(s)

	for idx := range runes {
		if idx > len(runes)-3 {
			break
		}
		Log(LOG_DEBUG, "tri-gram %s\n", string(runes[idx:idx+3]))
		result = append(result,
			fmt.Sprintf("%04x%04x%04x", runes[idx], runes[idx+1], runes[idx+2]))
	}

	return
}

func (r *Record) Write(c *Column) error {

	var idxWriter IdxWriter
	r.caching(c)

	if _, ok := r.cache[c.Name].(string); ok {
		idxWriter = IdxWriter{
			IsNum: false,
			ValueEncoder: func(r *Record) (results []string) {
				//return []string{toFname(r.Uint64Value(c))}
				return EncodeTri(r.StrValue(c))
			},
		}
	} else {
		idxWriter = IdxWriter{
			IsNum: true,
			ValueEncoder: func(r *Record) (results []string) {
				return []string{toFname(r.Uint64Value(c))}
			},
		}
	}
	return r.write(c, idxWriter)

}

func (r *Record) write(c *Column, w IdxWriter) error {

	for _, vName := range w.ValueEncoder(r) {

		wPath := ColumnPathWithStatus(c.TableDir(), c.Name, w.IsNum, vName, vName, RECORD_WRITING)
		path := ColumnPathWithStatus(c.TableDir(), c.Name, w.IsNum, vName, vName, RECORD_WRITTEN)
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
		if w.IsNum && r.Uint64Value(c) == 0 {
			spew.Dump(r)
		}

		e = SafeRename(wPath, path)
		if e != nil {
			os.Remove(wPath)
			Log(LOG_DEBUG, "F: rename %s -> %s \n", wPath, path)
			return e
		}
		Log(LOG_DEBUG, "S: renamed%s -> %s \n", wPath, path)

	}

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
	//Log(LOG_DEBUG, "decode %v \n", r.cache)
	return
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

func (r *Record) StrValue(c *Column) string {
	v, ok := r.cache[c.Name].(string)
	_ = ok
	return v
}

func (r *Record) ToFbs(inf interface{}) []byte {

	key, ok := inf.(uint64)
	if !ok {
		return nil
	}

	root := query.NewRoot()
	root.SetVersion(query.FromInt32(1))
	root.WithHeader()

	inv := query.NewInvertedMapNum()
	inv.SetKey(query.FromInt64(int64(key)))

	rec := query.NewRecord()
	rec.SetFileId(query.FromUint64(r.fileID))
	rec.SetOffset(query.FromInt64(r.offset))
	rec.SetSize(query.FromInt64(r.size))
	rec.SetOffsetOfValue(query.FromInt32(0))
	rec.SetValueSize(query.FromInt32(0))

	inv.SetValue(rec.CommonNode)

	root.SetIndexType(query.FromByte(byte(vfs_schema.IndexInvertedMapNum)))
	root.SetIndex(inv.CommonNode)

	// FIXME return io writer ?
	root.Merge()
	return root.R(0)
}

func RecordFromFbs(r io.Reader) *Record {
	root := query.Open(r, 512)

	rec := root.Index().InvertedMapNum().Value()

	return &Record{fileID: rec.FileId().Uint64(),
		offset: rec.Offset().Int64(),
		size:   rec.Size().Int64(),
	}
}

func (c *Column) caching() (e error) {
	e = c.cachingNum()

	if e != nil {
		c.IsNum = false
		e = c.cachingTri()
		return
	}
	c.IsNum = true
	return
}
func (c *Column) cachingTri() (e error) {
	path := ColumnPathWithStatus(c.TableDir(), c.Name, false, "*", "*", RECORD_WRITTEN)
	pat := fmt.Sprintf("%s.*.*", path)

	idxfiles, err := filepath.Glob(pat)
	if err != nil || len(idxfiles) == 0 {
		Log(LOG_WARN, "column.cachingTri(): %s is not found\n", pat)
		return ErrNotFoundFile
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

	return

}
func (c *Column) cachingNum() (e error) {

	path := ColumnPathWithStatus(c.TableDir(), c.Name, true, "*", "*", RECORD_WRITTEN)
	pat := fmt.Sprintf("%s.*.*", path)

	idxfiles, err := filepath.Glob(pat)
	if err != nil || len(idxfiles) == 0 {
		Log(LOG_WARN, "%s is not found\n", pat)
		return ErrNotFoundFile
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

	return
}

func (c *Column) keys(n int) (uint64, uint64) {
	return c.cache.caches[n].FirstEnd.first, c.cache.caches[n].FirstEnd.last
}

func (c *Column) cacheToRecord(n int) *Record {

	first := c.cache.caches[n].FirstEnd.first
	last := c.cache.caches[n].FirstEnd.last
	path := ColumnPathWithStatus(c.TableDir(), c.Name, c.IsNum, toFname(first), toFname(last), RECORD_WRITTEN)
	if !c.IsNum {
		path = ColumnPathWithStatus(c.TableDir(), c.Name, c.IsNum, toFnameTri(first), toFnameTri(last), RECORD_WRITTEN)
	}

	path = fmt.Sprintf("%s.%010x.%010x", path, c.cache.caches[n].Pos.fileID, c.cache.caches[n].Pos.offset)

	rio, e := os.Open(path)
	if e != nil {
		//spew.Dump(c.cache.caches)
		Log(LOG_WARN, "Column.cacheToRecord(): %s column index file not found\n", path)
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

func (s *Searcher) Start(fn func(*Record, uint64) bool) {

	firstKey, _ := s.c.keys(s.low)
	_, lastKey := s.c.keys(s.high)

	first := fn(s.c.cacheToRecord(s.low), firstKey)
	last := fn(s.c.cacheToRecord(s.high), lastKey)

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

type KeyRecord struct {
	key    uint64
	record *Record
}

type ResultInfoRange struct {
	s     *Searcher
	start int
	end   int
}

func (info ResultInfoRange) Start() SearchResult {

	s := info.s
	r := info.s.c.cacheToRecord(info.start)
	r.caching(s.c)
	return r.cache
}

func (info ResultInfoRange) Last() SearchResult {

	s := info.s
	r := info.s.c.cacheToRecord(info.end)
	r.caching(s.c)
	return r.cache
}
