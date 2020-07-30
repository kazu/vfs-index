package vfsindex

import (
	"context"

	//"encoding/csv"

	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/kazu/fbshelper/query/base"
	"github.com/kazu/vfs-index/query"

	"github.com/kazu/loncha"
	"github.com/kazu/vfs-index/vfs_schema"
)

const (
	RECORDS_INIT = 64
)

const (
	RECORD_WRITING byte = iota
	RECORD_WRITTEN
	RECORD_MERGING
	RECORD_MERGED
)

type Column struct {
	Table   string
	Name    string
	Dir     string
	Flist   *FileList
	IsNum   bool
	Dirties Records

	cache *IdxCaches

	ctx             context.Context
	ctxCancel       context.CancelFunc
	done            chan bool
	isMergeOnSearch bool
}

func ColumnPath(tdir, col string, isNum bool) string {
	if isNum {
		return filepath.Join(Opt.rootDir, tdir, col+".num.idx")

	}

	return filepath.Join(Opt.rootDir, tdir, col+".gram.idx")
}

func JoinExt(s ...string) string {

	return strings.Join(s, ".")

}

func AddingDir(s string, n int) string {

	if s == "*" {
		//return "**/"
		return "*/*/*/"
	}

	if n < 1 {
		n = 2
	}
	var b strings.Builder
	for i := 0; i < len(s); i += n {
		if len(s[i:]) < n {
			b.WriteString(s[i:])
		} else {
			b.WriteString(s[i : i+n])
		}
		b.WriteString("/")
	}
	return b.String()
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
		return fmt.Sprintf("%s.%s-%s", ColumnPath(tdir+"/"+AddingDir(s, 4), col, isNum), s, e)

	case RECORD_MERGING:
		return fmt.Sprintf("%s.merging.%d.%s-%s", ColumnPath(tdir, col, isNum), os.Getgid(), s, e)

	case RECORD_MERGED:
		return fmt.Sprintf("%s.merged.%s-%s", ColumnPath(tdir, col, isNum), s, e)

	}
	return ""

}

func (idx *Indexer) OpenCol(flist *FileList, table, col string) *Column {

	return NewColumn(flist, table, col)
}

func NewColumn(flist *FileList, table, col string) *Column {
	//if _, e := os.Stat(ColumnPath(tableDir)); os.IsNotExist(e) {
	c := &Column{
		Table:   table,
		Name:    col,
		Flist:   flist,
		Dirties: NewRecords(RECORDS_INIT),
		cache:   NewIdxCaches(),
	}
	c.IsNum = c.IsNumViaIndex()
	return c
}

func (c *Column) Update(d time.Duration) error {

	idxDir := filepath.Join(Opt.rootDir, c.Table)
	err := os.MkdirAll(idxDir, os.ModePerm)
	if err != nil {
		return err
	}

	for _, f := range c.Flist.Files {
		if len(f.name) <= len(filepath.Base(c.Table)) {
			continue
		}
		if f.name[0:len(filepath.Base(c.Table))] == c.Table {
			c.updateFile(f)
		}
	}
	c.WriteDirties()
	//Log(LOG_WARN, "Called WriteDirtues \n")
	// FIXME
	ctx, cancel := context.WithTimeout(context.Background(), d)
	//defer cancel()
	go c.MergingIndex(ctx)
	time.Sleep(d)
	cancel()
	<-c.done

	return nil
}

func (c *Column) updateFile(f *File) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for r := range f.Records(ctx, c.Flist.Dir) {
		if !r.IsExist(c) {
			c.Dirties = c.Dirties.Add(r)
		}
	}
}

func (c *Column) WriteDirties() {
	if Opt.cntConcurrent < 1 {
		Opt.cntConcurrent = 1
	}
	s := time.Now()
	Log(LOG_WARN, "Indexing %d concurrent\n", Opt.cntConcurrent)
	ch := make(chan int, Opt.cntConcurrent*4)
	chDone := make(chan bool, Opt.cntConcurrent)

	//bar := progressbar.Default(int64(len(c.Dirties)))
	bar := Pbar.Add("write index...", len(c.Dirties))
	defer func() {
		bar.SetTotal(bar.Current(), true)
	}()

	writeRecord := func(ch chan int) {
		for {
			i, ok := <-ch
			if !ok || i < 0 {
				break
			}
			r := c.Dirties[i]
			e := r.Write(c)
			if e == nil {
				c.Dirties[i] = nil
				bar.Increment()
			}
		}
		chDone <- true
	}
	for i := 0; i < Opt.cntConcurrent; i++ {
		go writeRecord(ch)
	}
	go func() {
		for i := range c.Dirties {
			ch <- i
		}
		for i := 0; i < Opt.cntConcurrent; i++ {
			ch <- -1
		}
	}()

	for i := 0; i < Opt.cntConcurrent; i++ {
		<-chDone
	}
	loncha.Delete(&c.Dirties, func(i int) bool {
		return c.Dirties[i] == nil
	})
	d := time.Now().Sub(s)
	Log(LOG_WARN, "WriteDiries() elapsed %s %d\n", d, len(c.Dirties))
}

func (c *Column) cancelAndWait() {
	if c.ctx != nil {
		c.ctxCancel()
		<-c.done
	}
}

func (c *Column) getIdxWriter() IdxWriter {

	var idxWriter IdxWriter

	if !c.IsNum {
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
	return idxWriter
}

func (c *Column) MergingIndex(ctx context.Context) error {

	var idxWriter IdxWriter

	if c.IsNumViaIndex() {
		c.IsNum = true
	}

	if !c.IsNum {
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
	return c.mergeIndex(idxWriter, ctx)

}
func (c *Column) noMergedPat() string {
	path := ColumnPathWithStatus(c.TableDir(), c.Name, c.IsNum, "*", "*", RECORD_WRITTEN)
	pat := fmt.Sprintf("%s.*.*", path)
	return pat
}

// func (c *Column) noMergedFPathNoCh() (idxfiles <-chan string, err error) {
// 	path := ColumnPathWithStatus(c.TableDir(), c.Name, c.IsNum, "*", "*", RECORD_WRITTEN)
// 	pat := fmt.Sprintf("%s.*.*", path)

// 	return paraGlobDebug(pat, true), nil
//}

func (c *Column) noMergedFPath() (idxfiles <-chan string, err error) {
	path := ColumnPathWithStatus(c.TableDir(), c.Name, c.IsNum, "*", "*", RECORD_WRITTEN)
	pat := fmt.Sprintf("%s.*.*", path)

	return paraGlobDebug(pat, true), nil
}

func (c *Column) noMergedFPathWithPat() (idxfiles <-chan string, pat string, err error) {
	path := ColumnPathWithStatus(c.TableDir(), c.Name, c.IsNum, "*", "*", RECORD_WRITTEN)
	pat = fmt.Sprintf("%s.*.*", path)

	return paraGlobDebug(pat, false), pat, nil
}

func idxPath2Info(idxpath string) (fileID uint64, offset int64, first uint64, last uint64) {

	strs := strings.Split(filepath.Base(idxpath), ".")
	if len(strs) < 5 {
		return
	}
	if len(strs) == 5 {
		return idxPath2InfoMerge(idxpath)
	}

	sRange := strs[3]

	fileID, _ = strconv.ParseUint(strs[4], 16, 64)
	offset, _ = strconv.ParseInt(strs[5], 16, 64)

	strs = strings.Split(sRange, "-")
	first, _ = strconv.ParseUint(strs[0], 16, 64)
	last, _ = strconv.ParseUint(strs[1], 16, 64)

	return

}

func idxPath2InfoMerge(idxpath string) (fileID uint64, offset int64, first uint64, last uint64) {

	strs := strings.Split(filepath.Base(idxpath), ".")
	if len(strs) != 5 {
		return
	}
	sRange := strs[4]

	strs = strings.Split(sRange, "-")
	first, _ = strconv.ParseUint(strs[0], 16, 64)
	last, _ = strconv.ParseUint(strs[1], 16, 64)

	return

}

type IndexPathInfo struct {
	fileID uint64
	offset int64
	first  uint64
	last   uint64
}

func NewIndexInfo(fileID uint64, offset int64, first uint64, last uint64) IndexPathInfo {
	return IndexPathInfo{
		fileID: fileID,
		offset: offset,
		first:  first,
		last:   last,
	}
}

func (c *Column) IsNumViaIndex() bool {

	var file *File
	if len(c.Flist.Files) == 0 {
		c.Flist.Reload()
	}

	for _, f := range c.Flist.Files {
		if len(f.name) <= len(filepath.Base(c.Table)) {
			continue
		}
		if f.name[0:len(filepath.Base(c.Table))] == c.Table {
			file = f
			break
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := <-file.Records(ctx, c.Flist.Dir)

	r.caching(c)
	if _, ok := r.cache[c.Name].(string); ok {
		return false
	}
	return true

}

func (c *Column) Path() string {
	return ColumnPath(c.TableDir(), c.Name, c.IsNum)
}

func (c *Column) loadIndex() error {
	if c.IsNumViaIndex() {
		c.IsNum = true
	}

	path := ColumnPathWithStatus(c.TableDir(), c.Name, c.IsNum, "*", "*", RECORD_MERGED)
	pat := fmt.Sprintf("%s", path)
	if !hasGlobCache(pat) {
		ch := paraGlob(pat)
		for range ch {
		}
	}

	//	cnt := globCacheInstance.Get(pat).Count()
	gCache := globCacheInstance.Get(pat)
	cnt := gCache.Count()

	for i := 0; i < cnt; i++ {
		file := string(query.PathInfoSingle(gCache.At(i)).Path().Bytes())
		first := NewIndexInfo(idxPath2Info(file)).first
		last := NewIndexInfo(idxPath2Info(file)).last
		c.cache.infos = append(c.cache.infos,
			&IdxInfo{
				path:  file,
				first: first,
				last:  last,
			})
	}

	if cnt == 0 {
		Log(LOG_WARN, "loadIndex() %s is not found. force creation ctx=%v\n", pat, c.ctx)
		if c.ctx == nil {
			c.ctx, c.ctxCancel = context.WithTimeout(context.Background(), 1*time.Minute)
		}
		go c.MergingIndex(c.ctx)
		return ErrNotFoundFile
	}

	return nil

}
func (c *Column) mergeIndex(w IdxWriter, ctx context.Context) error {

	c.done = make(chan bool, 2)
	defer func() {
		Log(LOG_DEBUG, "mergeIndex() done\n")
		close(c.done)
	}()
	Log(LOG_DEBUG, "mergeIndex() start\n")

	root := query.NewRoot()
	root.SetVersion(query.FromInt32(1))
	root.WithHeader()
	root.SetIndexType(query.FromByte(byte(vfs_schema.IndexIndexNum)))
	root.Flatten()

	idxNum := query.NewIndexNum()
	idxNum.Base = base.NewNoLayer(idxNum.Base)
	keyrecords := query.NewKeyRecordList()
	keyrecords.Base = base.NewNoLayer(keyrecords.Base)

	var keyRecord *query.KeyRecord
	var recs *query.RecordList
	var firstPath *IndexFile
	var lastPath *IndexFile
	noMergeIdxFiles := []*IndexFile{}

	total := 1000
	bar := Pbar.Add("merge index", 1000)

	finder := OpenIndexFile(c)

	i := 0

	finder.Select(
		OptAsc(true),
		OptCcondFn(func(f *IndexFile) CondType {
			if f.Ftype == IdxFileType_None {
				return CondSkip
			}
			if f.IsType(IdxFileType_NoComplete) {
				return CondSkip
			} else if f.IsType(IdxFileType_Merge) {
				return CondSkip
			} else if f.IsType(IdxFileType_Dir) {
				return CondLazy
			}

			return CondTrue
		}),
		OptTraverse(func(f *IndexFile) error {
			if firstPath == nil {
				firstPath = f
			}
			lastPath = f
			noMergeIdxFiles = append(noMergeIdxFiles, f)
			bar.Increment()
			i++

			if i > total-10 {
				total += total / 4
				bar.SetTotal(int64(total), false)
			}

			kr := f.KeyRecord()
			if kr == nil {
				Log(LOG_WARN, "mergeIndex(): %s column index file not found\n", f.Path)
				return nil
			}
			if keyRecord == nil || f.IdxInfo().first != keyRecord.Key().Uint64() {
				if keyRecord != nil {
					cnt := keyrecords.Count()
					e := keyRecord.SetRecords(recs.CommonNode)
					if e != nil {
						Log(LOG_ERROR, "mergeIndex(): %s fail to set to records\n", f.Path)
						return e
					}
					keyRecord.Flatten()
					e = keyrecords.SetAt(cnt, keyRecord)
					if e != nil {
						Log(LOG_ERROR, "mergeIndex(): %s fail to set to keyrecords cnt=%d\n", f.Path, cnt)
						return e
					}
				}
				keyRecord = query.NewKeyRecord()
				keyRecord.Base = base.NewNoLayer(keyRecord.Base)
				keyRecord.SetKey(query.FromUint64(f.IdxInfo().first))

			}

			cnt := keyRecord.Records().Count()
			if cnt == 0 {
				recs = query.NewRecordList()
				recs.Base = base.NewNoLayer(recs.Base)
			} else {
				recs = keyRecord.Records()
			}
			e := recs.SetAt(cnt, kr.Value())
			if e != nil {
				Log(LOG_ERROR, "mergeIndex(): %s fail to set to recs cnt=%d\n", f.Path, cnt)
				return e
			}

			select {
			case <-ctx.Done():
				Log(LOG_WARN, "mergeIndex cancel() last_merge=%s\n", f.Path)

				return ErrStopTraverse
			default:
			}

			return nil
		}),
	)
	defer func() {
		bar.SetTotal(int64(i), true)
		//Pbar.wg.Done()
	}()

	cnt := keyrecords.Count()
	if cnt == 0 {
		Log(LOG_WARN, "mergeIndex no write\n")
		return nil
	}

	if query.KeyRecordSingle(keyrecords.At(cnt-1)).Key().Uint64() != keyRecord.Key().Uint64() {
		keyRecord.SetRecords(recs.CommonNode)
		keyRecord.Flatten()
		keyrecords.SetAt(cnt, keyRecord)
	}
	keyrecords.Flatten()
	idxNum.SetIndexes(keyrecords.CommonNode)

	root.SetIndex(idxNum.CommonNode)
	root.Flatten()

	vname := func(key uint64) string {

		if c.IsNum {
			return toFname(key)
		}
		return fmt.Sprintf("%012x", key)
	}

	first := firstPath.IdxInfo().first
	last := lastPath.IdxInfo().last

	wIdxPath := ColumnPathWithStatus(c.TableDir(), c.Name, w.IsNum, vname(first), vname(last), RECORD_MERGING)
	path := ColumnPathWithStatus(c.TableDir(), c.Name, w.IsNum, vname(first), vname(last), RECORD_MERGED)

	io, e := os.Create(wIdxPath)
	if e != nil {
		Log(LOG_WARN, "F:mergeIndex() cannot create... %s\n", wIdxPath)
		return e
	}
	defer io.Close()

	io.Write(root.R(0))
	Log(LOG_DEBUG, "S: written %s \n", wIdxPath)
	e = SafeRename(wIdxPath, path)
	if e != nil {
		os.Remove(wIdxPath)
		Log(LOG_DEBUG, "F: rename %s -> %s \n", wIdxPath, path)
		return e
	}

	Log(LOG_DEBUG, "S: renamed %s -> %s \n", wIdxPath, path)

	// remove merged file
	for _, f := range noMergeIdxFiles {
		os.Remove(f.Path)
	}
	Log(LOG_DEBUG, "S: remove merged files count=%d \n", len(noMergeIdxFiles))

	return nil
}

func (c *Column) RecordEqInt(v int) (record *Record) {

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

func (c *Column) TableDir() string {

	return filepath.Join(c.Dir, c.Table)
}

func (c *Column) caching() (e error) {

	e = c.loadIndex()
	// if e == nil {
	// 	return
	// }

	if c.IsNumViaIndex() {
		c.IsNum = true
	}
	if c.IsNum {
		e = c.cachingNum()
	} else {
		e = c.cachingTri()
		return
	}
	return
}
func (c *Column) cachingTri() (e error) {
	Log(LOG_DEBUG, "cachingTri() start\n")
	defer Log(LOG_DEBUG, "cachingTri() done\n")
	pat := c.noMergedPat()

	if !hasGlobCache(pat) {
		ch, e := c.noMergedFPath()
		if e != nil {
			return e
		}
		for range ch {

		}
	}
	list := globCacheInstance.Get(pat)

	cnt := list.Count()
	if cnt == 0 {
		Log(LOG_WARN, "cachingTri(): %s is not found\n", pat)
		return ErrNotFoundFile
	}
	Log(LOG_WARN, "cachingTri(): %s is found, len=%d\n", pat, cnt)
	c.cache.caches.pat = pat

	c.cache.caches.cnt = cnt
	c.cache.caches.datas = make([]*IdxCache, c.cache.caches.cnt)
	datafirst, e := c.cachingTriBy(0)
	if e != nil {
		return e
	}
	c.cache.caches.datas[0] = datafirst
	last := c.cache.caches.cnt - 1
	c.cache.caches.datas[last], _ = c.cachingTriBy(last)

	return nil
}

func (c *Column) cachingTriBy(n int) (ic *IdxCache, e error) {

	idxpath := string(query.PathInfoSingle(globCacheInstance.Get(c.noMergedPat()).At(n)).Path().Bytes())
	if len(idxpath) == 0 {
		Log(LOG_WARN, "cachingTriBy(%d):  not found\n", n)
		return nil, ErrNotFoundFile
	}

	//FIXME: use idxPath2Info()
	strs := strings.Split(filepath.Base(idxpath), ".")
	if len(strs) != 6 {
		return nil, ErrInvalidIdxName
	}
	sRange := strs[3]
	fileID, _ := strconv.ParseUint(strs[4], 16, 64)
	offset, _ := strconv.ParseInt(strs[5], 16, 64)

	strs = strings.Split(sRange, "-")
	first, _ := strconv.ParseUint(strs[0], 16, 64)
	last, _ := strconv.ParseUint(strs[1], 16, 64)

	return &IdxCache{FirstEnd: Range{first: first, last: last},
		Pos: RecordPos{fileID: fileID, offset: offset}}, nil

}

func (c *Column) cachingNum() (e error) {

	path := ColumnPathWithStatus(c.TableDir(), c.Name, true, "*", "*", RECORD_WRITTEN)
	pat := fmt.Sprintf("%s.*.*", path)
	//idxfiles, err := filepath.Glob(pat)
	if !hasGlobCache(pat) {
		ch := paraGlob(pat)
		for range ch {

		}
	}
	cnt := globCacheInstance.Get(pat).Count()

	if cnt == 0 {
		Log(LOG_WARN, "%s is not found\n", pat)
		return ErrNotFoundFile
	}

	c.cache.caches.pat = pat
	c.cache.caches.cnt = cnt
	c.cache.caches.datas = make([]*IdxCache, c.cache.caches.cnt)
	if c.cache.caches.datas[0], e = c.cachingNumBy(0); e != nil {
		return e
	}
	last := c.cache.caches.cnt - 1
	c.cache.caches.datas[last], _ = c.cachingNumBy(last)

	return nil
}

func (c *Column) cachingNumBy(n int) (ic *IdxCache, e error) {

	pat := c.cache.caches.pat
	//fmt.Sprintf("%s.*.*", path)
	idxpath := string(query.PathInfoSingle(globCacheInstance.Get(c.noMergedPat()).At(n)).Path().Bytes())

	if len(idxpath) == 0 {
		Log(LOG_WARN, "%s is not found\n", pat)
		return nil, ErrNotFoundFile
	}

	strs := strings.Split(filepath.Base(idxpath), ".")
	if len(strs) != 6 {
		return nil, ErrInvalidIdxName
	}
	sRange := strs[3]
	fileID, _ := strconv.ParseUint(strs[4], 16, 64)
	offset, _ := strconv.ParseInt(strs[5], 16, 64)

	strs = strings.Split(sRange, "-")
	first, _ := strconv.ParseUint(strs[0], 16, 64)
	last, _ := strconv.ParseUint(strs[1], 16, 64)

	return &IdxCache{FirstEnd: Range{first: first, last: last},
		Pos: RecordPos{fileID: fileID, offset: offset}}, nil

}

func (c *Column) keys(n int) (uint64, uint64) {
	if len(c.cache.infos) > 0 {
		pos := c.head(n)
		return pos, pos
	}
	return c.getRowCache(n).FirstEnd.first, c.getRowCache(n).FirstEnd.last
	//return c.cache.caches[n].FirstEnd.first, c.cache.caches[n].FirstEnd.last
}

func (c *Column) cacheToRecords(n int) (record []*Record) {

	if len(c.cache.infos) == 0 {
		return []*Record{c.cacheToRecord(n)}
	}

	if c.cache.countInInfos() <= n {
		return []*Record{c.cacheToRecord(n - c.cache.countInInfos())}
	}

	//first := c.cache.head(n)
	return c.cache.records(n)

}
func (c *Column) cacheToRecord(n int) *Record {

	first := c.getRowCache(n).FirstEnd.first
	last := c.getRowCache(n).FirstEnd.last

	path := ColumnPathWithStatus(c.TableDir(), c.Name, c.IsNum, toFname(first), toFname(last), RECORD_WRITTEN)
	if !c.IsNum {
		path = ColumnPathWithStatus(c.TableDir(), c.Name, c.IsNum, toFnameTri(first), toFnameTri(last), RECORD_WRITTEN)
	}

	path = fmt.Sprintf("%s.%010x.%010x", path, c.getRowCache(n).Pos.fileID, c.getRowCache(n).Pos.offset)

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
	infos     []*IdxInfo
	caches    RowIndex
	negatives []*Range
}

type RowIndex struct {
	cnt      int
	pat      string
	firstEnd Range
	datas    []*IdxCache
}

type IdxInfo struct {
	path  string
	first uint64
	last  uint64
	buf   []byte
}

func (c *Column) getRowCache(n int) *IdxCache {
	r := &c.cache.caches
	if r.datas[n] != nil {
		return r.datas[n]
	}
	if c.IsNum {
		r.datas[n], _ = c.cachingNumBy(n)
	} else {
		r.datas[n], _ = c.cachingTriBy(n)
	}
	return r.datas[n]
}

func (info *IdxInfo) load(force bool) {

	if !force && len(info.buf) > 0 {
		return
	}
	f, e := os.Open(info.path)
	if e != nil {
		return
	}
	info.buf, e = ioutil.ReadAll(f)
}

func (col *Column) head(n int) uint64 {
	c := col.cache
	if c.countInInfos() <= n {
		return col.getRowCache(n - c.countInInfos()).FirstEnd.first
	}
	cur := 0

	for _, info := range c.infos {
		info.load(false)
		root := query.OpenByBuf(info.buf)
		if cur+root.Index().IndexNum().Indexes().Count() <= n {
			cur += root.Index().IndexNum().Indexes().Count()
			continue
		}
		pos := n - cur //cur + root.Index().IndexNum().Indexes().Count() - n

		return query.KeyRecordSingle(root.Index().IndexNum().Indexes().At(pos)).Key().Uint64()
	}
	return 0
}

// func (c *Column) cntOfValue(info *InfoRange) int {
// 	// if c.cache.countInInfos() <= n {
// 	// 	return int(c.cache.caches[n-c.cache.countInInfos()].FirstEnd.last + 1 - c.cache.caches[n-c.cache.countInInfos()].FirstEnd.first)
// 	// }

// 	if info.end <= c.cache.countInInfos() {
// 		return c.cntOfValueOnMerged(info)
// 	} else if info.start > c.cache.countInInfos() {
// 		return info.end - info.start
// 	}

// 	return c.cntOfValue(&InfoRange{start: info.start, end: c.cache.countInInfos() - 1}) +
// 		info.end - c.cache.countInInfos()
// }

// func (c *Column) cntOfValueOnMerged(info *InfoRange) int {

// 	return 0

// }

func (c *Column) cRecordlist(n int) (records *query.RecordList) {
	if c.cache.countInInfos() <= n {
		i := n - c.cache.countInInfos()
		r := c.cacheToRecord(i)
		//cur := c.cache.caches[n-c.cache.countInInfos()]
		//r :=
		rec := query.NewRecord()
		rec.SetFileId(query.FromUint64(r.fileID))
		rec.SetOffset(query.FromInt64(r.offset))
		rec.SetSize(query.FromInt64(r.size))
		rec.SetOffsetOfValue(query.FromInt32(0))
		rec.SetValueSize(query.FromInt32(0))

		records := query.NewRecordList()
		records.SetAt(0, rec)
		return records
	}

	//i := n - c.cache.countInInfos()

	return c.cache.recordlist(n)

}

func (c *Column) Key2Path(key uint64, state byte) string {

	strkey := toFnameTri(key)
	if c.IsNum {
		strkey = toFname(key)
	}
	return ColumnPathWithStatus(c.TableDir(), c.Name, c.IsNum, strkey, strkey, state)

}

func (c *IdxCaches) recordlist(n int) (records *query.RecordList) {

	cur := 0
	for _, info := range c.infos {
		info.load(false)
		root := query.OpenByBuf(info.buf)
		if cur+root.Index().IndexNum().Indexes().Count() <= n {
			cur += root.Index().IndexNum().Indexes().Count()
			continue
		}
		//pos := cur + root.Index().IndexNum().Indexes().Count() - n
		pos := n - cur
		//recods = make([]*Record,
		return query.KeyRecordSingle(root.Index().IndexNum().Indexes().At(pos)).Records()
	}
	return nil

}

func (c *IdxCaches) records(n int) (records []*Record) {

	list := c.recordlist(n)
	records = make([]*Record, list.Count())

	for i, rec := range list.All() {
		//buf := rec.R(rec.Node.Pos)
		records[i] = &Record{
			fileID: rec.FileId().Uint64(),
			offset: rec.Offset().Int64(),
			size:   rec.Size().Int64(),
		}
	}
	return
}

func (c *IdxCaches) countInInfos() int {

	cnt := 0
	for _, info := range c.infos {
		info.load(false)
		root := query.OpenByBuf(info.buf)
		cnt += root.Index().IndexNum().Indexes().Count()
	}
	return cnt

}

func (c *IdxCaches) countOfKeys() int {
	if len(c.infos) == 0 {
		return c.caches.cnt
	}

	return c.countInInfos() + c.caches.cnt

}

func InitIdxCaches(i *IdxCaches) {
	//i.caches = make([]*IdxCache, 0, MAX_IDX_CACHE)
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

// Searcher ... return Search object for search operation
func (c *Column) Searcher() *Searcher {
	//if len(c.cache.caches) == 0 {
	if c.cache.countOfKeys() == 0 || c.cache.caches.cnt == 0 {
		c.ctx, c.ctxCancel = context.WithTimeout(context.Background(), 1*time.Minute)
		c.caching()
	} else if c.ctx == nil && c.isMergeOnSearch {
		c.ctx, c.ctxCancel = context.WithTimeout(context.Background(), 1*time.Minute)
		go c.MergingIndex(c.ctx)
		time.Sleep(200 * time.Millisecond)
	}
	return &Searcher{
		c:    c,
		low:  0,
		high: c.cache.countOfKeys() - 1,
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

func (s *Searcher) start(fn func(*Record, uint64) bool) {

	firstKey, _ := s.c.keys(s.low)
	_, lastKey := s.c.keys(s.high)

	frec := s.c.cacheToRecords(s.low)[0]
	lrec := s.c.cacheToRecords(s.high)[len(s.c.cacheToRecords(s.high))-1]
	first := fn(frec, firstKey)
	last := fn(lrec, lastKey)

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

type OldSearchResult map[string]interface{}
type SearchResult string

type KeyRecord struct {
	key    uint64
	record *Record
}

type ResultInfoRange struct {
	s     *Searcher
	start int
	end   int
}

func (info ResultInfoRange) Start() OldSearchResult {

	s := info.s
	recods := info.s.c.cacheToRecords(info.start)
	r := recods[0]

	r.caching(s.c)
	return r.cache
}

func (info ResultInfoRange) Last() OldSearchResult {

	s := info.s
	records := info.s.c.cacheToRecords(info.end)
	r := records[len(records)-1]

	r.caching(s.c)
	return r.cache
}
