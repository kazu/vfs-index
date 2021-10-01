package vfsindex

import (
	"context"
	"sync"

	//"encoding/csv"

	"fmt"
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
		return fmt.Sprintf("%s.adding.%d.%s-%s", ColumnPath(tdir, col, isNum), time.Now().UnixNano(), s, e)
	case RECORD_WRITTEN:
		// <column name>.<index type>.<start>-<end>.<inode number>.<offset>
		return fmt.Sprintf("%s.%s-%s", ColumnPath(tdir+"/"+AddingDir(s, 4), col, isNum), s, e)

	case RECORD_MERGING:
		return fmt.Sprintf("%s.merging.%d.%s-%s", ColumnPath(tdir, col, isNum), time.Now().UnixNano(), s, e)

	case RECORD_MERGED:
		return fmt.Sprintf("%s.merged.%s-%s", ColumnPath(tdir, col, isNum), s, e)

	}
	return ""

}

func (idx *Indexer) openCol(flist *FileList, table, col string) *Column {

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
	c.IsNum = c.validateIndexType()
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
	go c.mergeIndex(ctx, nil)
	time.Sleep(d)
	cancel()
	<-c.done

	return nil
}

func (c *Column) updateFile(f *File) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for r := range f.Records(ctx, c.Flist.Dir) {
		Log(LOG_DEBUG, "record info record=%+v\n", r)
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

func (c *Column) mergeIndex(ctx context.Context, wg *sync.WaitGroup) error {

	var idxWriter IdxWriter

	if c.validateIndexType() {
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
	e := c.baseMergeIndex(idxWriter, ctx)
	if wg != nil {
		wg.Done()
	}
	return e
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

func (c *Column) validateIndexType() bool {

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

func (c *Column) baseMergeIndex(w IdxWriter, ctx context.Context) error {

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

	mergedKeyRecords := keyrecords
	mergedKeyRecords.Base = keyrecords.Base
	mergedKeyId2Records := map[uint64]*query.RecordList{}

	var firstPath *IndexFile
	var lastPath *IndexFile
	noMergeIdxFiles := []*IndexFile{}

	total := 1000
	bar := Pbar.Add("merge index", 1000)

	finder := OpenIndexFile(c)

	i := 0

	findKr := func(l *query.KeyRecordList, kr *query.InvertedMapNum) int {
		if l.Count() == 0 {
			return -1
		}
		idx := l.SearchIndex(func(q *query.KeyRecord) bool {
			return q.Key().Uint64() >= kr.Key().Uint64()
		})
		if idx == -1 {
			return idx
		}
		if lastMergedKr := l.AtWihoutError(idx); lastMergedKr != nil && lastMergedKr.Key().Uint64() != kr.Key().Uint64() {
			return -1
		}
		return idx
	}
	_ = findKr

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
			if mergedKeyId2Records[kr.Key().Uint64()] == nil {
				mergedKeyId2Records[kr.Key().Uint64()] = query.NewRecordList()
			}
			cnt := mergedKeyId2Records[kr.Key().Uint64()].Count()
			mergedKeyId2Records[kr.Key().Uint64()].SetAt(cnt, kr.Value())

			select {
			case <-ctx.Done():
				Log(LOG_WARN, "mergeIndex cancel() last_merge=%s\n", f.Path)

				return ErrStopTraverse
			default:
			}

			return nil
		}),
	)

	calledDefer := false
	deferFn := func() {
		bar.SetTotal(int64(i), true)
		calledDefer = true
	}

	defer func() {
		if calledDefer {
			deferFn()
		}
	}()

	keys := make([]uint64, len(mergedKeyId2Records))
	for key := range mergedKeyId2Records {
		keys = append(keys, key)
	}

	deferFn()

	wBar := Pbar.Add("write merge file", len(keys))

	for i, key := range keys {
		recs := mergedKeyId2Records[key]
		recs.Flatten()
		kr := query.NewKeyRecord()
		kr.Base = base.NewNoLayer(kr.Base)
		kr.SetKey(query.FromUint64(key))
		kr.SetRecords(recs)
		kr.Flatten()
		mergedKeyRecords.SetAt(i, kr)
		wBar.Increment()
	}
	wBar.SetTotal(int64(len(keys)), true)

	cnt := keyrecords.Count()
	if cnt == 0 {
		Log(LOG_WARN, "mergeIndex no write\n")
		return nil
	}

	mergedKeyRecords.Flatten()
	idxNum.SetIndexes(mergedKeyRecords)

	root.SetIndex(&query.Index{CommonNode: idxNum.CommonNode})
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
	for _, f := range noMergeIdxFiles {
		Log(LOG_DEBUG, "S: \tmerged-indexes %+v \n", f.Path)
	}
	e = SafeRename(wIdxPath, path)
	if e != nil {
		os.Remove(wIdxPath)
		Log(LOG_DEBUG, "F: rename %s -> %s \n", wIdxPath, path)
		return e
	}

	Log(LOG_DEBUG, "S: renamed %s -> %s \n", wIdxPath, path)

	// remove merged file
	cleanDirs := make([]string, 0, len(noMergeIdxFiles))
	for _, f := range noMergeIdxFiles {
		if req, e := f.removeWithParent(finder); e != nil || req {
			if e != nil && req {
				Log(LOG_WARN, "F:mergeIndex() cannot remove merged index %s error=%s \n", wIdxPath, e.Error())
			}
			if req {
				cleanDirs = append(cleanDirs, filepath.Dir(f.Path))
			}
		}
	}

	Log(LOG_DEBUG, "S: remove merged files count=%d \n", len(noMergeIdxFiles))
	if Opt.cleanAfterMergeing && len(cleanDirs) > 0 {
		c.cleanDirs(cleanDirs)
	}
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

func (c *Column) key2Path(key uint64, state byte) string {

	strkey := toFnameTri(key)
	if c.IsNum {
		strkey = toFname(key)
	}
	return ColumnPathWithStatus(c.TableDir(), c.Name, c.IsNum, strkey, strkey, state)

}

func (c *Column) CleanDirs() (cnt int) {
	return c.cleanDirs(nil)
}

func (c *Column) CleanDirTest(mode int) {

	cdir := make([]string, 0, 1)

	if mode == 1 {
		//cdir = append(cdir, "/Users/xtakei/git/cmd/tmp/vfs/mac/0029/3002/ff5e")
		cdir = append(cdir, "/Users/xtakei/git/cmd/tmp/vfs/mac/0029")
	}
	if ocdir := c.emptyDirs(cdir); ocdir != nil && len(ocdir) > 0 {
		fmt.Printf("dir=%s\n", ocdir[0])
	}
	return
}

func (c *Column) cleanDirs(idirs []string) (cnt int) {
	dirs := c.emptyDirs(idirs)
	cnt = len(dirs)
	bar := Pbar.Add("clean empty dir", cnt)

	for _, dir := range dirs {
		bar.Increment()
		os.RemoveAll(dir)
	}
	bar.SetTotal(int64(cnt), true)
	if cnt > 0 {
		for i, dir := range dirs {
			dirs[i] = filepath.Dir(dir)
		}
		c.cleanDirs(dirs)
	}
	return

}

type ParentDirs struct {
	m   map[string]bool
	min int
}

func NewParentDirs(base string, dirs []string) (pdirs ParentDirs) {

	allparent := func(odir string) (dirs []string) {

		dir := odir
		for true {
			dirs = append(dirs, dir)
			dir = filepath.Dir(dir)
			if base == filepath.Dir(dir) || len(base) >= len(filepath.Dir(dir)) {
				break
			}
		}
		return
	}
	_ = allparent
	pdirs = ParentDirs{m: map[string]bool{}, min: 100}

	for _, dir := range dirs {
		if len(dir) == 0 {
			continue
		}
		for _, pdir := range allparent(dir) {
			pdirs.m[pdir] = true
		}
	}
	return
}

func (pdir ParentDirs) Has(dir string) bool {
	return pdir.m[dir]
}

func (c *Column) emptyDirs(idirs []string) []string {

	rDirs := []string{}
	finder := OpenIndexFile(c)

	bar := Pbar.Add("find empty dir", 100)

	isFamily := func(path string) bool {
		return loncha.Contain(&idirs, func(i int) bool {
			if strings.Contains(path, idirs[i]) || strings.Contains(idirs[i], path) {
				return true
			}
			return false
		})
	}
	_ = isFamily
	parents := NewParentDirs(finder.Path, idirs)
	_ = parents

	finder.Select(
		OptAsc(true),
		OptCcondFn(func(f *IndexFile) CondType {

			if f.IsType(IdxFileType_Dir) {
				defer func() {
					bar.Increment()
					if bar.Current() >= 80 {
						bar.SetTotal(bar.Current()*2, false)
					}
				}()
				if f.Path == finder.Path {
					return CondLazy
				}

				if len(idirs) > 0 && !parents.Has(f.Path) {
					return CondFalse
				}
				parents.m[f.Path] = true

				if names, _ := readDirNames(f.Path); len(names) == 0 {
					//					fmt.Fprintf(os.Stderr, "found empty %s\n", f.Path)
					return CondTrue
				}
				return CondLazy
			}
			return CondFalse
		}),
		OptTraverse(func(f *IndexFile) error {
			//			fmt.Fprintf(os.Stderr, "found empty %s\n", f.Path)
			rDirs = append(rDirs, f.Path)
			return nil
		}),
	)
	bar.SetTotal(bar.Current(), true)
	return rDirs
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
