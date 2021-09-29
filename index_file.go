package vfsindex

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/kazu/loncha"

	"github.com/kazu/vfs-index/query"
)

type IndexFileType int

const (
	IdxFileType_None IndexFileType = 0
	IdxFileType_Dir  IndexFileType = 1 << iota
	IdxFileType_Merge
	IdxFileType_Write
	IdxFileType_MyColum
	IdxFileType_NoComplete
)

// IndexFile ... file entity of index file in index table directories
type IndexFile struct {
	Path  string
	Ftype IndexFileType
	c     *Column
}

type SelectOpt struct {
	asc         bool
	cond        CondFn
	traverse    TraverseFn
	start       uint64
	last        uint64
	enableRange bool
}

type SelectOption func(*SelectOpt)
type CondType byte
type TraverseFn func(f *IndexFile) error
type CondFn func(f *IndexFile) CondType

type ResultFn func(SkipFn)
type InfoFn func(RecordInfoArg)

type RecordInfoArg struct {
	isKeyRecord bool
	rec         *query.InvertedMapNum
	kr          *query.KeyRecord
	sCur        int
	krSfn       SkipFn
}

const (
	CondTrue CondType = iota
	CondSkip
	CondFalse
	CondLazy
)

func OptCcondFn(c CondFn) SelectOption {
	return func(opt *SelectOpt) {
		opt.cond = c
	}
}

func OptAsc(isAsc bool) SelectOption {
	return func(opt *SelectOpt) {
		opt.asc = isAsc
	}
}
func OptTraverse(fn TraverseFn) SelectOption {

	return func(opt *SelectOpt) {
		opt.traverse = fn
	}
}

func OptRange(start, last uint64) SelectOption {
	return func(opt *SelectOpt) {
		opt.start = start
		opt.last = last
		opt.enableRange = true
	}
}

func (opt *SelectOpt) Merge(opts []SelectOption) {
	for i := range opts {
		opts[i](opt)
	}
}

func LessEqString(s, d string) (isLess bool) {

	defer func() {
		if isLess {
			Log(LOG_DEBUG, "%s < %s == %v\n", s, d, isLess)
		} else {
			//Log(LOG_DEBUG, "%s < %s == %v\n", s, d, isLess)
		}
	}()
	limit := len(s)
	if len(s) > len(d) {
		limit = len(d)
	}
	for k := 0; k < limit; k++ {
		if []rune(s)[k] == []rune(d)[k] {
			continue
		}
		return []rune(s)[k] <= []rune(d)[k]
	}

	return true
}

func sortAlphabet(names []string) []string {
	sort.Slice(names, func(i, j int) bool {
		if len(names[i]) == len(names[j]) {
			for k := 0; k < len(names[i]); k++ {
				if []rune(names[i])[k] == []rune(names[j])[k] {
					continue
				}
				return []rune(names[i])[k] < []rune(names[j])[k]
			}
		}
		return names[i] < names[j]

	})
	return names
}

//   copy this file from file/filepath package
// readDirNames reads the directory named by dirname and returns
// a sorted list of directory entries.
func readDirNames(dirname string) ([]string, error) {
	f, err := os.Open(dirname)
	if err != nil {
		return nil, err
	}
	names, err := f.Readdirnames(-1)
	f.Close()
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

func OpenIndexFile(c *Column) (idxFile *IndexFile) {

	path := filepath.Join(Opt.rootDir, c.TableDir())

	idxFile = NewIndexFile(c, path)
	idxFile.Ftype = IdxFileType_Dir
	c.IsNum = c.validateIndexType()
	return
}

func NewIndexFile(c *Column, path string) *IndexFile {
	return &IndexFile{
		Path: path,
		c:    c,
	}
}

func ListMergedIndex(c *Column, fn CondFn, opts ...SelectOption) (result []*IndexFile) {

	OpenIndexFile(c).Select(
		OptAsc(true),
		OptCcondFn(fn),
		OptTraverse(func(f *IndexFile) error {
			result = append(result, f)
			return nil
		}),
	)
	return
}

func (f IndexFile) Column() *Column {
	return f.c
}

func (f *IndexFile) IsType(t IndexFileType) bool {
	return f.Ftype&t > 0
}
func (f *IndexFile) Init() {

	info, e := os.Stat(f.Path)
	if e == nil && info.IsDir() {
		f.Ftype = IdxFileType_Dir
		return
	}

	vlabel := func() string {
		if f.c.IsNum {
			return "num"
		}
		return "gram"
	}
	a := fmt.Sprintf("%s.%s.idx", f.c.Name, vlabel())
	_ = a

	strs := strings.Split(filepath.Base(f.Path), fmt.Sprintf("%s.%s.idx", f.c.Name, vlabel()))
	if len(strs) < 2 {
		f.Ftype = IdxFileType_None
		return
	}

	if strs := strings.Split(filepath.Base(f.Path), ".merged."); len(strs) > 1 {
		f.Ftype = IdxFileType_Merge
	} else if strs := strings.Split(filepath.Base(f.Path), ".merging."); len(strs) > 1 {
		f.Ftype = IdxFileType_Merge | IdxFileType_NoComplete
	} else if strs := strings.Split(filepath.Base(f.Path), ".adding."); len(strs) > 1 {
		f.Ftype = IdxFileType_Write | IdxFileType_NoComplete
	} else if strs := strings.Split(filepath.Base(f.Path), filepath.Base(f.c.Path())); len(strs) > 1 {
		f.Ftype = IdxFileType_Write
	}
}

func (f *IndexFile) Select(opts ...SelectOption) (err error) {
	opt := SelectOpt{enableRange: false}
	opt.Merge(opts)

	names, err := readDirNames(f.Path)
	names = sortAlphabet(names)
	if !opt.asc {
		sort.SliceStable(names, func(i, j int) bool { return i > j })
	}

	if opt.enableRange {
		k2rel := func(key uint64) string {
			path := f.c.key2Path(key, RECORD_WRITTEN)
			ret, _ := filepath.Rel(filepath.Join(Opt.rootDir, f.c.TableDir()), path)
			return ret
		}

		loncha.Filter(&names, func(i int) bool {
			path, _ := filepath.Rel(filepath.Join(Opt.rootDir, f.c.TableDir()), filepath.Join(f.Path, names[i]))
			return LessEqString(k2rel(opt.start), path) && LessEqString(path, k2rel(opt.last))
		})
	}

	afters := []*IndexFile{}

	for _, name := range names {
		f := NewIndexFile(f.c, filepath.Join(f.Path, name))
		f.Init()
		switch opt.cond(f) {
		case CondSkip:
			continue
		case CondFalse:
			continue
		case CondLazy:
			afters = append(afters, f)
		case CondTrue:
			e := opt.traverse(f)
			if e != nil {
				err = e
				goto FINISH
			}
			// e = f.Select(opts...)
			// if e != nil {
			// 	err = e
			// 	goto FINISH
			// }
		}
	}

	if len(afters) > 0 {
		for _, f := range afters {
			// e := opt.traverse(f)
			// if e != nil {
			// 	err = e
			// 	break
			// }
			e := f.Select(opts...)
			if e != nil {
				err = e
				break
			}

		}
	}

FINISH:
	return
}

// First ... Find first IndexFile.
func (f *IndexFile) First() *IndexFile {

	names, err := readDirNames(f.Path)
	if err != nil {
		return nil
	}
	names = sortAlphabet(names)

	dirs := []*IndexFile{}
	for _, name := range names {
		f := NewIndexFile(f.c, filepath.Join(f.Path, name))
		f.Init()

		if f.IsType(IdxFileType_NoComplete) {
			continue
		}

		if f.IsType(IdxFileType_Merge) {
			return f
		}

		if f.IsType(IdxFileType_Write) {
			return f
		}
		if f.IsType(IdxFileType_Dir) {
			//return f.First()
			dirs = append(dirs, f)
		}
	}
	if len(dirs) > 0 {
		for _, f := range dirs {
			if r := f.First(); r != nil {
				return r
			}
		}
	}
	return nil
}

// First ... Find first IndexFile.
func (f *IndexFile) Last() *IndexFile {
	names, err := readDirNames(f.Path)
	if err != nil {
		return nil
	}
	names = sortAlphabet(names)
	sort.SliceStable(names, func(i, j int) bool { return i > j })

	afters := []*IndexFile{}
	for _, name := range names {
		f := NewIndexFile(f.c, filepath.Join(f.Path, name))
		f.Init()

		if f.IsType(IdxFileType_NoComplete) {
			continue
		}

		if f.IsType(IdxFileType_Merge) {
			//return f
			afters = append(afters, f)
			//return f
			continue
		}

		if f.IsType(IdxFileType_Write) {
			return f
		}
		if f.IsType(IdxFileType_Dir) {
			//afters = append(afters, f)
			if r := f.Last(); r != nil {
				return r
			}
			//dirs = append(dirs, f)
			continue
		}
	}
	if len(afters) > 0 {
		return afters[0]
	}
	return nil

}

func (f *IndexFile) IdxInfo() IndexPathInfo {
	return NewIndexInfo(idxPath2Info(filepath.Base(f.Path)))
}

func (f *IndexFile) KeyRecord() (result *query.InvertedMapNum) {
	if !f.IsType(IdxFileType_Write) {
		return nil
	}
	if file, e := os.Open(f.Path); e == nil {
		buf, e := ioutil.ReadAll(file)
		defer file.Close()
		if e != nil {
			return nil
		}
		ret := query.OpenByBuf(buf).Index().InvertedMapNum()
		return &ret
	}
	return nil
}

func (f *IndexFile) KeyRecords() *query.KeyRecordList {
	if !f.IsType(IdxFileType_Merge) {
		return nil
	}
	if file, e := os.Open(f.Path); e == nil {
		buf, e := ioutil.ReadAll(file)
		defer file.Close()
		if e != nil {
			return nil
		}
		return query.OpenByBuf(buf).Index().IndexNum().Indexes()
	}
	return nil
}

func (f *IndexFile) FirstRecord() *Record {
	var rec *query.Record
	var e error

	if f.IsType(IdxFileType_Write) {
		rec = f.KeyRecord().Value()
	}
	if f.IsType(IdxFileType_Merge) {
		kr, _ := f.KeyRecords().First()
		rec, e = kr.Records().First()
	}
	if e != nil {
		return nil
	}

	return &Record{fileID: rec.FileId().Uint64(),
		offset: rec.Offset().Int64(),
		size:   rec.Size().Int64(),
	}
}

func (f *IndexFile) LastRecord() *Record {
	var rec *query.Record
	var e error

	if f.IsType(IdxFileType_Write) {
		rec = f.KeyRecord().Value()
	}
	if f.IsType(IdxFileType_Merge) {
		kr, _ := f.KeyRecords().Last()
		rec, e = kr.Records().Last()
	}
	if e != nil {
		return nil
	}

	return &Record{fileID: rec.FileId().Uint64(),
		offset: rec.Offset().Int64(),
		size:   rec.Size().Int64(),
	}
}

// RecordByKey ... return function getting splice of query.Record
// Deprecated: RecordByKey
//   should use recordByKey
func (f *IndexFile) RecordByKey2(key uint64) RecordFn {
	return f.recordByKey(key)
}

func (f *IndexFile) recordByKey(key uint64) RecordFn {

	return func(skipFn SkipFn) (records []*query.Record) {
		f.recordInfoByKey(key, func(arg RecordInfoArg) {
			if !arg.isKeyRecord {
				records = append(records, arg.rec.Value())
				return
			}
			if arg.kr == nil {
				return
			}
			Log(LOG_DEBUG, "records %+v\n", arg.kr.Records())
			Log(LOG_DEBUG, "record count %d\n", arg.kr.Records().Count())
			for i := 0; i < arg.kr.Records().Count(); i++ {
				sResult := arg.krSfn(arg.sCur + i)
				Log(LOG_DEBUG, "skip result %+v\n", sResult)
				if sResult == SkipTrue {
					continue
				}
				if sResult == SkipFinish {
					//skipCur += i
					return
				}
				r, _ := arg.kr.Records().At(i)
				recDump := func(r *query.Record) string {
					return fmt.Sprintf("FileId=%+v Offset=%+v", r.FileId().Uint64(), r.Offset().Int64())
				}
				Log(LOG_DEBUG, "found add %+v\n", recDump(r))
				records = append(records, r)
			}
			return
		})(skipFn)
		return
	}
}

func (f *IndexFile) countBy(key uint64) (cnt int) {
	cnt = 0
	f.recordInfoByKey(key, func(arg RecordInfoArg) {
		if !arg.isKeyRecord {
			cnt++
			return
		}

		for i := 0; i < arg.kr.Records().Count(); i++ {
			if arg.krSfn(arg.sCur+i) == SkipTrue {
				continue
			}
			if arg.krSfn(arg.sCur+i) == SkipFinish {
				//skipCur += i
				return
			}
			cnt++
		}
	})(EmptySkip)
	return cnt
}

func (f *IndexFile) recordInfoByKey(key uint64, fn InfoFn) ResultFn {

	return func(skipFn SkipFn) {
		elapsed := MesureElapsed()
		defer func() {
			if LogIsDebug() {
				Log(LOG_DEBUG, "RecordByKey(%s) %s\n", DecodeTri(key), elapsed("%s"))
			}
		}()

		idxs := f.FindByKey(key)
		skipCur := 0
		for _, idx := range idxs {
			if idx == nil {
				continue
			}
			Log(LOG_DEBUG, "recordInfoByKey() idx.path=%s\n", idx.Path)

			if skipFn(skipCur) == SkipFinish {
				return
			}
			if idx.IsType(IdxFileType_Write) {
				if skipFn(skipCur) == SkipTrue {
					skipCur++
					continue
				}
				fn(RecordInfoArg{false, idx.KeyRecord(), nil, skipCur, skipFn})
				skipCur++
			} else if idx.IsType(IdxFileType_Merge) {
				kr := idx.KeyRecords().Find(func(kr *query.KeyRecord) bool {
					Log(LOG_DEBUG, "KeyRecords().Find() kt.key=%+v(%s) key=%+v(%s)\n",
						kr.Key().Uint64(), DecodeTri(kr.Key().Uint64()),
						key, DecodeTri(key))
					Log(LOG_DEBUG, "  count=%d\n", kr.Records().Count())
					return kr.Key().Uint64() == key
				})
				if kr.CommonNode == nil {
					skipCur++
					continue
				}
				fmt.Printf("%+v %+v %+v\n", kr, skipCur, skipFn)
				fn(RecordInfoArg{true, nil, kr, skipCur, skipFn})
				skipCur += kr.Records().Count()
			}
		}
		return
	}
}

func (f *IndexFile) RecordNearByKey(key uint64, less bool) RecordFn {

	return func(skipFn SkipFn) (records []*query.Record) {
		idxs := f.FindNearByKey(key, less)

		defer func() {
			Log(LOG_DEBUG, "recs=%v\n", records)
		}()
		skipCur := 0
		//idx := idxs[0]
		var sidx, lidx uint64

		loncha.Delete(&idxs, func(i int) bool {
			return idxs[i].IsType(IdxFileType_Merge)
		})

		midxs := ListMergedIndex(f.c, func(f *IndexFile) CondType {
			if f.IsType(IdxFileType_NoComplete) {
				return CondSkip
			}
			if f.IsType(IdxFileType_Merge) {
				//return CondTrue
				if less && f.IdxInfo().first < key {
					return CondTrue
				}
				if !less && f.IdxInfo().last > key {
					return CondTrue
				}
			}
			return CondSkip
		})
		for i := range midxs {
			idx := midxs[i]
			krs := idx.KeyRecords().Select(func(kr *query.KeyRecord) bool {
				if less && (kr.Key().Uint64() <= key) {
					return true
				}
				if !less && (kr.Key().Uint64() >= key) {
					return true
				}
				return false
			})
			for _, kr := range krs {
				//defer func() { skipCur += kr.Records().Count() }()
				for j := 0; j < kr.Records().Count(); j++ {
					if skipFn(skipCur+j) == SkipTrue {
						continue
					}
					if skipFn(skipCur+j) == SkipFinish {
						break
					}

					r, _ := kr.Records().At(j)
					records = append(records, r)
				}
				skipCur += kr.Records().Count()
			}
		}

		if less {
			sidx = OpenIndexFile(f.c).First().IdxInfo().first
			lidx = key

			if len(midxs) > 0 {
				sidx = midxs[len(midxs)-1].IdxInfo().last + 1
			}

		} else {
			sidx = key
			lidx = OpenIndexFile(f.c).Last().IdxInfo().last
			if len(midxs) > 0 {
				sidx = midxs[len(midxs)-1].IdxInfo().last + 1
			}
		}
		//_ , _ = sidx, lidx
		OpenIndexFile(f.c).Select(
			OptAsc(less),
			OptRange(sidx, lidx),
			OptCcondFn(func(f *IndexFile) CondType {
				//FIXME: merge index support
				if f.IsType(IdxFileType_NoComplete) || f.IsType(IdxFileType_Merge) {
					return CondSkip
				} else if f.IsType(IdxFileType_Dir) {
					return CondLazy
				}
				return CondTrue
			}),
			OptTraverse(func(f *IndexFile) error {
				// 	return ErrStopTraverse
				//defer func() { skipCur++ }()
				if f.IsType(IdxFileType_Write) {
					defer func() { skipCur++ }()
					if skipFn(skipCur) == SkipTrue {
						return nil
					}
					if skipFn(skipCur) == SkipFinish {
						return errors.New("traverse finish")
					}

					records = append(records, f.KeyRecord().Value())
				} else if f.IsType(IdxFileType_Merge) {
					kr := f.KeyRecords().Find(func(kr *query.KeyRecord) bool {
						return kr.Key().Uint64() == key
					})
					defer func() { skipCur += kr.Records().Count() }()
					for i := 0; i < kr.Records().Count(); i++ {
						if skipFn(skipCur+i) == SkipTrue {
							continue
						}
						if skipFn(skipCur+i) == SkipFinish {
							return errors.New("traverse finish")
						}

						r, _ := kr.Records().At(i)
						records = append(records, r)
					}
				}
				return nil
			}),
		)

		return
	}
}

func (f *IndexFile) FindByKey(key uint64) (result []*IndexFile) {

	c := f.c
	if filepath.Join(Opt.rootDir, c.TableDir()) != f.Path {
		return nil //ErrNotIndexDir
	}

	strkey := toFnameTri(key)
	if c.IsNum {
		strkey = toFname(key)
	}

	pat := ColumnPathWithStatus(c.TableDir(), c.Name, c.IsNum, strkey, strkey, RECORD_WRITTEN)

	paths, _ := filepath.Glob(fmt.Sprintf("%s.*.*", pat))
	if len(paths) > 0 {
		for _, path := range paths {
			matchfile := NewIndexFile(c, path)
			matchfile.Init()
			result = append(result, matchfile)
		}
		return
	}

	return []*IndexFile{f.findAllFromMergeIdx(key)}

}

func (f *IndexFile) FindNearByKey(key uint64, less bool) (results []*IndexFile) {

	results = f.FindByKey(key)
	if len(results) > 0 && results[0] != nil {
		return results
	}
	c := f.c
	// if filepath.Join(Opt.rootDir, c.TableDir()) != f.Path {
	// 	return nil //ErrNotIndexDir
	// }
	strkey := toFnameTri(key)
	if c.IsNum {
		strkey = toFname(key)
	}
	var result *IndexFile
	//found := false
	pat := ColumnPathWithStatus(c.TableDir(), c.Name, c.IsNum, strkey, strkey, RECORD_WRITTEN)
	for {
		dir := NewIndexFile(f.c, filepath.Dir(pat))
		dir.Init()

		e := dir.Select(
			OptAsc(less),
			OptCcondFn(func(f *IndexFile) CondType {
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
				if less {
					if f.IdxInfo().first < key {
						result = f
					}
					if f.IdxInfo().first > key {
						return ErrStopTraverse
					}
				} else {
					if f.IdxInfo().first > key {
						result = f
					}

					if f.IdxInfo().first < key {
						return ErrStopTraverse
					}
				}

				// if !less && f.IdxInfo().first > key {
				// 	result = f
				// 	return ErrStopTraverse
				// }
				// if less && f.IdxInfo().last < key {
				// 	result = f
				// 	return ErrStopTraverse
				// }
				// result = f
				// return nil
				return nil
			}),
		)
		if e != ErrStopTraverse {
			result = nil
		}
		if result != nil {
			break
		}
		if dir.Path == filepath.Join(Opt.rootDir, c.TableDir()) {
			break
		}
		pat = filepath.Dir(pat)
	}
	if result == nil {
		return nil
	}
	return []*IndexFile{result}
}

func (f *IndexFile) findAllFromMergeIdx(key uint64) *IndexFile {

	c := f.c

	if filepath.Join(Opt.rootDir, c.TableDir()) != f.Path {
		return nil //ErrNotIndexDir
	}

	names, err := readDirNames(f.Path)
	if err != nil {
		return nil
	}
	sortAlphabet(names)

	type KeyFile struct {
		key  uint64
		file *IndexFile
	}

	// low := KeyFile{}
	// high := KeyFile{}

	for _, name := range names {
		f := NewIndexFile(f.c, filepath.Join(f.Path, name))
		f.Init()

		if f.IsType(IdxFileType_NoComplete) {
			continue
		}
		if f.IsType(IdxFileType_Merge) {
			// if f.IdxInfo().first < key && low.key < f.IdxInfo().first {
			// 	low.key = f.IdxInfo().first
			// 	low.file = f
			// }

			// if f.IdxInfo().last > key && high.key > f.IdxInfo().last {
			// 	high.key = f.IdxInfo().last
			// 	high.file = f
			// }

			// if low.file == f && high.file == f {
			// 	return f
			// }

			if key >= f.IdxInfo().first && key <= f.IdxInfo().last {
				return f
			}
		}
	}

	return nil

}

func (f *IndexFile) parentWith(t *IndexFile) *IndexFile {

	size := len(f.Path)
	if size > len(t.Path) {
		size = len(t.Path)
	}

	for i := 0; i < size; i++ {
		if f.Path[i] != t.Path[i] {
			return NewIndexFile(f.c, filepath.Dir(f.Path[:i]))
		}
	}

	return NewIndexFile(f.c, f.Path[:size])
}

func (f *IndexFile) childs(t IndexFileType) (cDirs []*IndexFile) {

	names, err := readDirNames(f.Path)

	if err != nil {
		return nil
	}
	names = sortAlphabet(names)

	cDirs = make([]*IndexFile, 0, len(names))

	for i := range names {
		c := NewIndexFile(f.c, filepath.Join(f.Path, names[i]))
		c.Init()
		cDirs = append(cDirs, c)
	}

	loncha.Delete(&cDirs, func(i int) bool {
		return !cDirs[i].IsType(t) || cDirs[i].IsType(IdxFileType_NoComplete)
	})
	return cDirs
}

func (l *IndexFile) middle(h *IndexFile) *IndexFile {

	p := l.parentWith(h)
	if p == nil {
		return p
	}

	p.Init()

	// names, err := readDirNames(p.Path)
	// if err != nil {
	// 	return nil
	// }
	cDirs := p.childs(IdxFileType_Dir)

	if len(cDirs) > 0 {
		lidx, _ := loncha.IndexOf(cDirs, func(i int) bool {
			p := l.parentWith(cDirs[i])
			if p == nil {
				return false
			}
			return p.Path == cDirs[i].Path
		})
		_ = lidx
		hidx, _ := loncha.IndexOf(cDirs, func(i int) bool {
			p := h.parentWith(cDirs[i])
			if p == nil {
				return false
			}
			return p.Path == cDirs[i].Path
		})
		_ = hidx

		midx := (lidx + hidx) / 2
		sects := []int{}
		for i := midx; i <= hidx; i++ {
			if midx-(i-midx) >= 0 && midx != i {
				sects = append(sects, midx-(i-midx))
			}
			sects = append(sects, i)
		}

		for _, idx := range sects {
			mf := cDirs[idx]
			r := mf.middleAsDir()
			if r != nil {
				return r
			}
		}
		return nil
	}
	return l.middleFile(p, h)

}

func (d *IndexFile) middleAsDir() *IndexFile {

	cDirs := d.childs(IdxFileType_Dir)
	if len(cDirs) == 1 {
		fmt.Printf("1 cDirs=%+v\n", cDirs[0])
		return cDirs[0].middleAsDir()
	}
	if len(cDirs) > 1 {
		fmt.Printf("> 1 cDirs=%+v\n", cDirs[0])
		return cDirs[0].middle(cDirs[len(cDirs)-1])
	}

	cFiles := d.childs(IdxFileType_Write)
	if len(cFiles) == 0 {
		return nil
	} else if len(cFiles) == 1 {
		return cFiles[0]
	}
	idx := 0
	if (len(cFiles)*10)/2 == len(cFiles)/2 {
		idx = len(cFiles) / 2
	} else {
		idx = len(cFiles)/2 + 1
	}
	return cFiles[idx]

}

func (l *IndexFile) middleFile(d, h *IndexFile) *IndexFile {

	files := d.childs(IdxFileType_Write)
	if len(files) == 0 {
		//os.Remove(d.Path)
		return nil
	}
	lidx, _ := loncha.IndexOf(files, func(i int) bool {
		return files[i].Path == l.Path
	})
	_ = lidx
	hidx, _ := loncha.IndexOf(files, func(i int) bool {
		return files[i].Path == h.Path
	})
	_ = hidx
	idx := (lidx + hidx) / 2
	return files[idx]
}

func (l *IndexFile) removeWithParent(finder *IndexFile) (require_clean bool, e error) {
	require_clean = false

	if e = os.Remove(l.Path); e != nil {
		require_clean = true
		return
	}
	path := l.Path
	for {
		path = filepath.Dir(path)
		if path == finder.Path {
			return
		}
		if e = os.Remove(path); e != nil {
			require_clean = true
			//e = fmt.Errorf("cannot remove file %s error=%s", path, e.Error())
			e = nil
			return
		}

	}
}
