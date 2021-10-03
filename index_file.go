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

// SelectOpt ... sarch options for select.
type SelectOpt struct {
	asc           bool
	cond          CondFn
	traverse      TraverseFn
	start         uint64
	last          uint64
	enableRange   bool
	enableBSearch bool
}

// SelectOption ... for setting option parameter in Select()
type SelectOption func(*SelectOpt)

// CondType ... Condition Type to traversse in Select()
type CondType byte

// TraverseFn ... function to traverse in Select()
type TraverseFn func(f *IndexFile) error

// CondFn ... function to check condition in Select()
type CondFn func(f *IndexFile) CondType

// ResultFn ... function to find record with SkipFn
type ResultFn func(SkipFn)

// InfoFn ... function to find record infomation
type InfoFn func(RecordInfoArg)

// RecordFn .. function to retrun record slices
type RecordFn func(SkipFn) []*query.Record

// CountFn .. function to retrun count of record
type CountFn func(SkipFn) int

// SkipFn .. function to filter record result
type SkipFn func(int) SkipType

// SearchFn ... aggreate RecordFn and CountFn
type SearchFn struct {
	forRecord bool
	RecFn     RecordFn
	CntFn     CountFn
}

// RecordInfoArg ... params for InfoFn
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

// DefaultSelectOpt ... return default of SelectOption
func DefaultSelectOpt() SelectOpt {

	return SelectOpt{
		enableRange:   false,
		enableBSearch: false,
	}
}

// OptCcondFn ... set option of CondFn in Select()
func OptCcondFn(c CondFn) SelectOption {
	return func(opt *SelectOpt) {
		opt.cond = c
	}
}

// OptAsc ... set option of order to search in Select()
func OptAsc(isAsc bool) SelectOption {
	return func(opt *SelectOpt) {
		opt.asc = isAsc
	}
}

// OptTraverse ... set option of TraverseFnin Select()
func OptTraverse(fn TraverseFn) SelectOption {

	return func(opt *SelectOpt) {
		opt.traverse = fn
	}
}

// OptRange ... set option of range Select()
func OptRange(start, last uint64) SelectOption {
	return func(opt *SelectOpt) {
		opt.start = start
		opt.last = last
		opt.enableRange = true
	}
}

func (opt *SelectOpt) merge(opts []SelectOption) {
	for i := range opts {
		opts[i](opt)
	}
}

// LessEqString ... compare strings . if equal or less , return true
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

func (fn SearchFn) Do(skipFn SkipFn) interface{} {
	if fn.forRecord {
		return fn.RecFn(skipFn)
	}

	return fn.CntFn(skipFn)
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

func (f *IndexFile) beforeSelect(opt *SelectOpt) (names []string, k2rel func(uint64) string, err error) {
	//opt := DefaultSelectOpt()
	// opt.merge(opts)

	names, err = readDirNames(f.Path)
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
	return
}

func (f *IndexFile) idxWithBsearch(names []string, opt *SelectOpt) (idx int) {

	idx = sort.Search(len(names), func(i int) bool {
		name := names[i]
		f := NewIndexFile(f.c, filepath.Join(f.Path, name))
		f.Init()
		switch opt.cond(f) {
		case CondSkip, CondFalse:
			return false
		case CondLazy:
			cnames, _, _ := f.beforeSelect(opt)
			if f.idxWithBsearch(cnames, opt) > -1 {
				return true
			}
			return false
		case CondTrue:
			return true
		}
		return false
	})
	if idx >= len(names) {
		return -1
	}
	return
}

func (f *IndexFile) Select(opts ...SelectOption) (err error) {
	opt := DefaultSelectOpt()
	opt.merge(opts)

	names, k2rel, err := f.beforeSelect(&opt)
	_, _, _ = names, k2rel, err

	idx := 0
	if opt.enableBSearch && opt.asc {
		idx = f.idxWithBsearch(names, &opt)
	}

	afters := []*IndexFile{}

	for i := idx; i < len(names); i++ {
		name := names[i]
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

// RecordByKey2 ... return function getting slice of query.Record
// Deprecated: RecordByKey
//   should use recordByKey
func (f *IndexFile) RecordByKey2(key uint64) RecordFn {
	return f.recordByKeyFn(key)
}
func (f *IndexFile) commonFnByKey(key uint64) (result SearchFn) {

	type AddFn func(interface{})

	baseFn := func(skipFn SkipFn, addFn AddFn) {
		f.recordInfoByKeyFn(key, func(arg RecordInfoArg) {
			if !arg.isKeyRecord {
				addFn(arg.rec.Value())
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
				addFn(r)
			}
			return
		})(skipFn)
	}
	result.RecFn = func(skipFn SkipFn) (records []*query.Record) {
		baseFn(skipFn, func(r interface{}) {
			records = append(records, r.(*query.Record))
		})
		return
	}

	result.CntFn = func(skipFn SkipFn) (cnt int) {
		baseFn(skipFn, func(r interface{}) {
			cnt++
		})
		return
	}

	return

}

func (f *IndexFile) recordByKeyFn(key uint64) RecordFn {
	return f.commonFnByKey(key).RecFn
}

func (f *IndexFile) countBy(key uint64) (cnt int) {
	return f.countFnBy(key)(EmptySkip)
}

func (f *IndexFile) countFnBy(key uint64) CountFn {
	return f.commonFnByKey(key).CntFn
}

func (f *IndexFile) recordInfoByKeyFn(key uint64, fn InfoFn) ResultFn {

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
				var kr *query.KeyRecord
				kr = nil
				if Opt.useBsearch {
					kr = BsearchInKeyRecord(key, idx.KeyRecords().Search(func(q *query.KeyRecord) bool {
						return q.Key().Uint64() >= key
					}))
				} else {
					kr = idx.KeyRecords().Find(func(kr *query.KeyRecord) bool {
						return kr.Key().Uint64() == key
					})
				}

				if kr == nil || kr.CommonNode == nil {
					skipCur++
					continue
				}
				fn(RecordInfoArg{true, nil, kr, skipCur, skipFn})
				skipCur += kr.Records().Count()
			}
		}
		return
	}
}

// RecordNearByKeyFn ... return function near matching of query.Record
func (f *IndexFile) RecordNearByKeyFn(key uint64, less bool) RecordFn {
	return f.commonNearFnByKey(key, less).RecFn
}

// CountNearByKeyFn ... return function count to matching near
func (f *IndexFile) CountNearByKeyFn(key uint64, less bool) CountFn {
	return f.commonNearFnByKey(key, less).CntFn
}

func (f *IndexFile) commonNearFnByKey(key uint64, less bool) (result SearchFn) {

	type AddFn func(interface{})

	baseFn := func(skipFn SkipFn, addFn AddFn) {
		idxs := f.FindNearByKey(key, less)
		skipCur := 0
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
					addFn(r)
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

					addFn(f.KeyRecord().Value())
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
						addFn(r)
					}
				}
				return nil
			}),
		)
	}
	result.RecFn = func(skipFn SkipFn) (records []*query.Record) {
		defer func() {
			Log(LOG_DEBUG, "RecordNearByKeyFn(): recs=%v\n", records)
		}()

		baseFn(skipFn, func(r interface{}) {
			records = append(records, r.(*query.Record))
		})
		return
	}
	result.CntFn = func(skipFn SkipFn) (cnt int) {
		defer func() {
			Log(LOG_DEBUG, "CountNearByKeyFn(): cnt=%d\n", cnt)
		}()
		baseFn(skipFn, func(r interface{}) {
			cnt++
		})
		return cnt
	}

	return
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

	return f.findAllFromMergeIdxs(key)
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
	idxs := f.findAllFromMergeIdxs(key)
	if len(idxs) == 0 {
		return nil
	}
	return idxs[0]

}
func (f *IndexFile) findAllFromMergeIdxs(key uint64) (result []*IndexFile) {

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
	var idx int

	if !Opt.useBsearch {
		goto NOT_USE_BSEARCH
	}

	idx = sort.Search(len(names), func(i int) bool {
		f := NewIndexFile(f.c, filepath.Join(f.Path, names[i]))
		f.Init()
		return f.IsType(IdxFileType_Merge) && key >= f.IdxInfo().first && key <= f.IdxInfo().last
	})
	result = make([]*IndexFile, 0, len(names)-idx)

	for i := idx; i < len(names); i++ {
		f := NewIndexFile(f.c, filepath.Join(f.Path, names[i]))
		f.Init()
		if f.IsType(IdxFileType_NoComplete) {
			continue
		}
		if f.IsType(IdxFileType_Merge) {
			if key >= f.IdxInfo().first && key <= f.IdxInfo().last {
				result = append(result, f)
				continue
			}
			if key > f.IdxInfo().last {
				break
			}
		}
	}

	return

NOT_USE_BSEARCH:

	// TODO: must binary-search. multiple result
	for _, name := range names {
		f := NewIndexFile(f.c, filepath.Join(f.Path, name))
		f.Init()

		if f.IsType(IdxFileType_NoComplete) {
			continue
		}
		if f.IsType(IdxFileType_Merge) {
			if key >= f.IdxInfo().first && key <= f.IdxInfo().last {
				return []*IndexFile{f}
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
