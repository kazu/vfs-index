package vfsindex

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/kazu/fbshelper/query/base"
	"github.com/kazu/loncha"
	"github.com/vbauerster/mpb/v5"

	"github.com/kazu/vfs-index/query"
)

// IndexFileType ... type of Index File
type IndexFileType int

const (
	// IdxFileTypeNoInit ... default on init
	IdxFileTypeNoInit IndexFileType = 0
	// IdxFileTypeNone ... default on init
	IdxFileTypeNone IndexFileType = 1 << iota
	// IdxFileTypeDir ... directory
	IdxFileTypeDir
	// IdxFileTypeMerge ... merged(multiple) index file
	IdxFileTypeMerge
	// IdxFileTypeWrite ... single index file
	IdxFileTypeWrite
	// IdxFileTypeMyColum undocumented
	IdxFileTypeMyColum
	// IdxFileTypeNoComplete ... index file is not completed
	IdxFileTypeNoComplete
)

// IndexFile ... file entity of index file in index table directories
type IndexFile struct {
	Path  string
	Ftype IndexFileType
	c     *Column
	cache indexFileCache
}

type indexFileCache struct {
	keyRecords *query.KeyRecordList
	keyRecord  *query.InvertedMapNum
	closer     io.Closer
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
	onlyType      SelectOptType
	recordFileID  uint64
	recordOffset  int64
}

// SelectOptType ... spefify IndexFile type for Select()
type SelectOptType struct {
	use bool
	t   IndexFileType
}

// ConfigResultFn ... configuration for ResultFn
type ConfigResultFn struct {
	useFileFIlter bool
	fileID        uint64
	offset        int64
}

// DefaultCofigResultFn ... default configuration of CofigResultFn
var DefaultCofigResultFn ConfigResultFn = ConfigResultFn{useFileFIlter: false}

// SelectOption ... for setting option parameter in Select()
type SelectOption func(*SelectOpt)

// CondType ... Condition Type to traversse in Select()
type CondType byte

// TraverseFn ... function to traverse in Select()
type TraverseFn func(f *IndexFile) error

// CondFn ... function to check condition in Select()
type CondFn func(f *IndexFile) CondType

// OptResultFn ... option of ResultFn
type OptResultFn func(*ConfigResultFn)

// ResultFn ... function to find record with SkipFn
type ResultFn func(SkipFn, ...OptResultFn)

// InfoFn ... function to find record infomation
type InfoFn func(RecordInfoArg)

// RecordFn .. function to retrun record slices
type RecordFn func(SkipFn) []*query.Record

// CountFn .. function to retrun count of record
type CountFn func(SkipFn) int

// SkipFn .. function to filter record result
type SkipFn func(int) SkipType

// RecordChFn ... function to retrun record slices via chan
type RecordChFn func(in <-chan *query.Record, out chan<- *query.Record)

// SearchFn ... aggreate RecordFn and CountFn
type SearchFn struct {
	forRecord bool
	RecFn     RecordFn
	CntFn     CountFn
	RecChFn   RecordChFn
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
	// CondTrue undocumented
	CondTrue CondType = iota
	// CondSkip undocumented
	CondSkip
	// CondFalse undocumented
	CondFalse
	// CondLazy undocumented
	CondLazy
)

// DefaultSelectOpt ... return default of SelectOption
func DefaultSelectOpt() SelectOpt {

	return SelectOpt{
		enableRange:   false,
		enableBSearch: false,
		onlyType:      SelectOptType{use: false},
		recordFileID:  0,
		recordOffset:  0,
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

// OptOnly ... set option of index file type of Select()
func OptOnly(t IndexFileType) SelectOption {
	return func(opt *SelectOpt) {
		opt.onlyType.use = true
		opt.onlyType.t = t

	}
}

// OptRecord ... filter by Record infomation of index file type of Select()
func OptRecord(fileID uint64, offset int64) SelectOption {
	return func(opt *SelectOpt) {
		opt.recordFileID = fileID
		opt.recordOffset = offset
	}
}

func (opt *SelectOpt) merge(opts []SelectOption) {
	for i := range opts {
		opts[i](opt)
	}
}

// OptFilterWithFile ... filter by Record infomation of ResultFn
func OptFilterWithFile(fileID uint64, offset int64) OptResultFn {

	return func(c *ConfigResultFn) {
		c.useFileFIlter = true
		c.fileID = fileID
		c.offset = offset
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
func dirnamesByType(dirname string, t IndexFileType) (names []string, e error) {

	return dirnamesByTypeAndRecord(dirname, t, 0, 0)
}

func dirnamesByTypeAndRecord(dirname string, t IndexFileType, fileID uint64, offset int64) (names []string, e error) {

	pfix := "*.*"
	if fileID > 0 {
		pfix = fmt.Sprintf("*.%010x.%010x", fileID, offset)
	}

	switch t {
	case IdxFileTypeWrite:
		names, e = filepath.Glob(filepath.Join(dirname, "*/*/*/*.*.idx.*-"+pfix))
		goto REMOVE_REL
	case IdxFileTypeMerge:
		names, e = filepath.Glob(filepath.Join(dirname, "*.merged.*"))
		goto REMOVE_REL
	default:
		names, e = readDirNames(dirname)
		goto RESULT
	}

REMOVE_REL:
	for i, _ := range names {
		names[i], _ = filepath.Rel(dirname, names[i])
	}

RESULT:
	return
}

// OpenIndexFile undocumented
func OpenIndexFile(c *Column) (idxFile *IndexFile) {

	path := filepath.Join(Opt.rootDir, c.TableDir())

	idxFile = NewIndexFile(c, path)
	idxFile.Ftype = IdxFileTypeDir
	c.IsNum = c.validateIndexType()
	return
}

// NewIndexFile ... make new IndexFile
func NewIndexFile(c *Column, path string) (file *IndexFile) {
	file = &IndexFile{
		Path: path,
		c:    c,
	}
	file.init(false)
	return file
}

// ListMergedIndex ... return all merged index
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

// Do undocumented
func (fn SearchFn) Do(skipFn SkipFn) interface{} {
	if fn.forRecord {
		return fn.RecFn(skipFn)
	}

	return fn.CntFn(skipFn)
}

// Column undocumented
func (f IndexFile) Column() *Column {
	return f.c
}

// IsType ... check by IndexFileType
func (f *IndexFile) IsType(t IndexFileType) bool {
	return f.Ftype&t > 0
}

// Init undocumented
func (f *IndexFile) Init() {
	f.init(true)
}

func (f *IndexFile) init(force bool) {

	if !force && f.Ftype != IdxFileTypeNoInit {
		return
	}

	info, e := os.Stat(f.Path)
	if e == nil && info.IsDir() {
		f.Ftype = IdxFileTypeDir
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
		f.Ftype = IdxFileTypeNone
		return
	}

	if strs := strings.Split(filepath.Base(f.Path), ".merged."); len(strs) > 1 {
		f.Ftype = IdxFileTypeMerge
	} else if strs := strings.Split(filepath.Base(f.Path), ".merging."); len(strs) > 1 {
		f.Ftype = IdxFileTypeMerge | IdxFileTypeNoComplete
	} else if strs := strings.Split(filepath.Base(f.Path), ".adding."); len(strs) > 1 {
		f.Ftype = IdxFileTypeWrite | IdxFileTypeNoComplete
	} else if strs := strings.Split(filepath.Base(f.Path), filepath.Base(f.c.Path())); len(strs) > 1 {
		f.Ftype = IdxFileTypeWrite
	}
}

func (f *IndexFile) beforeSelect(opt *SelectOpt) (names []string, k2rel func(uint64) string, err error) {
	//opt := DefaultSelectOpt()
	// opt.merge(opts)

	if opt.onlyType.use {
		names, err = dirnamesByType(f.Path, opt.onlyType.t)
	} else {
		names, err = readDirNames(f.Path)
	}

	names = sortAlphabet(names)
	if !opt.asc {
		sort.SliceStable(names, func(i, j int) bool { return i > j })
	}

	if opt.enableRange {
		k2rel = func(key uint64) string {
			path := f.c.key2Path(key, RECORD_WRITTEN)
			ret, _ := filepath.Rel(filepath.Join(Opt.rootDir, f.c.TableDir()), path)
			return ret
		}
		relstart := k2rel(opt.start)
		rellast := k2rel(opt.last)

		sidx := sort.Search(len(names), func(i int) bool {
			path, _ := filepath.Rel(filepath.Join(Opt.rootDir, f.c.TableDir()), filepath.Join(f.Path, names[i]))
			return LessEqString(relstart, path)
		})
		_ = sidx
		lidx := sort.Search(len(names), func(i int) bool {
			path, _ := filepath.Rel(filepath.Join(Opt.rootDir, f.c.TableDir()), filepath.Join(f.Path, names[i]))
			return LessEqString(rellast, path)
		})
		if sidx < 0 || sidx == len(names) {
			names = []string{}
			return
		}
		if lidx < 0 || lidx == len(names) {
			lidx = len(names) - 1
		}
		names = names[sidx:lidx]

	}
	return
}

func (f *IndexFile) idxWithBsearch(names []string, opt *SelectOpt) (idx int) {

	idx = sort.Search(len(names), func(i int) bool {
		name := names[i]
		f := NewIndexFile(f.c, filepath.Join(f.Path, name))
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

// Select .. search indexfile by SelectOption
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

		if f.IsType(IdxFileTypeNoComplete) {
			continue
		}

		if f.IsType(IdxFileTypeMerge) {
			return f
		}

		if f.IsType(IdxFileTypeWrite) {
			return f
		}
		if f.IsType(IdxFileTypeDir) {
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

		if f.IsType(IdxFileTypeNoComplete) {
			continue
		}

		if f.IsType(IdxFileTypeMerge) {
			//return f
			afters = append(afters, f)
			//return f
			continue
		}

		if f.IsType(IdxFileTypeWrite) {
			return f
		}
		if f.IsType(IdxFileTypeDir) {
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

// ResetCache ... reset cache and close io for caching
func (f *IndexFile) ResetCache() {

	if f.cache.closer != nil {
		f.cache.closer.Close()
	}
	f.cache.keyRecords = nil
	f.cache.keyRecord = nil

}

func (f *IndexFile) KeyRecord() *query.InvertedMapNum {

	if f.cache.keyRecord != nil {
		return f.cache.keyRecord
	}

	return f.neoKeyRecord()
}

// KeyRecord ... return KeyRecord from flatbuffers
func (f *IndexFile) oldKeyRecord() (result *query.InvertedMapNum) {

	if !f.IsType(IdxFileTypeWrite) {
		return nil
	}
	if file, e := os.Open(f.Path); e == nil {
		buf, e := ioutil.ReadAll(file)
		defer file.Close()
		if e != nil {
			return nil
		}
		ret := query.OpenByBuf(buf).Index().InvertedMapNum()
		f.cache.keyRecord = &ret
		return f.cache.keyRecord
	}
	return nil
}

func (f *IndexFile) neoKeyRecord() (result *query.InvertedMapNum) {

	if !f.IsType(IdxFileTypeWrite) {
		return nil
	}
	if file, e := os.Open(f.Path); e == nil {
		if e != nil {
			return nil
		}
		ret := query.Open(file, 512).Index().InvertedMapNum()
		f.cache.keyRecord = &ret
		f.cache.closer = file
		return f.cache.keyRecord
	}
	return nil
}

// KeyRecords ... return KeyRecordList from flatbuffers
func (f *IndexFile) KeyRecords() *query.KeyRecordList {
	if f.cache.keyRecords != nil {
		return f.cache.keyRecords
	}

	return f.neoKeyRecords()
}

func (f *IndexFile) OldKeyRecords() *query.KeyRecordList {

	if !f.IsType(IdxFileTypeMerge) {
		return nil
	}
	if file, e := os.Open(f.Path); e == nil {
		buf, e := ioutil.ReadAll(file)
		defer file.Close()
		if e != nil {
			return nil
		}
		f.cache.keyRecords = query.OpenByBuf(buf).Index().IndexNum().Indexes()
		return f.cache.keyRecords
	}
	return nil
}

func (f *IndexFile) neoKeyRecords() *query.KeyRecordList {

	if !f.IsType(IdxFileTypeMerge) {
		return nil
	}
	if file, e := os.Open(f.Path); e == nil {
		if e != nil {
			return nil
		}
		f.cache.keyRecords = query.Open(file, 4096).Index().IndexNum().Indexes()
		f.cache.closer = file
		// if f.cache.keyRecords.NodeList.ValueInfo.Size <= 0 {
		// 	f.cache.keyRecords.NodeList.ValueInfo = base.ValueInfo(f.cache.keyRecords.List().InfoSlice())
		// }
		// if f.cache.keyRecords.LenBuf() < f.cache.keyRecords.NodeList.Node.Pos+f.cache.keyRecords.NodeList.ValueInfo.Size {
		// 	f.cache.keyRecords.R(f.cache.keyRecords.NodeList.Node.Pos + f.cache.keyRecords.NodeList.ValueInfo.Size - 1)
		// }

		return f.cache.keyRecords
	}
	return nil
}

// FirstRecord undocumented
func (f *IndexFile) FirstRecord() *Record {
	var rec *query.Record
	var e error

	if f.IsType(IdxFileTypeWrite) {
		rec = f.KeyRecord().Value()
	}
	if f.IsType(IdxFileTypeMerge) {
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

// LastRecord undocumented
func (f *IndexFile) LastRecord() *Record {
	var rec *query.Record
	var e error

	if f.IsType(IdxFileTypeWrite) {
		rec = f.KeyRecord().Value()
	}
	if f.IsType(IdxFileTypeMerge) {
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

	baseFn := func(skipFn SkipFn, addFn AddFn) func(skipFn SkipFn, opts ...OptResultFn) {
		return f.recordInfoByKeyFn(key, func(arg RecordInfoArg) {
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
		})
	}

	result.RecChFn = func(in <-chan *query.Record, out chan<- *query.Record) {

		if in == nil {
			goto NO_IN
		}

		go func(in <-chan *query.Record, out chan<- *query.Record) {
			defer recoverAndIgnore()
			for rec := range in {
				if rec == nil {
					break
				}

				baseFn(EmptySkip, func(r interface{}) {
					cRec := r.(*query.Record)
					if rec.FileId().Uint64() != cRec.FileId().Uint64() {
						return
					}
					if rec.Offset().Int64() != cRec.Offset().Int64() {
						return
					}
					out <- cRec
				})(EmptySkip, OptFilterWithFile(rec.FileId().Uint64(), rec.Offset().Int64()))
			}
			out <- nil

		}(in, out)
		return

	NO_IN:
		go func(out chan<- *query.Record) {
			defer recoverAndIgnore()
			baseFn(EmptySkip, func(r interface{}) {
				cRec := r.(*query.Record)
				out <- cRec
			})(EmptySkip)
			out <- nil
		}(out)

		return
	}

	result.RecFn = func(skipFn SkipFn) (records []*query.Record) {
		baseFn(skipFn, func(r interface{}) {
			records = append(records, r.(*query.Record))
		})(skipFn)
		return
	}

	result.CntFn = func(skipFn SkipFn) (cnt int) {
		baseFn(skipFn, func(r interface{}) {
			cnt++
		})(skipFn)
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
func (f *IndexFile) recordByKeyChFn(key uint64) RecordChFn {
	return f.commonFnByKey(key).RecChFn
}

func (f *IndexFile) countFnBy(key uint64) CountFn {
	return f.commonFnByKey(key).CntFn
}

func (f *IndexFile) recordInfoByKeyFn(key uint64, fn InfoFn) ResultFn {

	return func(skipFn SkipFn, opts ...OptResultFn) {
		opt := DefaultCofigResultFn
		for _, optF := range opts {
			optF(&opt)
		}

		elapsed := MesureElapsed()
		defer func() {
			if LogIsDebug() {
				Log(LOG_DEBUG, "RecordByKey(%s) %s\n", DecodeTri(key), elapsed("%s"))
			}
		}()
		var idxs []*IndexFile
		if opt.useFileFIlter {
			idxs = f.findByKeyAndRecord(key, opt.fileID, opt.offset)
		} else {
			idxs = f.FindByKey(key)
		}
		skipCur := 0
		for _, idx := range idxs {
			if idx == nil {
				continue
			}
			Log(LOG_DEBUG, "recordInfoByKey() idx.path=%s\n", idx.Path)

			if skipFn(skipCur) == SkipFinish {
				return
			}
			if idx.IsType(IdxFileTypeWrite) {
				if skipFn(skipCur) == SkipTrue {
					skipCur++
					continue
				}
				fn(RecordInfoArg{false, idx.KeyRecord(), nil, skipCur, skipFn})
				skipCur++
			} else if idx.IsType(IdxFileTypeMerge) {
				var kr *query.KeyRecord
				kr = nil
				if Opt.useBsearch {
					krList := idx.KeyRecords()

					krList.IO = base.NewNoLayer(krList.IO)
					krList.NodeList.ValueInfo = base.ValueInfo(krList.List().InfoSlice())

					nKrList := query.NewKeyRecordList()
					nKrList.IO = base.NewNoLayer(nKrList.IO)
					nKrList.IO.Impl().Copy(krList.IO.Impl(), krList.NodeList.ValueInfo.Pos-4,
						krList.NodeList.ValueInfo.Size+4, 0, 0)
					krList = nKrList
					krList.NodeList.ValueInfo = base.ValueInfo(krList.List().InfoSlice())
				SORT:

					if !krList.List().IsSorted(MakeKRLessFn(krList)) {
						krList.SortBy(MakeKRLessFn(krList))
						goto SORT
					}

					kr = BsearchInKeyRecord(key, krList.Search(func(q *query.KeyRecord) bool {
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
				if opt.fileID > 0 {
					lessFn := func(i, j int) bool {
						iRec := kr.Records().AtWihoutError(i)
						jRec := kr.Records().AtWihoutError(j)
						if iRec.FileId().Uint64() < jRec.FileId().Uint64() {
							return true
						}
						if iRec.FileId().Uint64() != jRec.FileId().Uint64() {
							return false
						}
						if iRec.Offset().Int64() < jRec.Offset().Int64() {
							return true
						}
						return true

					}
					if !kr.Records().List().IsSorted(lessFn) {
						kr.Records().SortBy(lessFn)
					}
					recI := kr.Records().SearchIndex(func(r *query.Record) bool {
						if r.FileId().Uint64() < opt.fileID {
							return true
						}
						if r.FileId().Uint64() > opt.fileID {
							return false
						}
						if r.Offset().Int64() < opt.offset {
							return true
						}
						return false
					})
					if recI < 0 {
						recI = 0
					}

					if kr.Records().AtWihoutError(recI).FileId().Uint64() != opt.fileID {
						goto GOTO_NEXT
					}
					if kr.Records().AtWihoutError(recI).Offset().Int64() != opt.offset {
						goto GOTO_NEXT
					}
				}
				fn(RecordInfoArg{true, nil, kr, skipCur, skipFn})
			GOTO_NEXT:
				skipCur += kr.Records().Count()
			}
		}
		return
	}
}

func (f *IndexFile) keyRecordsBy(key uint64, less bool) <-chan *query.KeyRecord {
	ch := make(chan *query.KeyRecord, 10)
	idx := f
	var i int

	if !f.IsType(IdxFileTypeMerge) {
		close(ch)
		return ch
	}

	lessFn := func(i, j int) bool {
		return idx.KeyRecords().AtWihoutError(i).Key().Uint64() < idx.KeyRecords().AtWihoutError(j).Key().Uint64()
	}
	loopCond := func(i, size int, less bool) bool {
		if less {
			return i > -1
		}

		return i < size
	}

	loopInc := func(i int, less bool) int {
		if less {
			return i - 1
		}
		return i + 1
	}

	if !idx.KeyRecords().List().IsSorted(lessFn) {
		goto NO_SORTED
	}

	i = idx.KeyRecords().SearchIndex(func(kr *query.KeyRecord) bool {
		return kr.Key().Uint64() < key
	})

	if i >= idx.KeyRecords().Count() {
		i = idx.KeyRecords().Count() - 1
	}
	if i < 0 {
		i = 0
	}

	go func(ch chan<- *query.KeyRecord) {
		for j := i; loopCond(j, idx.KeyRecords().Count(), less); j = loopInc(j, less) {
			v := idx.KeyRecords().AtWihoutError(j)
			if v == nil {
				continue
			}
			ch <- v
		}
		close(ch)
	}(ch)
	return ch

NO_SORTED:

	krs := f.KeyRecords().Select(func(kr *query.KeyRecord) bool {
		if less && (kr.Key().Uint64() <= key) {
			return true
		}
		if !less && (kr.Key().Uint64() >= key) {
			return true
		}
		return false
	})
	go func(ch chan<- *query.KeyRecord) {
		for _, kr := range krs {
			ch <- kr
		}
		close(ch)
	}(ch)

	return ch

}

func MakeLessFn(recs *query.RecordList) func(i, j int) bool {

	return func(i, j int) bool {
		iRec := recs.AtWihoutError(i)
		jRec := recs.AtWihoutError(j)
		if iRec.FileId().Uint64() < jRec.FileId().Uint64() {
			return true
		}
		if iRec.FileId().Uint64() != jRec.FileId().Uint64() {
			return false
		}
		if iRec.Offset().Int64() < jRec.Offset().Int64() {
			return true
		}
		return true
	}
}

func MakeKRLessFn(krList *query.KeyRecordList) func(i, j int) bool {

	return func(i, j int) bool {
		iKr := krList.AtWihoutError(i)
		jKr := krList.AtWihoutError(j)
		return iKr.Key().Uint64() < jKr.Key().Uint64()
	}
}

func MakeSearchInxexFn(fileID uint64, offset int64) func(r *query.Record) bool {

	return func(r *query.Record) bool {
		if r.FileId().Uint64() < fileID {
			return false
		}
		if r.FileId().Uint64() > fileID {
			return true
		}
		if r.Offset().Int64() < offset {
			return false
		}
		return true
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
func recoverAndIgnore() {
	if x := recover(); x != nil {
		Log(LOG_WARN, "avoid write close channel=%v", x)
	}
}

func (f *IndexFile) commonNearFnByKey(key uint64, less bool) (result SearchFn) {

	type AddFn func(interface{})

	baseFn := func(skipFn SkipFn, addFn AddFn) func(skipFn SkipFn, opts ...OptResultFn) {
		return func(skipFn SkipFn, opts ...OptResultFn) {
			opt := DefaultCofigResultFn
			for _, optF := range opts {
				optF(&opt)
			}

			skipCur := 0
			var sidx, lidx uint64

			midxs := ListMergedIndex(f.c, func(f *IndexFile) CondType {
				if f.IsType(IdxFileTypeNoComplete) {
					return CondSkip
				}
				if f.IsType(IdxFileTypeMerge) {
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

				for kr := range idx.keyRecordsBy(key, less) {
					if opt.fileID > 0 {
						if !kr.Records().List().IsSorted(MakeLessFn(kr.Records())) {
							kr.Records().SortBy(MakeLessFn(kr.Records()))
						}
						rIdx := kr.Records().SearchIndex(MakeSearchInxexFn(opt.fileID, opt.offset))
						if rIdx >= kr.Records().Count() {
							rIdx = kr.Records().Count() - 1
						}
						if rIdx < 0 {
							rIdx = 0
						}
						rec := kr.Records().AtWihoutError(rIdx)
						if rec.FileId().Uint64() != opt.fileID || rec.Offset().Int64() != opt.offset || skipFn(skipCur+rIdx) == SkipTrue {
							skipCur += kr.Records().Count()
							continue
						}

						addFn(rec)

					} else {

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
					}
					skipCur += kr.Records().Count()
				}
			}

			if less {
				sidx = OpenIndexFile(f.c).First().IdxInfo().first
				lidx = key

				if len(midxs) > 0 {
					// FIXME
					// nsidx := midxs[len(midxs)-1].IdxInfo().last + 1
					// if nsidx < lidx {
					// 	sidx = nsidx
					// }
				}

			} else {
				sidx = key
				lidx = OpenIndexFile(f.c).Last().IdxInfo().last
				if len(midxs) > 0 {
					sidx = midxs[len(midxs)-1].IdxInfo().last + 1
				}
			}

			selectOpts := []SelectOption{
				OptAsc(less),
				OptOnly(IdxFileTypeWrite),
				OptRange(sidx, lidx),
				OptCcondFn(func(f *IndexFile) CondType {
					//FIXME: merge index support
					if f.IsType(IdxFileTypeNoComplete) || f.IsType(IdxFileTypeMerge) {
						return CondSkip
					} else if f.IsType(IdxFileTypeDir) {
						return CondLazy
					}
					if opt.fileID == 0 {
						return CondTrue
					}

					if f.FirstRecord().fileID == opt.fileID || f.FirstRecord().offset == opt.offset {
						return CondTrue
					}
					return CondSkip

				}),
				OptTraverse(func(f *IndexFile) error {
					if f.IsType(IdxFileTypeWrite) {
						defer func() { skipCur++ }()
						if skipFn(skipCur) == SkipTrue {
							return nil
						}
						if skipFn(skipCur) == SkipFinish {
							return errors.New("traverse finish")
						}

						addFn(f.KeyRecord().Value())
					} else if f.IsType(IdxFileTypeMerge) { // FIXME: not run this routine?
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
			}

			if opt.fileID > 0 {
				selectOpts = append(selectOpts, OptRecord(opt.fileID, opt.offset))
			}

			OpenIndexFile(f.c).Select(selectOpts...)
		}
	}

	result.RecChFn = func(in <-chan *query.Record, out chan<- *query.Record) {
		if in == nil {
			goto NO_IN
		}

		go func(in <-chan *query.Record, out chan<- *query.Record) {
			defer recoverAndIgnore()
			for rec := range in {
				if rec == nil {
					break
				}

				baseFn(EmptySkip, func(r interface{}) {
					cRec := r.(*query.Record)
					if rec.FileId().Uint64() != cRec.FileId().Uint64() {
						return
					}
					if rec.Offset().Int64() != cRec.Offset().Int64() {
						return
					}
					out <- cRec
				})(EmptySkip, OptFilterWithFile(rec.FileId().Uint64(), rec.Offset().Int64()))
			}
			out <- nil

		}(in, out)
		return

	NO_IN:
		go func(out chan<- *query.Record) {
			defer recoverAndIgnore()
			baseFn(EmptySkip, func(r interface{}) {
				cRec := r.(*query.Record)
				out <- cRec
			})(EmptySkip)
			out <- nil
		}(out)

		return

	}

	result.RecFn = func(skipFn SkipFn) (records []*query.Record) {
		defer func() {
			Log(LOG_DEBUG, "RecordNearByKeyFn(): recs=%v\n", records)
		}()

		baseFn(skipFn, func(r interface{}) {
			records = append(records, r.(*query.Record))
		})(skipFn)
		return
	}
	result.CntFn = func(skipFn SkipFn) (cnt int) {
		defer func() {
			Log(LOG_DEBUG, "CountNearByKeyFn(): cnt=%d\n", cnt)
		}()
		baseFn(skipFn, func(r interface{}) {
			cnt++
		})(skipFn)
		return cnt
	}

	return
}

func (f *IndexFile) findByKeyAndRecord(key uint64, fileID uint64, offset int64) (result []*IndexFile) {

	c := f.c
	if filepath.Join(Opt.rootDir, c.TableDir()) != f.Path {
		return nil //ErrNotIndexDir
	}

	strkey := toFnameTri(key)
	if c.IsNum {
		strkey = toFname(key)
	}

	pat := ColumnPathWithStatus(c.TableDir(), c.Name, c.IsNum, strkey, strkey, RECORD_WRITTEN)

	var paths []string
	if fileID == 0 {
		paths, _ = filepath.Glob(fmt.Sprintf("%s.*.*", pat))
	} else {
		paths, _ = filepath.Glob(fmt.Sprintf("%s.%010x.%010x", pat, fileID, offset))
	}

	if len(paths) > 0 {
		for _, path := range paths {
			matchfile := NewIndexFile(c, path)
			result = append(result, matchfile)
		}
		return
	}

	return f.findAllFromMergeIdxs(key)
}

func (f *IndexFile) FindByKey(key uint64) (result []*IndexFile) {

	return f.findByKeyAndRecord(key, 0, 0)
}

func (f *IndexFile) FindNearByKey(key uint64, less bool) (results []*IndexFile) {

	return f.findNearByKeyAndRecord(key, less, 0, 0)
}

func (f *IndexFile) findNearByKeyAndRecord(key uint64, less bool, fileID uint64, offset int64) (results []*IndexFile) {

	results = f.findByKeyAndRecord(key, fileID, offset)
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

		e := dir.Select(
			OptAsc(less),
			OptCcondFn(func(f *IndexFile) CondType {
				if f.IsType(IdxFileTypeNoComplete) {
					return CondSkip
				} else if f.IsType(IdxFileTypeMerge) {
					return CondSkip
				} else if f.IsType(IdxFileTypeDir) {
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

	names, err := dirnamesByType(f.Path, IdxFileTypeMerge)
	loncha.Filter(&names, func(i int) bool {
		return c.Name == names[i][0:len(c.Name)]
	})

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
		return f.IsType(IdxFileTypeMerge) && key >= f.IdxInfo().first && key <= f.IdxInfo().last
	})
	result = make([]*IndexFile, 0, len(names)-idx)

	for i := idx; i < len(names); i++ {
		f := NewIndexFile(f.c, filepath.Join(f.Path, names[i]))
		if f.IsType(IdxFileTypeNoComplete) {
			continue
		}
		if f.IsType(IdxFileTypeMerge) {
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

		if f.IsType(IdxFileTypeNoComplete) {
			continue
		}
		if f.IsType(IdxFileTypeMerge) {
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
		cDirs = append(cDirs, c)
	}

	loncha.Delete(&cDirs, func(i int) bool {
		return !cDirs[i].IsType(t) || cDirs[i].IsType(IdxFileTypeNoComplete)
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
	cDirs := p.childs(IdxFileTypeDir)

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

	cDirs := d.childs(IdxFileTypeDir)
	if len(cDirs) == 1 {
		fmt.Printf("1 cDirs=%+v\n", cDirs[0])
		return cDirs[0].middleAsDir()
	}
	if len(cDirs) > 1 {
		fmt.Printf("> 1 cDirs=%+v\n", cDirs[0])
		return cDirs[0].middle(cDirs[len(cDirs)-1])
	}

	cFiles := d.childs(IdxFileTypeWrite)
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

	files := d.childs(IdxFileTypeWrite)
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

func id2RecordsToKeyRecordList(krlist *query.KeyRecordList, KkeyID2Records map[uint64]*query.RecordList) (wBar *mpb.Bar) {

	keys := make([]uint64, 0, len(KkeyID2Records))

	wBar = Pbar.Add("create KeyRecordlist", len(keys))

	for key := range KkeyID2Records {
		keys = append(keys, key)
	}

	sort.SliceStable(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	for i, key := range keys {
		recs := KkeyID2Records[key]
		recs.Flatten()
		kr := query.NewKeyRecord()
		kr.IO = base.NewNoLayer(kr.IO)
		kr.SetKey(query.FromUint64(key))
		kr.SetRecords(recs)
		kr.Flatten()
		krlist.SetAt(i, kr)
		wBar.Increment()
	}
	krlist.Flatten()
	wBar.SetTotal(int64(len(keys)), true)
	return
}
