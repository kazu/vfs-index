package vfsindex

import (
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

func OpenIndexFile(c *Column) (idxFile *IndexFile) {

	path := filepath.Join(Opt.rootDir, c.TableDir())

	idxFile = NewIndexFile(c, path)
	idxFile.Ftype = IdxFileType_Dir
	c.IsNum = c.IsNumViaIndex()
	return
}

func NewIndexFile(c *Column, path string) *IndexFile {
	return &IndexFile{
		Path: path,
		c:    c,
	}
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

type SelectOpt struct {
	asc      bool
	cond     CondFn
	traverse TraverseFn
}

type SelectOption func(*SelectOpt)
type CondType byte
type TraverseFn func(f *IndexFile) error
type CondFn func(f *IndexFile) CondType

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

func (opt *SelectOpt) Merge(opts []SelectOption) {
	for i := range opts {
		opts[i](opt)
	}
}

func (f *IndexFile) Select(opts ...SelectOption) (err error) {
	opt := SelectOpt{}
	opt.Merge(opts)

	names, err := readDirNames(f.Path)
	names = sortAlphabet(names)
	if !opt.asc {
		sort.SliceStable(names, func(i, j int) bool { return i > j })
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
				fmt.Printf("dir=%s path=%s\n", f.Path, r.Path)
				return r
			}
			//dirs = append(dirs, f)
			continue
		}
	}
	if len(afters) > 0 {
		//sort.Slice(afters, func(i, j int) bool { return i > j })
		for _, f := range afters {
			if r := f.Last(); r != nil {
				return r
			}
		}
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
	//fmt.Printf("%s\n", path)
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
