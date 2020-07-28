package vfsindex

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/kazu/vfs-index/query"
)

type IndexFileType int

const (
	IdxFileType_None IndexFileType = 0
	IdxFileType_Dir  IndexFileType = 1 << iota
	IdxFileType_Merge
	IdxFileType_Write
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

// func (f *IndexFile) Select(asc bool) (result []*IndexFile) {

// 	names, err := readDirNames(f.Path)
// 	if err != nil {
// 		return nil
// 	}
// 	if !asc {
// 		sort.Slice(names, func(i, j int) bool {
// 			return names[i] > names[i]
// 		})
// 	}
// 	afters := []*IndexFile{}

// 	for _, name := range names {
// 		f := NewIndexFile(f.c, filepath.Join(f.Path, name))
// 		f.Init()

// 		if f.IsType(IdxFileType_NoComplete) {
// 			continue
// 		}
// 		if f.IsType(IdxFileType_Write) {
// 			afters = append(result, f)
// 			continue
// 		}
// }

// First ... Find first IndexFile.
func (f *IndexFile) First() *IndexFile {

	names, err := readDirNames(f.Path)
	if err != nil {
		return nil
	}
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
	sort.Slice(names, func(i, j int) bool {
		return names[i] > names[i]
	})

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
