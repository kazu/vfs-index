package vfsindex

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kazu/vfs-index/query"
	"github.com/stretchr/testify/assert"
)

// func TestMain(m *testing.M) {
// 	setup()
// 	ret := m.Run()
// 	if ret == 0 {
// 		//teardown()
// 	}
// 	os.Exit(ret)
// }

const IdxDir string = "testdata/vfs-inter"
const DataDir string = "testdata/data"
const TestRoot string = "testdata"
const IdxNoInterDir string = "testdata/vfs"

func setup() {

	CurrentLogLoevel = LOG_WARN
	if FileExist(IdxDir) {
		//os.RemoveAll(IdxDir)
		return
	}
	Untar("testdata/idx-inter.tar.gz", "testdata")
}

func teardown() {
	if FileExist(IdxDir) {
		os.RemoveAll(IdxDir)
	}
	if FileExist(IdxNoInterDir) {
		os.RemoveAll(IdxNoInterDir)
	}
}

func Test_IndexFile(t *testing.T) {
	setup()

	CurrentLogLoevel = LOG_WARN
	idx, e := Open(DataDir, RootDir(IdxDir))

	sCond :=
		idx.On("test",
			MergeDuration(1*time.Second),
			ReaderColumn("content"),
			MergeOnSearch(false))

	c := sCond.Column()
	finder := OpenIndexFile(c)
	keyrecords := finder.First().KeyRecords()
	kr, e := keyrecords.First()

	assert.NoError(t, e)
	assert.Equal(t, IdxFileType_Dir, finder.Ftype)
	assert.True(t, keyrecords.Count() > 0)
	assert.Equal(t, kr.Key().Uint64(), finder.First().IdxInfo().first)

	kr2 := finder.Last().KeyRecord()
	assert.Equal(t, kr2.Key().Uint64(), finder.Last().IdxInfo().last)

	f := NewIndexFile(c, IdxDir+"test/content.gram.idx.merged.000a000a0023-003000385e74")
	f.Init()

	assert.True(t, f.IsType(IdxFileType_Merge))
	assert.Equal(t, uint64(0x000a000a0023), f.IdxInfo().first)

	f = NewIndexFile(c, IdxDir+"/test/0033/0039/0053/id.num.idx.003300390053-003300390053.00064507ea.0000000004")
	f.Init()

	assert.Equal(t, IdxFileType_None, f.Ftype)
	f = NewIndexFile(c, IdxDir+"/test/0033/0033/0035/content.gram.idx.003300330035-003300330035.00064507ea.0000000004")
	f.Init()
	a := f.KeyRecord().Key().Int64()
	_ = a
	assert.True(t, f.IsType(IdxFileType_Write))

}
func Test_FindByKey_IndexFIle(t *testing.T) {

	setup()

	CurrentLogLoevel = LOG_WARN
	idx, e := Open(DataDir,
		RootDir(IdxDir))

	sCond := idx.On("test", ReaderColumn("id"), MergeOnSearch(true))

	c := sCond.Column()
	finder := OpenIndexFile(c)

	f := finder.FindByKey(1944369)

	assert.NoError(t, e)
	assert.NotNil(t, uint64(1944369), f[0].IdxInfo().first)

	f = finder.FindByKey(3301755)

	assert.NotNil(t, uint64(3301755), f[0].IdxInfo().first)
}

func Test_SearchCond_First(t *testing.T) {

	setup()

	CurrentLogLoevel = LOG_WARN
	idx, e := Open(DataDir,
		RootDir(IdxDir))

	sCond := idx.On("test", ReaderColumn("id"), MergeOnSearch(false))

	str := sCond.FindBy("id", uint64(132763)).First(ResultOutput("json")).(string)

	assert.NoError(t, e)
	assert.True(t, len(str) > 0)
}

func Test_SearchCond_Select(t *testing.T) {

	setup()

	CurrentLogLoevel = LOG_WARN
	idx, e := Open(DataDir,
		RootDir(IdxDir))

	sCond := idx.On("test", ReaderColumn("id"), MergeOnSearch(false))

	str := sCond.Select(func(cond SearchElem) bool {
		return cond.Op("id", "==", uint64(1944367))
	}).First(ResultOutput("json")).(string)
	assert.NoError(t, e)
	assert.True(t, len(str) > 0)

	str = sCond.Select(func(cond SearchElem) bool {
		return cond.Op("id", "==", uint64(1944367))
	}).First(ResultOutput("csv")).(string)
	fmt.Printf("%s\n", str)
	assert.True(t, len(str) > 0)

	infs := sCond.Select(func(cond SearchElem) bool {
		return cond.Op("id", ">", uint64(0))
	}).Limit(3).All().([]interface{})

	assert.Equal(t, 3, len(infs))

}

func Test_SearchCond_SelectGram(t *testing.T) {
	CurrentLogLoevel = LOG_WARN
	setup()

	idx, e := Open(DataDir,
		RootDir(IdxDir))

	sCond := idx.On("test", ReaderColumn("title"), MergeOnSearch(false))

	ostr := sCond.Select(func(cond SearchElem) bool {
		return cond.Op("title", "==", "拉致問")
	}).First(ResultOutput("json"))
	str, ok := ostr.(string)
	_ = ok

	assert.NoError(t, e)
	assert.True(t, len(str) > 4)

}

func Test_SearchCond_FirstGram(t *testing.T) {
	setup()

	CurrentLogLoevel = LOG_WARN
	idx, e := Open(DataDir,
		RootDir(IdxDir))

	sCond := idx.On("test", ReaderColumn("title"), MergeOnSearch(false))

	str := sCond.FindBy("title", "拉致問").First(ResultOutput("json")).(string)

	assert.NoError(t, e)
	assert.True(t, len(str) > 0)
}

func Test_IndexFile_Init(t *testing.T) {

	a := "/Users/xtakei/git/vfs-index/example/vfs-tmp/30ab/30b5/0029/name.gram.idx.30ab30b50029-30ab30b50029.00014829a5.0000040eea"
	strs := strings.Split(filepath.Base(a), "name.gram.idx")

	assert.NotNil(t, a)
	assert.True(t, len(strs) >= 2)
	strs = strings.Split(filepath.Base(a), "id.num.idx")

	assert.True(t, len(strs) < 2)
}

func Test_SearchCondQuery_FirstGram(t *testing.T) {
	setup()

	CurrentLogLoevel = LOG_WARN
	idx, e := Open(DataDir,
		RootDir(IdxDir))

	sCond := idx.On("test", ReaderColumn("title"), MergeOnSearch(false))

	str := sCond.Query(`title == "拉致問"`).First(ResultOutput("json")).(string)

	assert.NoError(t, e)
	assert.True(t, len(str) > 0)
}

func Test_SearchCondQueryLess_FirstGram(t *testing.T) {
	setup()

	CurrentLogLoevel = LOG_WARN
	idx, e := Open(DataDir,
		RootDir(IdxDir))

	sCond := idx.On("test", ReaderColumn("title"), MergeOnSearch(false))

	str := sCond.Query(`title <= "拉致問"`).First(ResultOutput("json")).(string)

	assert.NoError(t, e)
	assert.True(t, len(str) > 0)
}

func Test_IndexFile_Select(t *testing.T) {
	setup()

	CurrentLogLoevel = LOG_WARN
	s := time.Now()
	idx, e := Open(DataDir,
		RootDir(IdxDir))
	fmt.Printf("open: elapse %s\n", time.Now().Sub(s))

	sCond := idx.On("test", ReaderColumn("content"), MergeOnSearch(true))
	fmt.Printf("On: elapse %s\n", time.Now().Sub(s))

	c := sCond.Column()
	finder := OpenIndexFile(c)

	matches := []*IndexFile{}

	e = finder.Select(
		OptAsc(true),
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

			matches = append(matches, f)
			if len(matches) == 100 {
				return ErrStopTraverse
			}
			return nil
		}),
	)
	fmt.Printf("Select1: elapse %s\n", time.Now().Sub(s))

	assert.Error(t, e)
	assert.True(t, len(matches) == 100)

	sCond = idx.On("test", ReaderColumn("content"), MergeOnSearch(true))
	fmt.Printf("On2: elapse %s\n", time.Now().Sub(s))
	c = sCond.Column()
	matches = []*IndexFile{}

	f := OpenIndexFile(c)
	k2rel := func(key uint64) (ret string) {
		path := f.c.key2Path(key, RECORD_WRITTEN)
		ret, _ = filepath.Rel(filepath.Join(Opt.rootDir, f.c.TableDir()), path)
		return
	}
	path, _ := filepath.Rel(filepath.Join(Opt.rootDir, f.c.TableDir()), filepath.Join(f.Path, "0045"))
	isLess := LessEqString(k2rel(0x4500580000), path) && LessEqString(path, k2rel(0x4500582664))
	_ = isLess

	e = f.Select(
		OptAsc(true),
		OptRange(0, 0x4500582664),
		OptCcondFn(func(f *IndexFile) CondType {
			if f.Ftype == IdxFileType_None {
				return CondSkip
			}

			if f.IsType(IdxFileType_NoComplete) {
				return CondSkip
			}
			fmt.Printf("cond %s\n", f.Path)
			if f.IsType(IdxFileType_Dir) {
				return CondLazy
			}

			if f.IsType(IdxFileType_Merge) {
				return CondSkip
			}
			return CondTrue
		}),
		OptTraverse(func(f *IndexFile) error {
			fmt.Printf("match %s\n", f.Path)
			matches = append(matches, f)
			if len(matches) == 100 {
				return ErrStopTraverse
			}
			return nil
		}),
	)
	fmt.Printf("Select2: elapse %s\n", time.Now().Sub(s))
	assert.Error(t, e)
	assert.Equal(t, 100, len(matches))
}

func Test_Recrod_ToFbs(t *testing.T) {

	r := &Record{
		fileID: 1,
		offset: 2,
		size:   3,
	}
	r.cache = map[string]interface{}{}
	r.cache["name"] = interface{}("hogehoge")

	buf := r.ToFbs(TriKeys("hogehoge")[0])

	trikeies := TriKeys("hogehoge")
	_ = trikeies
	root := query.OpenByBuf(buf)
	a := root.Index().InvertedMapNum().Key().Uint64()
	_ = a
	assert.NotEqual(t, uint64(0),
		root.Index().InvertedMapNum().Key().Uint64())

}

func Test_IndexFile_parentWith(t *testing.T) {
	setup()

	CurrentLogLoevel = LOG_WARN
	idx, _ := Open(DataDir,
		RootDir(IdxDir))

	sCond := idx.On("test", ReaderColumn("content"), MergeOnSearch(false))

	c := sCond.Column()

	f1 := NewIndexFile(c, "testdata/vfs-inter/test/ff1a/0031/0038/content.gram.idx.ff1a00310038-ff1a00310038.00064507ea.0000000004")
	f1.Init()

	f2 := NewIndexFile(c, "testdata/vfs-inter/test/ff1a/7b2c/0031/content.gram.idx.ff1a7b2c0031-ff1a7b2c0031.00064507ea.0000000004")
	f2.Init()

	f3 := f1.parentWith(f2)

	assert.Equal(t, f3.Path, "testdata/vfs-inter/test/ff1a")

	d := NewIndexFile(c, f3.Path)
	d.Init()

	childs := d.childs(IdxFileType_Dir)
	assert.True(t, len(childs) > 0)

	// m := f2.middle(f1)
	// _ = m
	// assert.True(t, len(childs) > 0)

	tests := []struct {
		path1 string
		path2 string
	}{
		{
			"testdata/vfs-inter/test/0045/0066/006e/content.gram.idx.00450066006e-00450066006e.00064507ea.0000000004",
			"testdata/vfs-inter/test/ff5e/005b/005b/content.gram.idx.ff5e005b005b-ff5e005b005b.00064507ea.0000000004",
		},
		{
			"testdata/vfs-inter/test/0049/0041/007d/content.gram.idx.00490041007d-00490041007d.00064507ea.0000000004",
			"testdata/vfs-inter/test/004c/0075/0063/content.gram.idx.004c00750063-004c00750063.00064507ea.000000e786",
		},
		{
			"testdata/vfs-inter/test/0049/004e/003a/content.gram.idx.0049004e003a-0049004e003a.00064507ea.0000000004",
			"testdata/vfs-inter/test/0059/0054/0041/content.gram.idx.005900540041-005900540041.00064507ea.0000000004",
		},
	}

	for _, tt := range tests {
		t.Run("middle "+tt.path1, func(t *testing.T) {
			f1 := NewIndexFile(c, tt.path1)
			f1.Init()
			f2 := NewIndexFile(c, tt.path2)
			f2.Init()
			m := f1.middle(f2)
			assert.NotNil(t, m)
		})
	}

}

func Test_IndexFile_FindNearByKey(t *testing.T) {
	setup()

	CurrentLogLoevel = LOG_WARN
	idx, _ := Open(DataDir,
		RootDir(IdxDir))

	sCond := idx.On("test", ReaderColumn("title"), MergeOnSearch(false))

	c := sCond.Column()
	finder := OpenIndexFile(c)

	//file := "example/vfs-tmp/test/0045/0078/0045/name.gram.idx.004500780045-004500780045"
	key := TriKeys("拉致問")[0] + 1

	results := finder.FindNearByKey(key, true)

	assert.True(t, key >= results[0].IdxInfo().first)

	results = finder.FindNearByKey(key, false)
	assert.True(t, key <= results[0].IdxInfo().last)
}

func Test_IndexFile_RecordByKey(t *testing.T) {
	setup()

	CurrentLogLoevel = LOG_WARN
	idx, _ := Open(DataDir,
		RootDir(IdxDir))

	sCond := idx.On("test", ReaderColumn("title"), MergeOnSearch(false))

	finder := OpenIndexFile(sCond.Column())

	keys := TriKeys("拉致問題")

	fn := finder.recordByKey(keys[0])

	sf2 := NewSearchFinder(sCond.Column())
	sf2.recordFns = append(sf2.recordFns, fn)
	sf2.skipdFns = append(sf2.skipdFns, EmptySkip)
	sf2.skipdFns[0] = sf2.And(0, keys[1])
	results := sf2.All().([]interface{})

	result := results[0].(map[string]interface{})
	val := result[sCond.Column().Name].(string)

	assert.True(t, len(val) > 0)
	assert.Equal(t, "北朝鮮による日本人拉致問題", val)
}

func Test_IndexFile_RecordNearByKey(t *testing.T) {
	setup()

	CurrentLogLoevel = LOG_WARN
	idx, _ := Open(DataDir,
		RootDir(IdxDir))

	sCond := idx.On("test", ReaderColumn("content"), MergeOnSearch(false))

	finder := OpenIndexFile(sCond.Column())

	key := uint64(0x5300740075)
	//key := uint64(0x0a000a0033)

	fn := finder.RecordNearByKey(key, true)

	sf2 := NewSearchFinder(sCond.Column())
	sf2.recordFns = append(sf2.recordFns, fn)
	sf2.skipdFns = append(sf2.skipdFns, EmptySkip)
	alls := sf2.All()

	results := alls.([]interface{})
	vals := []string{}

	for i := range results {
		result := results[i].(map[string]interface{})
		val := result[sCond.Column().Name].(string)
		vals = append(vals, val)
	}

	assert.Equal(t, 22, len(vals))
}

func Test_IndexFile_Select2(t *testing.T) {
	setup()

	CurrentLogLoevel = LOG_WARN
	idx, _ := Open(DataDir,
		RootDir(IdxDir))

	sCond := idx.On("test", ReaderColumn("title"), MergeOnSearch(false))

	results := sCond.Select(func(cond SearchElem) bool {
		return cond.Op("title", "==", "拉致問")
	}).All().([]interface{})

	result := results[0].(map[string]interface{})
	val := result[sCond.Column().Name].(string)

	assert.True(t, len(val) > 0)
	assert.Equal(t, "北朝鮮による日本人拉致問題", val)
}

func Test_Parse_CSV(t *testing.T) {
	setup()

	CurrentLogLoevel = LOG_WARN
	idx, _ := Open(DataDir,
		RootDir(IdxDir))

	sCond := idx.On("test", ReaderColumn("title"), MergeOnSearch(false))
	_ = sCond

	dec, e := GetDecoder("test.1.csv")
	assert.NoError(t, e)

	ctx, cancel := context.WithCancel(context.Background())

	fname := DataDir + "/test.1.csv"
	f, e := os.Open(fname)
	assert.NoError(t, e)

	var rec *Record
	for r := range dec.Tokenizer(ctx, f, &File{id: 123, name: fname, index_at: time.Now().UnixNano()}) {
		rec = r
	}
	cancel()
	f.Close()
	ff, _ := os.Open(fname)
	raw, _ := ioutil.ReadAll(ff)
	ff.Close()
	raw = raw[rec.offset : rec.offset+rec.size]

	data := make(map[string]interface{})
	e = dec.Decoder(raw, &data)
	assert.NoError(t, e)
	assert.Equal(t, "ぺこぱ", data["title"].(string))
}

func Test_IndexFile_cleanDirs(t *testing.T) {
	setup()

	CurrentLogLoevel = LOG_WARN
	idx, _ := Open(DataDir,
		RootDir(IdxDir))

	sCond := idx.On("test", ReaderColumn("title"), MergeOnSearch(false))

	c := sCond.Column()

	c.cleanDirs()
	dirs := c.emptyDirs()

	assert.True(t, len(dirs) == 0)
}

func Test_Fn(t *testing.T) {

	a := struct {
		fn func() int
	}{
		fn: func() int { return 1 },
	}

	b := a.fn
	a.fn = func() int { return 2 }

	assert.Equal(t, 1, b())

}

func Test_decodeTri(t *testing.T) {

	s := "好きな"

	t3 := EncodeTri(s)
	assert.Equal(t, 1, len(t3))

	v, _ := strconv.ParseUint(t3[0], 16, 64)
	vv := (v >> (8 * 4) & 0xffff)
	assert.Equal(t, s, DecodeTri(v), fmt.Sprintf("s=%s v=0x%x", t3[0], vv))

}

func Test_countByIndexFile(t *testing.T) {

	setup()

	CurrentLogLoevel = LOG_WARN
	idx, _ := Open(DataDir, RootDir(IdxDir))

	sCond :=
		idx.On("test",
			MergeDuration(1*time.Second),
			ReaderColumn("content"),
			MergeOnSearch(false))

	c := sCond.Column()
	finder := OpenIndexFile(c)
	tris := TriKeys("活動内")
	cnt := finder.countBy(tris[0])

	assert.Equal(t, cnt, 4)
}

// func Test_RecordByKey2(t *testing.T) {

// 	setup()

// 	CurrentLogLoevel = LOG_WARN
// 	idx, _ := Open(DataDir, RootDir(IdxDir))

// 	sCond :=
// 		idx.On("test",
// 			MergeDuration(1*time.Second),
// 			ReaderColumn("content"),
// 			MergeOnSearch(false))

// 	c := sCond.Column()
// 	finder := OpenIndexFile(c)
// 	tris := TriKeys("活動内")
// 	//cnt := finder.countBy(tris[0])
// 	qr1 := finder.recordByKey(tris[0])(EmptySkip)
// 	qr2 := finder.RecordByKey2(tris[0])(EmptySkip)
// 	assert.Equal(t, qr1, qr2)

// 	type TRecord struct {
// 		fileID uint64
// 		offset int64
// 		size   int64
// 	}

// 	fn := func(qr *query.Record) TRecord {
// 		return TRecord{
// 			qr.FileId().Uint64(),
// 			qr.Offset().Int64(),
// 			qr.Size().Int64(),
// 		}
// 	}
// 	assert.Equal(t, len(qr1), len(qr2))
// 	if len(qr1) != len(qr2) {
// 		return
// 	}
// 	for i := range qr1 {
// 		assert.Equal(t, fn(qr1[i]), fn(qr2[i]), i)
// 	}
// 	assert.NotEqual(t, TRecord{1, 0, 0}, TRecord{2, 0, 0})
// }
