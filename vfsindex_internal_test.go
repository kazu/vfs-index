package vfsindex

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kazu/fbshelper/query/base"
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

const fileIDOfidxInter string = "17dCuo_6yhPpp1wq3y6mZCTnIB50enZ45"

func donwloadFromgdrive(id, dst string) {

	resp, _ := http.Get("https://drive.google.com/uc?export=download&id=" + id)
	w, _ := os.Create(dst)
	io.Copy(w, resp.Body)
	w.Close()

}
func setup() {

	CurrentLogLoevel = LOG_WARN
	if FileExist(IdxDir + "/test") {
		//os.RemoveAll(IdxDir)
		return
	}
	if !FileExist("testdata/idx-inter.tar.gz") {
		donwloadFromgdrive(fileIDOfidxInter, "testdata/idx-inter.tar.gz")
	}

	Untar("testdata/idx-inter.tar.gz", "testdata")
}

func teardown() {
	// if FileExist(IdxDir) {
	// 	os.RemoveAll(IdxDir)
	// }
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
	okr, _ := finder.First().OldKeyRecords().First()
	okr.Key()
	kr.Key()

	assert.NoError(t, e)
	assert.Equal(t, IdxFileTypeDir, finder.Ftype)
	assert.True(t, keyrecords.Count() > 0)
	assert.Equal(t, finder.First().IdxInfo().first, kr.Key().Uint64())

	kr2 := finder.Last().KeyRecord()
	assert.Equal(t, kr2.Key().Uint64(), finder.Last().IdxInfo().last)

	f := NewIndexFile(c, IdxDir+"test/content.gram.idx.merged.000a000a0023-003000385e74")
	f.Init()

	assert.True(t, f.IsType(IdxFileTypeMerge))
	assert.Equal(t, uint64(0x000a000a0023), f.IdxInfo().first)

	f = NewIndexFile(c, IdxDir+"/test/0033/0039/0053/id.num.idx.003300390053-003300390053.00064507ea.0000000004")
	f.Init()

	assert.Equal(t, IdxFileTypeNone, f.Ftype)
	f = NewIndexFile(c, IdxDir+"/test/0033/0033/0035/content.gram.idx.003300330035-003300330035.00064507ea.0000000004")
	f.Init()
	a := f.KeyRecord().Key().Int64()
	_ = a
	assert.True(t, f.IsType(IdxFileTypeWrite))

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
	}).Limit(3).All(OptQueryUseChan(true)).([]interface{})

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

	q := sCond.Query(`title <= "拉致問"`)

	str := q.First(ResultOutput("json"), ResultStreamt(true), OptQueryUseChan(true)).(string)

	assert.NoError(t, e)
	assert.True(t, len(str) > 0)
}

func Test_SearchCondQueryLess_FirstGramUseStream(t *testing.T) {
	setup()

	CurrentLogLoevel = LOG_WARN
	idx, e := Open(DataDir,
		RootDir(IdxDir))

	sCond := idx.On("test", ReaderColumn("title"), MergeOnSearch(false))

	q := sCond.Query(`title <= "拉致問" && id >= 1377865`)

	str := q.First(
		ResultOutput("json"),
		OptQueryUseChan(true),
		ResultStreamt(true),
	).(string)

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
		//OptRange(0x0b00582664, 0x4500582664),
		OptCcondFn(func(f *IndexFile) CondType {
			if f.Ftype == IdxFileTypeNone {
				return CondSkip
			}

			if f.IsType(IdxFileTypeNoComplete) {
				return CondSkip
			}
			fmt.Printf("cond %s\n", f.Path)
			if f.IsType(IdxFileTypeDir) {
				return CondLazy
			}

			if f.IsType(IdxFileTypeMerge) {
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

	matches = []*IndexFile{}
	e = f.Select(
		OptAsc(true),
		OptRange(0, 0x4500582664),
		OptOnly(IdxFileTypeWrite),
		OptCcondFn(func(f *IndexFile) CondType {
			if f.Ftype == IdxFileTypeNone {
				return CondSkip
			}

			if f.IsType(IdxFileTypeNoComplete) {
				return CondSkip
			}
			fmt.Printf("cond %s\n", f.Path)
			if f.IsType(IdxFileTypeDir) {
				return CondLazy
			}

			if f.IsType(IdxFileTypeMerge) {
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
	fmt.Printf("Select3: elapse %s\n", time.Now().Sub(s))
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
	assert.NotEqualf(t, uint64(0x0),
		root.Index().InvertedMapNum().Key().Uint64(), "root.Index().InvertedMapNum().Key().Uint64()=0x%x", root.Index().InvertedMapNum().Key().Uint64())

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

	childs := d.childs(IdxFileTypeDir)
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

	fn := finder.recordByKeyFn(keys[0])

	sf2 := NewSearchFinder(sCond.Column())
	sf2.recordFns = append(sf2.recordFns, SearchFn{RecFn: fn})
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

	fn := finder.commonNearFnByKey(key, true)

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

	assert.Equal(t, 24, len(vals))
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
func Test_GetDecoder(t *testing.T) {
	dec, e := GetDecoder("test.1.csv")

	assert.NoError(t, e)
	assert.Equal(t, "csv", dec.FileType)

	dec, e = GetDecoder("test.1.json")

	assert.NoError(t, e)
	assert.Equal(t, "json", dec.FileType)

	dec, e = GetDecoder("test.1.json.lz4")

	assert.NoError(t, e)
	assert.Equal(t, "json", dec.FileType)

}

func Test_Parse_Jsonl(t *testing.T) {
	dec, _ := GetDecoder("test.1.json")
	const jsonStream = `
	{"Name": "Ed", "Text": "Knock knock."}
	{"Name": "Sam", "Text": "Who's there?"}
	{"Name": "Ed", "Text": "Go fmt."}
	{"Name": "Sam", "Text": "Go fmt who?"}
	{"Name": "Ed", "Text": "Go fmt yourself!"}
	`
	rio := strings.NewReader(jsonStream)
	var rec *Record
	ctx, cancel := context.WithCancel(context.Background())
	for r := range dec.Tokenizer(ctx, rio, &File{id: 123, name: "test", index_at: time.Now().UnixNano()}) {
		rec = r
	}
	cancel()
	fmt.Printf("%+v", rec)

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

	c.cleanDirs(nil)
	dirs := c.emptyDirs(nil)

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

func DupBase(src base.IO) (dst base.IO) {

	dbytes := make([]byte, len(src.R(0)))
	copy(dbytes, src.R(0))

	dst = src.NewFromBytes(dbytes)

	dst.SetDiffs(src.GetDiffs())
	return dst
}

func Test_IndexFileMerged_Merge(t *testing.T) {

	setup()

	mergedeCond := func(srcs []*IndexFileMerged, iparams interface{}) error {
		params := iparams.(struct {
			path    string
			cnt     int
			overlap int
		})

		if len(srcs) < 2 {
			return errors.New("fail prepare")
		}
		om0 := srcs[0]

		m1 := srcs[1]

		m0, e := om0.Merge(m1)
		if e != nil {
			return fmt.Errorf("e=%s fail to merge", e)
		}

		if m0.keyRecords().Count() != m1.keyRecords().Count()+om0.keyRecords().Count()-params.overlap {
			return fmt.Errorf("before_merge_cnt=%d src_merge_cnt=%d after_merge_cnt=%d",
				om0.keyRecords().Count(),
				m1.keyRecords().Count(),
				m0.keyRecords().Count())
		}

		okr0, e := om0.keyRecords().Last()
		if e != nil {
			return fmt.Errorf("e=%s base list cannot got last record", e)
		}
		_ = okr0

		kr0, e := m0.keyRecords().Last()
		if e != nil {
			return fmt.Errorf("e=%s error to get last record of merged list ", e)
		}
		kr1, e := m1.keyRecords().Last()
		if e != nil {
			return fmt.Errorf("e=%s error to get source list  for merging list  in  last record", e)
		}

		if kr0.Key().Uint64() != kr1.Key().Uint64() {
			var bkr1, bkr0 strings.Builder
			kr0.Dump(kr0.Node.Pos, base.OptDumpOut(&bkr0), base.OptDumpSize(1000))
			kr1.Dump(kr1.Node.Pos, base.OptDumpOut(&bkr1), base.OptDumpSize(1000))
			return fmt.Errorf("kr0(key=0x%x)=\n%s\nkr1(key=0x%x)=\n%s\n",
				kr0.Key().Uint64(),
				bkr0.String(),
				kr1.Key().Uint64(),
				bkr1.String())

		}

		return nil
	}

	tests := []struct {
		name   string
		data   string
		params struct {
			path    string
			cnt     int
			overlap int
		}
		prepare func(iparams interface{}) []*IndexFileMerged
		condFn  func(srcs []*IndexFileMerged, iparams interface{}) error
	}{
		{
			name: "simple split",
			data: "testdata/vfs-inter/test/content.gram.idx.merged.000a000a0023-003000385e74",
			params: struct {
				path    string
				cnt     int
				overlap int
			}{
				path:    "testdata/vfs-inter/test/content.gram.idx.merged.000a000a0023-003000385e74",
				cnt:     0,
				overlap: 0,
			},
			prepare: func(iparams interface{}) []*IndexFileMerged {
				params := iparams.(struct {
					path    string
					cnt     int
					overlap int
				})

				idxer, e := Open(DataDir, RootDir(IdxDir))
				if e != nil {
					return nil
				}
				m := OpenIndexFileMerged("test", "content", params.path, idxer)
				dsts, e := m.Split(params.cnt)
				if e != nil {
					return nil
				}
				return dsts
			},
			condFn: func(srcs []*IndexFileMerged, iparams interface{}) error {
				if len(srcs) < 2 {
					return errors.New("fail prepare")
				}

				m0 := srcs[0]
				m1 := srcs[1]

				kr0, e := m0.keyRecords().Last()
				if e != nil {
					return fmt.Errorf("e=%s cannot got last record", e)
				}
				kr1, e := m1.keyRecords().First()
				if e != nil {
					return fmt.Errorf("e=%s cannot got last record", e)
				}

				if kr0.Key().Uint64() >= kr1.Key().Uint64() {
					var bkr1, bkr0 strings.Builder
					kr0.Dump(kr0.Node.Pos, base.OptDumpOut(&bkr0), base.OptDumpSize(100))
					kr1.Dump(kr1.Node.Pos, base.OptDumpOut(&bkr1), base.OptDumpSize(100))
					return fmt.Errorf("kr0=%s kr1=%s\n", bkr0.String(), bkr1.String())

				}

				return nil
			},
		},
		{
			name: "simple merge",
			data: "testdata/vfs-inter/test/content.gram.idx.merged.000a000a0023-003000385e74",
			params: struct {
				path    string
				cnt     int
				overlap int
			}{
				path:    "testdata/vfs-inter/test/content.gram.idx.merged.000a000a0023-003000385e74",
				cnt:     10,
				overlap: 0,
			},
			prepare: func(iparams interface{}) []*IndexFileMerged {
				params := iparams.(struct {
					path    string
					cnt     int
					overlap int
				})

				idx, e := Open(DataDir, RootDir(IdxDir))
				if e != nil {
					return nil
				}
				m := OpenIndexFileMerged("test", "content", params.path, idx)
				dsts, e := m.Split(params.cnt)
				if e != nil {
					return nil
				}
				return dsts
			},
			condFn: mergedeCond,
		},
		{
			name: "overlap merge",
			data: "testdata/vfs-inter/test/content.gram.idx.merged.000a000a0023-003000385e74",
			params: struct {
				path    string
				cnt     int
				overlap int
			}{
				path:    "testdata/vfs-inter/test/content.gram.idx.merged.000a000a0023-003000385e74",
				cnt:     100,
				overlap: 2,
			},
			prepare: func(iparams interface{}) []*IndexFileMerged {
				params := iparams.(struct {
					path    string
					cnt     int
					overlap int
				})

				idx, e := Open(DataDir, RootDir(IdxDir))
				if e != nil {
					return nil
				}
				m := OpenIndexFileMerged("test", "content", params.path, idx)
				pdsts, e := m.Split(params.cnt)
				if e != nil {
					return nil
				}
				if params.overlap == 0 {
					return pdsts
				}
				dsts := make([]*IndexFileMerged, 2)
				dsts[0] = pdsts[0]

				cdsts, e := m.Split(params.cnt - params.overlap)
				if e != nil {
					return nil
				}
				c0 := cdsts[0].keyRecords().AtWihoutError(0).Key().Uint64()
				c1 := cdsts[1].keyRecords().AtWihoutError(0).Key().Uint64()
				_, _ = c0, c1

				dsts[1] = cdsts[1]

				return dsts
			},
			condFn: mergedeCond,
		},
	}

	for _, tt := range tests {
		// if tt.name != "overlap merge" {
		// 	continue
		// }
		t.Run(fmt.Sprintf("%s  path=%s cnt=%d", tt.name, tt.params.path, tt.params.cnt), func(t *testing.T) {

			lists := tt.prepare(tt.params)
			assert.NotNil(t, lists)
			assert.NoError(t, tt.condFn(lists, tt.params))
		})
	}

}

func Test_Distance(t *testing.T) {

	src := "鬼滅の刃"
	dst := "炭治郎"

	CurrentLogLoevel = LOG_WARN
	idx, e := Open(DataDir, RootDir(IdxDir))

	sCond := idx.On("test", ReaderColumn("content"), Output(MapInfOutput))
	hoge := sCond.distanceOfString(src, dst)

	assert.NoError(t, e)
	assert.NotNil(t, hoge)

	//assert.Truef(t, dur > dur2, "dur=%s <= dur2=%s", dur, dur2)

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
