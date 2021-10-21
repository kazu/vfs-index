package vfsindex_test

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
	"time"

	"go/types"

	"github.com/kazu/fbshelper/query/base"
	vfs "github.com/kazu/vfs-index"
	"github.com/kazu/vfs-index/expr"
	"github.com/kazu/vfs-index/query"
	"github.com/stretchr/testify/assert"
)

const IdxDir string = "testdata/vfs"
const DataDir string = "testdata/data"
const TestRoot string = "testdata"

const IdxInterDir string = "testdata/vfs-inter"

const fileIdOfidx string = "16HSrgkXLGP27-TisHK3U0poQRvIPsxxN"

func DefaultOption() vfs.Option {
	return vfs.RootDir(IdxDir)
}

func OpenIndexer() (*vfs.Indexer, error) {
	return vfs.Open(DataDir, DefaultOption(), vfs.MergeDuration(10*time.Second))

}

func donwloadFromgdrive(id, dst string) {

	resp, _ := http.Get("https://drive.google.com/uc?export=download&id=" + id)
	w, _ := os.Create(dst)
	io.Copy(w, resp.Body)
	w.Close()

}

func setup() {

	vfs.CurrentLogLoevel = vfs.LOG_WARN
	if vfs.FileExist(IdxDir) {
		//os.RemoveAll(IdxDir)
		return
	}
	if !vfs.FileExist("testdata/idx.tar.gz") {
		donwloadFromgdrive(fileIdOfidx, "testdata/idx.tar.gz")
	}

	vfs.Untar("testdata/idx.tar.gz", "testdata")
}

func teardown() {
	if vfs.FileExist(IdxDir) {
		os.RemoveAll(IdxDir)
	}
	// if vfs.FileExist(IdxInterDir) {
	// 	os.RemoveAll(IdxInterDir)
	// }
}

func TestMain(m *testing.M) {
	setup()
	use_teardown := true
	if os.Getenv("GO_TEST_NO_TEARDOWN") == "true" {
		use_teardown = false
	}

	ret := m.Run()
	if ret == 0 && use_teardown {
		teardown()
	}
	os.Exit(ret)
}

func TestOpen(t *testing.T) {
	idx, e := vfs.Open(DataDir, DefaultOption())

	assert.NotNil(t, idx)
	assert.NoError(t, e)

}

type DiscardString struct{}

func (io DiscardString) WriteString(s string) (int, error) {
	return len(s), nil
}

func TestRegist(t *testing.T) {

	vfs.LogWriter = DiscardString{}
	registDir := TestRoot + "/vfs-regist-test"
	os.RemoveAll(registDir)
	idx, e := vfs.Open(DataDir,
		vfs.RootDir(registDir), vfs.MergeDuration(10*time.Second))

	assert.NotNil(t, idx)
	assert.NoError(t, e)

	e = idx.Regist("test", "id")
	assert.NoError(t, e)
	os.RemoveAll(registDir)
}

func TestStringRegist(t *testing.T) {

	idx, e := vfs.Open(DataDir, DefaultOption(), vfs.MergeDuration(10*time.Second))

	assert.NotNil(t, idx)
	assert.NoError(t, e)

	e = idx.Regist("test", "title")
	assert.NoError(t, e)
}

func Test_SearchStringAll(t *testing.T) {
	vfs.CurrentLogLoevel = vfs.LOG_WARN

	idx, e := OpenIndexer()

	sCond := idx.On("test", vfs.ReaderColumn("title"), vfs.Output(vfs.MapInfOutput))

	matches := sCond.Select(func(cond vfs.SearchElem) bool {
		return cond.Op("title", "==", "警視庁")
	}).All()

	assert.NoError(t, e)
	assert.True(t, 0 < len(matches.([]interface{})))

	matches2 := sCond.Match("渡辺麻友").All().([]interface{})

	//result_id, ok := results[0]["id"].(uint64)
	//idx.Cols["name"].CancelAndWait()
	sCond.CancelAndWait()
	assert.NoError(t, e)
	assert.True(t, 0 < len(matches2))

}

func Test_SearchSmallString(t *testing.T) {
	vfs.CurrentLogLoevel = vfs.LOG_WARN
	idx, e := OpenIndexer()

	sCond := idx.On("test", vfs.ReaderColumn("title"), vfs.Output(vfs.MapInfOutput))
	matches := sCond.Match("鬼滅").All().([]interface{})

	assert.NoError(t, e)
	assert.True(t, 0 == len(matches))
	sCond.CancelAndWait()
}

func Test_SearchQueryt(t *testing.T) {
	vfs.CurrentLogLoevel = vfs.LOG_WARN
	idx, e := OpenIndexer()
	qstr := "id == 132763"
	q, _ := expr.GetExpr(qstr)

	sCond := idx.On("test", vfs.ReaderColumn(q.Column), vfs.Output(vfs.MapInfOutput))
	results := sCond.Query(qstr).All().([]interface{})

	expected := interface{}(uint64(132763))
	assert.NoError(t, e)
	assert.True(t, 0 < len(results))
	assert.Equal(t, expected, results[0].(map[string]interface{})["id"])
	sCond.CancelAndWait()
}

func Test_SearchQueryString(t *testing.T) {
	vfs.CurrentLogLoevel = vfs.LOG_WARN
	idx, e := OpenIndexer()
	qstr := `title.search("鬼滅の")`
	q, _ := expr.GetExpr(qstr)

	sCond := idx.On("test", vfs.ReaderColumn(q.Column), vfs.Output(vfs.MapInfOutput), vfs.UseBsearch(true))
	sfinder := sCond.Query(qstr)
	results := sfinder.All(vfs.OptQueryStat(true)).([]interface{})

	stats := sfinder.Stats()
	_ = stats

	sCond = idx.On("test", vfs.ReaderColumn(q.Column), vfs.Output(vfs.MapInfOutput), vfs.UseBsearch(false))
	sfinder = sCond.Query(qstr)
	results2 := sfinder.All(vfs.OptQueryStat(true)).([]interface{})

	stats2 := sfinder.Stats()
	_ = stats2

	assert.NoError(t, e)
	assert.True(t, 0 < len(results))
	assert.True(t, len(results) == len(results2))
	sCond.CancelAndWait()
}

func Test_SearcAndhQuery(t *testing.T) {
	vfs.CurrentLogLoevel = vfs.LOG_WARN
	idx, e := OpenIndexer()
	qstr := `title.search("鬼滅の") && id == 3365460`
	//qstr = `id == 3365460 && title.search("鬼滅の")`
	q, _ := expr.GetExpr(qstr)

	sCond := idx.On("test", vfs.ReaderColumn(q.Column), vfs.Output(vfs.MapInfOutput))
	results := sCond.Query(qstr).All().([]interface{})

	assert.NoError(t, e)
	assert.True(t, 0 < len(results))
	sCond.CancelAndWait()
}

func Test_SearcAndhQueryUseChan(t *testing.T) {
	vfs.CurrentLogLoevel = vfs.LOG_WARN
	idx, e := OpenIndexer()
	qstr := `title.search("鬼滅の") && id == 3365460`
	//qstr = `id == 3365460 && title.search("鬼滅の")`
	q, _ := expr.GetExpr(qstr)

	sCond := idx.On("test", vfs.ReaderColumn(q.Column), vfs.Output(vfs.MapInfOutput))
	s := time.Now()
	results := sCond.Query(qstr).All().([]interface{})
	dur := time.Now().Sub(s)
	_ = dur

	assert.NoError(t, e)
	assert.True(t, 0 < len(results))
	sCond.CancelAndWait()

	sCond = idx.On("test", vfs.ReaderColumn(q.Column), vfs.Output(vfs.MapInfOutput))
	s = time.Now()
	results = sCond.Query(qstr).All(vfs.OptQueryUseChan(true)).([]interface{})
	dur2 := time.Now().Sub(s)
	_ = dur2
	assert.NoError(t, e)
	assert.True(t, 0 < len(results))
	sCond.CancelAndWait()

	assert.Truef(t, dur > dur2, "dur=%s <= dur2=%s", dur, dur2)

}

func Test_SearcUseStream(t *testing.T) {
	vfs.CurrentLogLoevel = vfs.LOG_WARN
	idx, e := OpenIndexer()
	qstr := `title.search("鬼滅の") && id == 3365460`
	//qstr = `id == 3365460 && title.search("鬼滅の")`
	q, _ := expr.GetExpr(qstr)

	sCond := idx.On("test", vfs.ReaderColumn(q.Column), vfs.Output(vfs.JsonOutput))
	results := sCond.Query(qstr).All(vfs.OptQueryUseChan(true),
		vfs.ResultOutput("json"),
		vfs.ResultStreamt(true)).(chan interface{})

	var out []interface{}
	for result := range results {
		if result == nil {
			close(results)
			break
		}
		out = append(out, result)
	}

	assert.NoError(t, e)
	assert.True(t, 0 < len(out))
	sCond.CancelAndWait()

}

func TestSize(t *testing.T) {
	a := types.Config{}
	assert.NotNil(t, a)

}

func MapStore(m map[string]string, key, value string) {

	m[key] = value

}

func TestStoreMap(t *testing.T) {

	m := map[string]string{}

	MapStore(m, "hoge", "hoa")

	assert.Equal(t, "hoa", m["hoge"])

}

func TestUtf8Rune(t *testing.T) {

	str := "世界おはようohayou世界"

	a := []rune(str)

	assert.NotNil(t, a)
}

func TestEncodeTri(t *testing.T) {

	str := "おはよう俺様の世界へwellcome"
	vals := vfs.EncodeTri(str)
	runes := []rune(str)
	assert.Equal(t, len(runes)-2, len(vals))

}

func TestAddingDir(t *testing.T) {

	s := "308830533057"
	d := vfs.AddingDir(s, 4)

	assert.Equal(t, d, "3088/3053/3057/")

	s = "0007530e41"
	d = vfs.AddingDir(s, 4)
	assert.Equal(t, d, "0007/530e/41/")

}

func TestMerge(t *testing.T) {
	vfs.CurrentLogLoevel = vfs.LOG_WARN
	mergeDir := TestRoot + "/vfs-tmp"
	idx, e := vfs.Open(DataDir,
		vfs.RootDir(mergeDir), vfs.MergeDuration(1*time.Second))
	idx.Regist("test", "id")

	sCond := idx.On("test", vfs.ReaderColumn("id"), vfs.MergeOnSearch(true))
	sCond.StartMerging()
	time.Sleep(10 * time.Second)
	sCond.CancelAndWait()

	assert.Equal(t, 1, 1)
	assert.NoError(t, e)

	os.RemoveAll(mergeDir)
}

func TestBufWriterIO(t *testing.T) {

	f, _ := os.Create("hoge.txt")
	defer os.Remove("hoge.txt")

	b := vfs.NewBufWriterIO(f, 512)

	for i := 0; i < 8000; i++ {
		l := i % 10
		b.Write([]byte(fmt.Sprintf("%d", l)))
	}
	b.Flush()

	f.Close()

	f, _ = os.Open("hoge.txt")
	buf, _ := ioutil.ReadAll(f)

	assert.True(t, len(buf) > 1000)
	i := 475
	l := 475 % 10
	assert.Equal(t, fmt.Sprintf("%d", l)[0], buf[i])
}

func Test_WriteAt_BufWriterIO(t *testing.T) {

	f, _ := os.Create("hoge.txt")
	defer os.Remove("hoge.txt")

	b := vfs.NewBufWriterIO(f, 512)

	for i := 0; i < 8000; i++ {
		l := i % 10
		b.WriteAt([]byte(fmt.Sprintf("%d", l)), int64(i+10))
	}
	b.Flush()

	f.Close()

	f, _ = os.Open("hoge.txt")
	buf, _ := ioutil.ReadAll(f)

	assert.True(t, len(buf) > 1000)
	i := 475
	l := 475 % 10
	assert.Equal(t, fmt.Sprintf("%d", l)[0], buf[i])
}

func TestColumnPathWithStatus(t *testing.T) {

	OpenIndexer()
	s := vfs.ColumnPathWithStatus("test", "name", false, "308830533057", "308830533057", vfs.RECORD_WRITTEN)
	s2 := vfs.ColumnPathWithStatus("test", "name", false, "*", "*", vfs.RECORD_WRITTEN)

	assert.Equal(t, s, "testdata/vfs/test/3088/3053/3057/name.gram.idx.308830533057-308830533057")
	assert.Equal(t, s2, "testdata/vfs/test/*/*/*/name.gram.idx.*-*")

}

func Test_RecordListInsertSort(t *testing.T) {

	list := query.NewRecordList()
	list.Base = base.NewNoLayer(list.Base)

	for i := 0; i < 10; i++ {

		rec := query.NewRecord()
		rec.SetFileId(query.FromUint64(uint64(2)))
		rec.SetOffset(query.FromInt64(int64(i * 2)))
		list.SetAt(list.Count(), rec)
	}

	i := 9

	rec := query.NewRecord()
	rec.SetFileId(query.FromUint64(uint64(2)))
	rec.SetOffset(query.FromInt64(int64(i)))
	//list.SetAt(list.Count(), rec)

	idx := list.SearchIndex(func(r *query.Record) bool {
		if status, e := vfs.CompareRecord(r, rec); status > vfs.RecordEQ && e == nil {
			return true
		}
		return false
	})

	hl := vfs.HookRecordList{RecordList: list}
	hl.InsertWithKeepSort(rec, idx)

	for _, rec := range list.All() {
		fmt.Printf("rec.0x%x\n", rec.Offset().Int64())
	}

	want := list.AtWihoutError(idx)
	cmp, e := vfs.CompareRecord(want, rec)

	assert.Nil(t, e)
	assert.Equal(t, vfs.RecordEQ, cmp)

}
