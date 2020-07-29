package vfsindex_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"go/types"

	vfs "github.com/kazu/vfs-index"
	"github.com/kazu/vfs-index/expr"
	"github.com/stretchr/testify/assert"
)

func DefaultOption() vfs.Option {
	// return vfs.Option{
	// 	RootDir: "/Users/xtakei/git/vfs-index/example/vfs",
	// }
	return vfs.RootDir("/Users/xtakei/git/vfs-index/example/vfs")
}

func OpenIndexer() (*vfs.Indexer, error) {
	return vfs.Open("/Users/xtakei/git/vfs-index/example/data", DefaultOption(), vfs.MergeDuration(10*time.Second))

}

func setup() {
	vfs.CurrentLogLoevel = vfs.LOG_WARN

}

func teardown() {

}

func TestMain(m *testing.M) {
	setup()
	ret := m.Run()
	if ret == 0 {
		teardown()
	}
	os.Exit(ret)
}

func TestOpen(t *testing.T) {
	idx, e := vfs.Open("/Users/xtakei/git/vfs-index/example/data", DefaultOption())

	assert.NotNil(t, idx)
	assert.NoError(t, e)

}

type DiscardString struct{}

func (io DiscardString) WriteString(s string) (int, error) {
	return len(s), nil
}

func TestRegist(t *testing.T) {

	vfs.LogWriter = DiscardString{}

	os.RemoveAll("/Users/xtakei/git/vfs-index/example/vfs-regist-test")
	idx, e := vfs.Open("/Users/xtakei/git/vfs-index/example/data",
		vfs.RootDir("/Users/xtakei/git/vfs-index/example/vfs-regist-test"), vfs.MergeDuration(10*time.Second))

	assert.NotNil(t, idx)
	assert.NoError(t, e)

	e = idx.Regist("test", "id")
	assert.NoError(t, e)
}

func TestStringRegist(t *testing.T) {
	//vfs.CurrentLogLoevel = vfs.LOG_DEBUG
	//vfs.LogWriter = DiscardString{}

	idx, e := vfs.Open("/Users/xtakei/git/vfs-index/example/data", DefaultOption(), vfs.MergeDuration(10*time.Second))

	assert.NotNil(t, idx)
	assert.NoError(t, e)

	e = idx.Regist("test", "name")
	assert.NoError(t, e)
}

// func Test_SearcherFindAll(t *testing.T) {

// 	//vfs.LogWriter = DiscardString{}

// 	idx, e := OpenIndexer()

// 	sCond := idx.On("test", vfs.ReaderColumn("id"), vfs.Output(vfs.MapInfOutput))

// 	records := sCond.Searcher().Select(func(m vfs.Match) bool {
// 		v := m.Get("id").(uint64)
// 		return v > 4568788719
// 	}).All()

// 	results := sCond.ToMapInfs(records)

// 	result_id, ok := results[0]["id"].(uint64)

// 	assert.NoError(t, e)
// 	assert.True(t, ok)
// 	assert.True(t, 1164 <= len(results))
// 	assert.Equal(t, result_id > 4568788719, true, "must bigger 4568788719")

// 	records = sCond.Searcher().Select(func(m vfs.Match) bool {
// 		v := m.Get("id").(uint64)
// 		return v < 122878513
// 	}).All()
// 	results = sCond.ToMapInfs(records)

// 	result_id, ok = results[0]["id"].(uint64)
// 	assert.NoError(t, e)
// 	assert.True(t, ok)
// 	assert.True(t, 3 <= len(results))
// 	assert.Equal(t, result_id < 122878513, true, "must smaller 122878513")
// 	sCond.CancelAndWait()

// }

func Test_SearchStringAll(t *testing.T) {
	vfs.CurrentLogLoevel = vfs.LOG_WARN

	idx, e := OpenIndexer()

	sCond := idx.On("test", vfs.ReaderColumn("name"), vfs.Output(vfs.MapInfOutput))

	matches := sCond.Select(func(cond vfs.SearchCondElem) bool {
		return cond.Op("name", "==", "無門会")
	}).All()

	assert.NoError(t, e)
	assert.True(t, 0 < len(matches))

	matches2 := sCond.Match("ロシア人").All()

	//result_id, ok := results[0]["id"].(uint64)
	//idx.Cols["name"].CancelAndWait()
	sCond.CancelAndWait()
	fmt.Printf("cnt=%d  %v\n", len(matches2), matches2)
	assert.NoError(t, e)
	assert.True(t, 0 < len(matches2))

}

func Test_SearchSmallString(t *testing.T) {
	vfs.CurrentLogLoevel = vfs.LOG_WARN
	idx, e := OpenIndexer()

	sCond := idx.On("test", vfs.ReaderColumn("name"), vfs.Output(vfs.MapInfOutput))
	matches := sCond.Match("無門").All()

	assert.NoError(t, e)
	//assert.True(t, 0 < len(matches))
	assert.True(t, 0 == len(matches))
	sCond.CancelAndWait()
}

func Test_SearchQueryt(t *testing.T) {
	vfs.CurrentLogLoevel = vfs.LOG_WARN
	idx, e := OpenIndexer()
	qstr := "id == 130988471"
	q, _ := expr.GetExpr(qstr)

	sCond := idx.On("test", vfs.ReaderColumn(q.Column), vfs.Output(vfs.MapInfOutput))
	results := sCond.Query(qstr).All()

	expected := interface{}(uint64(130988471))
	assert.NoError(t, e)
	assert.True(t, 0 < len(results))
	assert.Equal(t, expected, results[0].(map[string]interface{})["id"])
	sCond.CancelAndWait()
}

func Test_SearchQueryString(t *testing.T) {
	vfs.CurrentLogLoevel = vfs.LOG_WARN
	idx, e := OpenIndexer()
	qstr := `name.search("ロシア")`
	q, _ := expr.GetExpr(qstr)

	sCond := idx.On("test", vfs.ReaderColumn(q.Column), vfs.Output(vfs.MapInfOutput))
	results := sCond.Query(qstr).All()

	//expected := interface{}(uint64(130988471))
	assert.NoError(t, e)
	assert.True(t, 0 < len(results))
	//assert.Equal(t, expected, results[0]["id"])
	sCond.CancelAndWait()
}

func TestSize(t *testing.T) {
	//	assert.Equal(t, len(int64), 8)
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

	fmt.Printf("a=%c 0x%4x %#U size=%d %v\n", a[0], a[0], a[0], len(string(a[0:3])), a[0:3])
	assert.NotNil(t, a)
}

func TestEncodeTri(t *testing.T) {

	str := "おはよう俺様の世界へwellcome"
	vals := vfs.EncodeTri(str)
	runes := []rune(str)
	assert.Equal(t, len(runes)-2, len(vals))

	var a int64
	a = -1

	b := fmt.Sprintf("%x\n", uint64(a))

	l := len(b)
	fmt.Println(l)

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
	idx, e := vfs.Open("/Users/xtakei/git/vfs-index/example/data",
		vfs.RootDir("/Users/xtakei/git/vfs-index/example/vfs-tmp"), vfs.MergeDuration(10*time.Second))

	sCond := idx.On("test", vfs.ReaderColumn("id"), vfs.MergeOnSearch(true))
	sCond.StartMerging()
	time.Sleep(10 * time.Second)
	sCond.CancelAndWait()
	assert.Equal(t, 1, 1)
	assert.NoError(t, e)
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
		//b.Write([]byte(fmt.Sprintf("%d", l)))
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

	assert.Equal(t, s, "/Users/xtakei/git/vfs-index/example/vfs/test/3088/3053/3057/name.gram.idx.308830533057-308830533057")
	assert.Equal(t, s2, "/Users/xtakei/git/vfs-index/example/vfs/test/*/*/*/name.gram.idx.*-*")

}
