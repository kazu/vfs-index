package vfsindex_test

import (
	"fmt"
	"os"
	"testing"

	"go/types"

	vfs "github.com/kazu/vfs-index"
	"github.com/stretchr/testify/assert"
)

func DefaultOption() vfs.Option {
	return vfs.Option{
		RootDir: "/Users/xtakei/git/vfs-index/example/vfs",
	}
}

func OpenIndexer() (*vfs.Indexer, error) {
	return vfs.Open("/Users/xtakei/git/vfs-index/example/data", DefaultOption())

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

func TestRegist(t *testing.T) {

	idx, e := vfs.Open("/Users/xtakei/git/vfs-index/example/data", DefaultOption())

	assert.NotNil(t, idx)
	assert.NoError(t, e)

	e = idx.Regist("test", "id")
	assert.NoError(t, e)
}

func TestStringRegist(t *testing.T) {

	idx, e := vfs.Open("/Users/xtakei/git/vfs-index/example/data", DefaultOption())

	assert.NotNil(t, idx)
	assert.NoError(t, e)

	e = idx.Regist("test", "name")
	assert.NoError(t, e)
}

func Test_SearcherFirst(t *testing.T) {

	idx, e := OpenIndexer()

	result := idx.On("test", vfs.ReaderOpt{"column": "id"}).Searcher().Select(func(m vfs.Match) bool {
		v := m.Get("id").(uint64)
		return v < 122878513
	}).First()

	result_id, ok := result["id"].(uint64)

	assert.NoError(t, e)
	assert.True(t, ok)
	assert.Equal(t, result_id < 122878513, true, "must smaller 122878513")

}

func Test_SearcherFindAll(t *testing.T) {
	idx, e := OpenIndexer()

	results := idx.On("test", vfs.ReaderOpt{"column": "id"}).Searcher().Select(func(m vfs.Match) bool {
		v := m.Get("id").(uint64)
		return v > 4568788719
	}).All()

	result_id, ok := results[0]["id"].(uint64)

	assert.NoError(t, e)
	assert.True(t, ok)
	assert.Equal(t, 1164, len(results))
	assert.Equal(t, result_id > 4568788719, true, "must bigger 4568788719")

	results = idx.On("test", vfs.ReaderOpt{"column": "id"}).Searcher().Select(func(m vfs.Match) bool {
		v := m.Get("id").(uint64)
		return v < 122878513
	}).All()

	result_id, ok = results[0]["id"].(uint64)
	assert.NoError(t, e)
	assert.True(t, ok)
	assert.Equal(t, 3, len(results))
	assert.Equal(t, result_id < 122878513, true, "must smaller 122878513")

}

func Test_SearchStringAll(t *testing.T) {
	vfs.CurrentLogLoevel = vfs.LOG_WARN
	idx, e := OpenIndexer()
	sval := vfs.SearchVal("逆突き")

	info := idx.On("test", vfs.ReaderOpt{"column": "name"}).Searcher().Select(func(m vfs.Match) bool {
		return m.Uint64("name") <= sval
	}).Select(func(m vfs.Match) bool {
		return m.Uint64("name") >= sval
	})

	matches := info.All()
	assert.NoError(t, e)
	assert.True(t, 0 < len(matches))

	matches = idx.On("test", vfs.ReaderOpt{"column": "name"}).Searcher().Match("ロシア人").All()

	//result_id, ok := results[0]["id"].(uint64)
	fmt.Printf("cnt=%d  %v\n", len(matches), matches)
	assert.True(t, 0 < len(matches))
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
