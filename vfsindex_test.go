package vfsindex_test

import (
	"testing"

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

func Test_SearcherFirst(t *testing.T) {

	idx, e := OpenIndexer()

	result := idx.On("test", vfs.ReaderOpt{"column": "id"}).Searcher().First(func(m vfs.Match) bool {
		v := m.Get("id").(uint64)
		return v < 122878513
	})

	result_id, ok := result["id"].(uint64)

	assert.NoError(t, e)
	assert.True(t, ok)
	assert.Equal(t, result_id < 122878513, true, "must smaller 122878513")

}

func Test_SearcherFindAll(t *testing.T) {
	idx, e := OpenIndexer()

	results := idx.On("test", vfs.ReaderOpt{"column": "id"}).Searcher().FindAll(func(m vfs.Match) bool {
		v := m.Get("id").(uint64)
		return v > 4568788719
	})

	result_id, ok := results[0]["id"].(uint64)

	assert.NoError(t, e)
	assert.True(t, ok)
	assert.Equal(t, 1164, len(results))
	assert.Equal(t, result_id > 4568788719, true, "must bigger 4568788719")

	results = idx.On("test", vfs.ReaderOpt{"column": "id"}).Searcher().FindAll(func(m vfs.Match) bool {
		v := m.Get("id").(uint64)
		return v < 122878513
	})

	result_id, ok = results[0]["id"].(uint64)
	assert.NoError(t, e)
	assert.True(t, ok)
	assert.Equal(t, 3, len(results))
	assert.Equal(t, result_id < 122878513, true, "must smaller 122878513")

}
