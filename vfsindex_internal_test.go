package vfsindex

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_IndexFIle(t *testing.T) {

	CurrentLogLoevel = LOG_WARN
	idx, e := Open("/Users/xtakei/git/vfs-index/example/data",
		RootDir("/Users/xtakei/git/vfs-index/example/vfs-tmp"))

	sCond := idx.On("test", ReaderColumn("id"), MergeOnSearch(true))

	c := sCond.Column()
	finder := OpenIndexFile(c)
	keyrecords := finder.First().KeyRecords()
	kr, e := keyrecords.First()

	assert.NoError(t, e)
	assert.Equal(t, IdxFileType_Dir, finder.Ftype)
	assert.True(t, keyrecords.Count() > 0)
	assert.Equal(t, uint64(kr.Key().Int64()), finder.First().IdxInfo().first)

	kr2 := finder.Last().KeyRecord()
	assert.Equal(t, uint64(kr2.Key().Uint64()), finder.Last().IdxInfo().last)

	f := NewIndexFile(c, "/Users/xtakei/git/vfs-index/example/vfs-tmp/test/id.num.idx.merged.0000bfca7c-01127d061c")
	f.Init()

	assert.True(t, f.IsType(IdxFileType_Merge))
	assert.Equal(t, uint64(0x0000bfca7c), f.IdxInfo().first)

	f = NewIndexFile(c, "/Users/xtakei/git/vfs-index/example/vfs-tmp/test/0000/bfca/7e/id.num.idx.0000bfca7e-0000bfca7e.0001483950.0000000f02")
	f.Init()
	assert.True(t, f.IsType(IdxFileType_Write))
	f = NewIndexFile(c, "/Users/xtakei/git/vfs-index/example/vfs-tmp/test/30d8/30d6/30f3/name.gram.idx.30d830d630f3-30d830d630f3.00014829a5.0000032116")
	f.Init()
	assert.Equal(t, IdxFileType_None, f.Ftype)

}
func Test_FindByKey_IndexFIle(t *testing.T) {

	CurrentLogLoevel = LOG_WARN
	idx, e := Open("/Users/xtakei/git/vfs-index/example/data",
		RootDir("/Users/xtakei/git/vfs-index/example/vfs-tmp"))

	sCond := idx.On("test", ReaderColumn("id"), MergeOnSearch(true))

	c := sCond.Column()
	finder := OpenIndexFile(c)

	f := finder.FindByKey(0xbfca7e)

	assert.NoError(t, e)
	assert.NotNil(t, uint64(0xbfca7e), f.IdxInfo().first)

	f = finder.FindByKey(0xbfca7c)

	assert.NotNil(t, uint64(0xbfca7c), f.IdxInfo().first)
}

func Test_SearchCond_First(t *testing.T) {
	CurrentLogLoevel = LOG_WARN
	idx, e := Open("/Users/xtakei/git/vfs-index/example/data",
		RootDir("/Users/xtakei/git/vfs-index/example/vfs-tmp"))

	sCond := idx.On("test", ReaderColumn("id"), MergeOnSearch(false))

	str := sCond.FindBy("id", uint64(0xbfca7e)).First(ResultOutput("json")).(string)

	assert.NoError(t, e)
	assert.True(t, len(str) > 0)
}

func Test_SearchCond_Select(t *testing.T) {
	CurrentLogLoevel = LOG_WARN
	idx, e := Open("/Users/xtakei/git/vfs-index/example/data",
		RootDir("/Users/xtakei/git/vfs-index/example/vfs-tmp"))

	sCond := idx.On("test", ReaderColumn("id"), MergeOnSearch(false))

	//str := sCond.FindBy("id", uint64(0xbfca7e)).First(ResultOutput("json")).(string)
	str := sCond.Select(func(cond SearchCondElem) bool {
		return cond.Op("id", "==", uint64(0xbfca7e))
	}).First(ResultOutput("json")).(string)
	assert.NoError(t, e)
	assert.True(t, len(str) > 0)
}

func Test_SearchCond_SelectGram(t *testing.T) {
	CurrentLogLoevel = LOG_WARN
	idx, e := Open("/Users/xtakei/git/vfs-index/example/data",
		RootDir("/Users/xtakei/git/vfs-index/example/vfs-tmp"))

	sCond := idx.On("test", ReaderColumn("name"), MergeOnSearch(false))

	//str := sCond.FindBy("id", uint64(0xbfca7e)).First(ResultOutput("json")).(string)
	str := sCond.Select(func(cond SearchCondElem) bool {
		return cond.Op("name", "==", "無門会")
	}).First(ResultOutput("json")).(string)
	assert.NoError(t, e)
	assert.True(t, len(str) > 0)
}

func Test_SearchCond_FirstGram(t *testing.T) {
	CurrentLogLoevel = LOG_WARN
	idx, e := Open("/Users/xtakei/git/vfs-index/example/data",
		RootDir("/Users/xtakei/git/vfs-index/example/vfs-tmp"))

	sCond := idx.On("test", ReaderColumn("name"), MergeOnSearch(false))

	str := sCond.FindBy("name", "無門会").First(ResultOutput("json")).(string)

	assert.NoError(t, e)
	assert.True(t, len(str) > 0)
}

func Test_IndexFile_Init(t *testing.T) {

	a := "/Users/xtakei/git/vfs-index/example/vfs-tmp/30ab/30b5/0029/name.gram.idx.30ab30b50029-30ab30b50029.00014829a5.0000040eea"
	strs := strings.Split(filepath.Base(a), "name.gram.idx")

	assert.NotNil(t, a)
	assert.NotNil(t, strs)

}

func Test_SearchCondQuery_FirstGram(t *testing.T) {
	CurrentLogLoevel = LOG_WARN
	idx, e := Open("/Users/xtakei/git/vfs-index/example/data",
		RootDir("/Users/xtakei/git/vfs-index/example/vfs-tmp"))

	sCond := idx.On("test", ReaderColumn("name"), MergeOnSearch(false))

	str := sCond.Query(`name == "無門会"`).First(ResultOutput("json")).(string)

	assert.NoError(t, e)
	assert.True(t, len(str) > 0)
}
