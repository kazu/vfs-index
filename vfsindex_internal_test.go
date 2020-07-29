package vfsindex

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/kazu/vfs-index/query"
	"github.com/stretchr/testify/assert"
)

func Test_IndexFIle(t *testing.T) {

	CurrentLogLoevel = LOG_WARN
	idx, e := Open("/Users/xtakei/git/vfs-index/example/data",
		RootDir("/Users/xtakei/git/vfs-index/example/vfs-tmp"))

	sCond := idx.On("test", ReaderColumn("name"), MergeOnSearch(false))

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

	f := NewIndexFile(c, "/Users/xtakei/git/vfs-index/example/vfs-tmp/test/name.gram.idx.merged.00200020002e-00490020004c")
	f.Init()

	assert.True(t, f.IsType(IdxFileType_Merge))
	assert.Equal(t, uint64(0x00200020002e), f.IdxInfo().first)

	f = NewIndexFile(c, "/Users/xtakei/git/vfs-index/example/vfs-tmp/test/0000/bfca/7e/id.num.idx.0000bfca7e-0000bfca7e.0001483950.0000000f02")
	f.Init()

	assert.Equal(t, IdxFileType_None, f.Ftype)
	f = NewIndexFile(c, "/Users/xtakei/git/vfs-index/example/vfs-tmp/test/ff61/30d9/30c3/name.gram.idx.ff6130d930c3-ff6130d930c3.00014829a5.000003f5b1")
	f.Init()
	a := f.KeyRecord().Key().Int64()
	_ = a
	assert.True(t, f.IsType(IdxFileType_Write))

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
	assert.NotNil(t, uint64(0xbfca7e), f[0].IdxInfo().first)

	f = finder.FindByKey(0xbfca7c)

	assert.NotNil(t, uint64(0xbfca7c), f[0].IdxInfo().first)
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
	assert.True(t, len(strs) >= 2)
	strs = strings.Split(filepath.Base(a), "id.num.idx")

	assert.True(t, len(strs) < 2)
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

func Test_IndexFile_Select(t *testing.T) {

	CurrentLogLoevel = LOG_WARN
	idx, e := Open("/Users/xtakei/git/vfs-index/example/data",
		RootDir("/Users/xtakei/git/vfs-index/example/vfs-tmp"))

	sCond := idx.On("test", ReaderColumn("id"), MergeOnSearch(true))

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

	assert.Error(t, e)
	assert.True(t, len(matches) == 100)

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
