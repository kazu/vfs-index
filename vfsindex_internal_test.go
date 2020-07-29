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

func Test_IndexFile_parentWith(t *testing.T) {

	CurrentLogLoevel = LOG_DEBUG
	idx, _ := Open("/Users/xtakei/git/vfs-index/example/data",
		RootDir("/Users/xtakei/git/vfs-index/example/vfs-tmp"))

	sCond := idx.On("test", ReaderColumn("name"), MergeOnSearch(false))

	c := sCond.Column()

	f1 := NewIndexFile(c, "example/vfs-tmp/test/9060/304f/308d/name.gram.idx.9060304f308d-9060304f308d.00014829a5.00000346db")
	f1.Init()

	f2 := NewIndexFile(c, "example/vfs-tmp/test/9060/0029/0020/name.gram.idx.906000290020-906000290020.00014829a5.0000003bfa")
	f2.Init()

	f3 := f1.parentWith(f2)

	assert.Equal(t, f3.Path, "example/vfs-tmp/test/9060")

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
			"example/vfs-tmp/test/0045/0058/0020/name.gram.idx.004500580020-004500580020.00014829a5.0000042963",
			"example/vfs-tmp/test/ff5e/9ed2/30ae/name.gram.idx.ff5e9ed230ae-ff5e9ed230ae.00014829a5.0000035c6f",
		},
		{
			"example/vfs-tmp/test/0049/0043/0020/name.gram.idx.004900430020-004900430020.00014829a5.000001107a",
			"example/vfs-tmp/test/004c/7248/005d/name.gram.idx.004c7248005d-004c7248005d.00014829a5.0000042633",
		},
		{
			"example/vfs-tmp/test/0049/0043/0020/name.gram.idx.004900430020-004900430020.00014829a5.000000fd15",
			"example/vfs-tmp/test/0058/30c1/30e3/name.gram.idx.005830c130e3-005830c130e3.00014829a5.000003b8e3",
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
