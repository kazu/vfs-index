package vfsindex_test

import (
	"testing"

	vfs "github.com/kazu/vfs-index"
	"github.com/stretchr/testify/assert"
)

func DefaultOption() {
	return vfs.Option{
		RootDir: "/Users/xtakei/git/vfs-index/example/vfs",
	}
}

func TestOpen(t *testing.T) {
	idx, e := vfs.Open("/Users/xtakei/git/vfs-index/example/data", DefaultOption())
	assert.NoError(t, err)

}
