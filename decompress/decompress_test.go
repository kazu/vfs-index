package decompress_test

import (
	"bytes"
	"fmt"
	"testing"

	"os"

	"github.com/kazu/vfs-index/decompress"
	"github.com/pierrec/lz4"
	"github.com/stretchr/testify/assert"
)

func setup() {

	f, _ := os.Create("test.lz4")
	defer f.Close()

	b := bytes.NewBuffer(nil)

	fmt.Fprint(b, "12345678")
	w := lz4.NewWriter(f)
	w.Write(b.Bytes())
	w.Close()
	return
}

func teardown() {
	os.Remove("test.lz4")
}

func Test_Lz4_ReadAt(t *testing.T) {
	setup()
	defer teardown()

	dec := decompress.NewLz4("test.lz4")
	b := []byte("87654321")
	n, e := dec.ReadAt(b[3:], 3)

	assert.NoError(t, e)
	assert.Equal(t, 5, n)
	assert.Equal(t, "45678", string(b[3:]))
}

func Test_GetFormat(t *testing.T) {

	ext, tt := decompress.GetFormat("hogehoge.csv.lz4")
	assert.Equal(t, ".csv", ext)
	assert.Equal(t, decompress.Lz4Type, tt)

	ext, tt = decompress.GetFormat("hogehoge.json.lz4")
	assert.Equal(t, ".json", ext)
	assert.Equal(t, decompress.Lz4Type, tt)

	ext, tt = decompress.GetFormat("hogehoge.csv")
	assert.Equal(t, ".csv", ext)
	assert.Equal(t, decompress.NoDecompressType, tt)

	ext, tt = decompress.GetFormat("hogehoge.json")
	assert.Equal(t, ".json", ext)
	assert.Equal(t, decompress.NoDecompressType, tt)
}
