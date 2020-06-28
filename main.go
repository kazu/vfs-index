package main

import (
	"fmt"
	"image/png"
	"io/ioutil"
	"log"
	"os"
	"reflect"

	"encoding/json"

	"github.com/Nr90/imgsim"
	"github.com/davecgh/go-spew/spew"
	"github.com/kazu/vfs-index/indexer"
	"github.com/ugorji/go/codec"

	"github.com/gabriel-vasile/mimetype"
	"github.com/h2non/filetype"
)

func old_main() {

	//imgfile, err := os.Open("2020-03-16-13.29.35.png")
	ahash, dhash, err := path2Hash("/Users/xtakei/Desktop/2020-03-16-13.29.35.png")
	_ = err
	fmt.Printf("ahash=%v dhash=%v\n", ahash, dhash)
	ahash, dhash, err = path2Hash("/Users/xtakei/Desktop/2020-03-16-13.29.41.png")
	fmt.Printf("ahash=%v dhash=%v\n", ahash, dhash)
}

const EMPTY_HASH imgsim.Hash = imgsim.Hash(0)

func path2Hash(p string) (imgsim.Hash, imgsim.Hash, error) {
	imgfile, err := os.Open(p)
	defer imgfile.Close()
	if err != nil {
		return EMPTY_HASH, EMPTY_HASH, err
	}
	img, err := png.Decode(imgfile)
	if err != nil {
		return EMPTY_HASH, EMPTY_HASH, err
	}
	ahash := imgsim.AverageHash(img)
	dhash := imgsim.DifferenceHash(img)
	return ahash, dhash, nil
}
func main() {
	//new_main()
	main_search()
	//json_main()
	//file_type()
}

func new_main() {

	opt := indexer.Option{
		RootDir: "/Users/xtakei/git/vfs-index/example/vfs",
	}

	idx, e := indexer.Open("/Users/xtakei/git/vfs-index/example/data", opt)
	if e != nil {
		return
	}

	idx.Regist("test", "id")
	//spew.Dump(idx)
	/*
		fmt.Println(filepath.Base("/hogehoge/hoge.ext"))

		fmt.Println(filepath.Dir("/hogehoge/hoge.ext"))
		fmt.Println(filepath.Dir("hogehoge"))
	*/
}
func main_search() {

	opt := indexer.Option{
		RootDir: "/Users/xtakei/git/vfs-index/example/vfs",
	}

	idx, e := indexer.Open("/Users/xtakei/git/vfs-index/example/data", opt)
	if e != nil {
		return
	}

	z := idx.On("test", indexer.ReaderOpt{"column": "id"}).Searcher().First(func(m indexer.Match) bool {
		v := m.Get("id").(uint64)
		return v < 122878513
	})
	spew.Dump(z)
}

/*
type Record struct {
	Id int
	Title string
	Country string
	Created time.Time
}

func sample() {


	ch := vfs_indexer.Open("./data/scopus/").Filter(func(d *Record) bool {
		return d.Counter == "JP"
	}).Limit(10)

}*/

var (
	jh codec.JsonHandle
)

func main_codec() {

	//	b := []byte{}
	jh.MapType = reflect.TypeOf(map[string]interface{}(nil))
	//jh.DecodeOptions.
	var data interface{}
	//jh.SliceType = reflect.TypeOf([]string(nil))

	f, e := os.Open("example/data/test2.json")
	spew.Dump(e)

	dec := codec.NewDecoder(f, &jh)
	err := dec.Decode(&data)
	spew.Dump(err)
	f.Close()

	spew.Dump(data)
}

func GetFirstObject() {
	d, _ := ioutil.ReadFile("example/data/test.json")

	fmt.Printf("%s\n", d[1:104])

}

func json_main() {
	//GetFirstObject()
	//return

	f, e := os.Open("example/data/test.json")
	defer f.Close()

	var data map[string]interface{}
	_ = data
	if e != nil {
		return
	}
	dec := json.NewDecoder(f)
	t, _ := dec.Token()

	fmt.Printf("%T: %v\n\n!!!!\n", t, t)

	fmt.Printf("offset=%d\n", dec.InputOffset())
	for dec.More() {
		// decode an array value (Message)
		fmt.Printf("--B%d--\n", dec.InputOffset())

		err := dec.Decode(&data)
		if err != nil {
			log.Fatal(err)
		}

		spew.Dump(data)
		fmt.Printf("--A%d--\n", dec.InputOffset())
	}

}

func file_type() {

	buf, _ := ioutil.ReadFile("example/data/test.json")
	kind, _ := filetype.Match(buf)
	spew.Dump(kind)

	//data, _ := ioutil.ReadFile("example/data/test.json")
	//mime := mimetype.Detect(data)
	mime, _ := mimetype.DetectFile("example/vfs/test/id.num.idx.122878512-122878512.21506469.00219162")
	fmt.Println(mime.String(), mime.Extension())

}
