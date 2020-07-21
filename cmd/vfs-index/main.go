package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/davecgh/go-spew/spew"
	vfs "github.com/kazu/vfs-index"
)

const Usage string = `
vfs-index ... indexer/search in vfs data(json,csv)
Usage: vfs-indx <subcmd> <Flags>
vfs-index
  subcmd 
	index		index data
	search		search data
	merge       merge index (only 1 minute)

  Flags:
	-index  	directory for index data
	-h     		help for vfs-index
	-column		column name for index  
	-table		table name for index. prefix name in data file
	-data		data directory
	-q			search query

`

func main() {

	if len(os.Args) == 1 {
		fmt.Println(Usage)
		return
	}

	var flagCmd *flag.FlagSet

	switch os.Args[1] {
	case "index":
		flagCmd = flag.NewFlagSet("index", flag.ExitOnError)

	case "search":
		flagCmd = flag.NewFlagSet("search", flag.ExitOnError)
	case "merge":
		flagCmd = flag.NewFlagSet("merge", flag.ExitOnError)
		//search(query, indexDir, column, table, dir, first)
		//flag.Parse(os.Args[:2])
	}
	var indexDir, column, table, dir, query string
	var first, help bool
	flagCmd.StringVar(&indexDir, "index", "./vfs", "directory of index")
	flagCmd.StringVar(&column, "column", "id", "column name")
	flagCmd.StringVar(&table, "table", "table", "table name")
	flagCmd.StringVar(&dir, "data", "./", "datadir")
	flagCmd.StringVar(&query, "q", "", "search query")
	flagCmd.BoolVar(&first, "f", false, "only output one record")
	flagCmd.BoolVar(&help, "h", false, "help")

	flagCmd.Parse(os.Args[2:])

	//fmt.Printf("%v %v %v %v\n", indexDir, column, table, dir)
	//return
	if help {
		fmt.Println(Usage)
		return
	}

	switch flagCmd.Name() {
	case "index":
		indexing(indexDir, column, table, dir)
	case "search":
		search(query, indexDir, column, table, dir, first)
	case "merge":
		merge(indexDir, column, table, dir)
	}

	return
}

func indexing(indexDir, column, table, dir string) {
	cur, _ := os.Getwd()
	opt := vfs.Option{
		RootDir: filepath.Join(cur, indexDir),
	}
	idx, e := vfs.Open(filepath.Join(cur, dir), opt)

	if e != nil {
		fmt.Printf("E: Open(%s) fail errpr=%s\n", dir, e)
	}
	e = idx.Regist(table, column)

}

func merge(indexDir, column, table, dir string) {

	//vfs.CurrentLogLoevel = vfs.LOG_ERROR

	cur, _ := os.Getwd()
	opt := vfs.Option{
		RootDir: filepath.Join(cur, indexDir),
	}
	idx, e := vfs.Open(filepath.Join(cur, dir), opt)

	if e != nil {
		fmt.Printf("E: Open(%s) fail errpr=%s\n", dir, e)
	}
	sCond := idx.On(table, vfs.ReaderOpt{"column": column})
	sCond.Searcher()
	time.Sleep(1 * time.Minute)
	sCond.CancelAndWait()

}

func search(query, indexDir, column, table, dir string, first bool) {

	vfs.CurrentLogLoevel = vfs.LOG_ERROR
	cur, _ := os.Getwd()

	opt := vfs.Option{
		RootDir: filepath.Join(cur, indexDir),
	}
	idx, e := vfs.Open(filepath.Join(cur, dir), opt)
	if e != nil {
		fmt.Printf("E: Open(%s) fail errpr=%s\n", dir, e)
	}

	ival, e := strconv.ParseUint(query, 10, 64)

	var info *vfs.SearchInfo

	sCond := idx.On(table, vfs.ReaderOpt{"column": column})
	if e == nil {
		info = sCond.Searcher().Select(func(m vfs.Match) bool {
			v := m.Get(column).(uint64)
			return v <= ival
		}).Select(func(m vfs.Match) bool {
			v := m.Get(column).(uint64)
			return v >= ival
		})
	} else {
		info = sCond.Searcher().Match(query)
	}

	if first {
		result := info.First()
		fmt.Printf("%v\n", spew.Sdump(result))
	} else {
		results := info.All()
		for _, result := range results {
			fmt.Printf("%s\n", spew.Sdump(result))
		}
	}
	sCond.CancelAndWait()
}
