package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	vfs "github.com/kazu/vfs-index"
	"github.com/kazu/vfs-index/expr"
)

const Usage string = `
vfs-index ... indexer/search in vfs data(json,csv)
Usage: vfs-indx <subcmd> <Flags>
vfs-index
  subcmd 
	index		index data
	search		search data
	merge		merge index (only 1 minute)

  Flags:
	-index  	directory for index data
	-h     		help for vfs-index
	-column		column name for index  
	-table		table name for index. prefix name in data file
	-data		data directory
	-qstdin		string search via STDIN, if qstdin is set, ignore -q flag
	-q		search query 
				example)  
					string search: 		'name.search("foobar")'
					numeric condition:	'id == 23456'
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
	var first, help, nomerge, qstdin bool
	flagCmd.StringVar(&indexDir, "index", "./vfs", "directory of index")
	flagCmd.StringVar(&column, "column", "id", "column name")
	flagCmd.StringVar(&table, "table", "table", "table name")
	flagCmd.StringVar(&dir, "data", "./", "datadir")
	flagCmd.StringVar(&query, "q", "", "search query")
	flagCmd.BoolVar(&qstdin, "qstdin", false, "string search by stdin")

	flagCmd.BoolVar(&first, "f", false, "only output one record")
	flagCmd.BoolVar(&help, "h", false, "help")
	flagCmd.BoolVar(&nomerge, "nomerge", false, "not merge index on search")

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
		if qstdin {
			line, _, _ := bufio.NewReader(os.Stdin).ReadLine()
			query = fmt.Sprintf("%s.search(\"%s\")", column, line)
		}

		search(query, indexDir, column, table, dir, first, nomerge)
	case "merge":
		merge(indexDir, column, table, dir)
	}

	return
}

func indexing(indexDir, column, table, dir string) {
	vfs.CurrentLogLoevel = vfs.LOG_WARN
	//vfs.CurrentLogLoevel = vfs.LOG_DEBUG

	cur, _ := os.Getwd()
	opt := vfs.RootDir(filepath.Join(cur, indexDir))
	idx, e := vfs.Open(filepath.Join(cur, dir), opt, vfs.RegitConcurrent(8))

	if e != nil {
		fmt.Printf("E: Open(%s) fail errpr=%s\n", dir, e)
	}
	e = idx.Regist(table, column)

}

func merge(indexDir, column, table, dir string) {
	vfs.CurrentLogLoevel = vfs.LOG_WARN
	//vfs.CurrentLogLoevel = vfs.LOG_DEBUG

	cur, _ := os.Getwd()
	opt := vfs.RootDir(filepath.Join(cur, indexDir))
	idx, e := vfs.Open(filepath.Join(cur, dir), opt)

	if e != nil {
		fmt.Printf("E: Open(%s) fail errpr=%s\n", dir, e)
	}
	sCond := idx.On(table, vfs.ReaderColumn(column),
		//vfs.MergeDuration(1*time.Second),
		vfs.MergeOnSearch(true))

	sCond.StartMerging()
	time.Sleep(1 * time.Minute)
	sCond.CancelAndWait()

}

func search(query, indexDir, column, table, dir string, first, nomerge bool) {
	vfs.CurrentLogLoevel = vfs.LOG_WARN

	vfs.CurrentLogLoevel = vfs.LOG_ERROR
	cur, _ := os.Getwd()

	q, _ := expr.GetExpr(query)
	if len(q.Column) > 0 {
		column = q.Column
	}

	idx, e := vfs.Open(filepath.Join(cur, dir), vfs.RootDir(filepath.Join(cur, indexDir)))
	if e != nil {
		fmt.Printf("E: Open(%s) fail errpr=%s\n", dir, e)
	}

	//ival, e := strconv.ParseUint(query, 10, 64)

	sCond := idx.On(table, vfs.ReaderColumn(column), vfs.MergeOnSearch(!nomerge))
	//info = sCond.Query(query)

	if first {
		result := sCond.Query2(query).First(vfs.ResultOutput("json"))
		fmt.Printf("%s\n", result)
	} else {
		results := sCond.Query2(query).All(vfs.ResultOutput("json"))
		fmt.Printf("%s\n", results)
	}
	sCond.CancelAndWait()

	// if first {
	// 	result := sCond.ToJsonStr(info.First())
	// 	fmt.Printf("%s\n", result)
	// } else {
	// 	results := sCond.ToJsonStrs(info.All())
	// 	for _, result := range results {
	// 		fmt.Printf("%s\n", result)
	// 	}
	// }
	//sCond.CancelAndWait()
}
