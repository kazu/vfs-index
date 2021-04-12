package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	vfs "github.com/kazu/vfs-index"
	"github.com/kazu/vfs-index/expr"
)

const Usage string = `vfs-index ... indexer/search in vfs data(json,csv)

Usage: 
	vfs-indx [command]

Available Commands:
	index		index data
	search		search data
	merge		merge index (only 1 minute)

Flags:
	-h,-help    help for vfs-index

`
const UsageIndex string = `create index

Usage:
	vfs-index index

Flags:	
	-index  	directory for index data
	-column		column name for index  
	-table		table name for index. prefix name in data file
	-data		data directory
	-h,-help    help for index
`

const UsageSearch string = `Search index

Usage:
	vfs-index search

Flags:	
	-index  	directory for index data
	-column		column name for index  
	-table		table name for index. prefix name in data file
	-data		data directory
	-output 	output format, json, csv available. Default: json 
	-qstdin		string search via STDIN, if qstdin is set, ignore -q flag
	-q,-query	search query 
				example)  
					string search: 		'name.search("foobar")'
					numeric condition:	'id == 23456' or 'id < 23456' ...
	-h,-help    help for search
`

const UsageMerge string = `Merge splited index file

Usage:
	vfs-index merge

Flags:	
	-index  	directory for index data
	-column		column name for index  
	-table		table name for index. prefix name in data file
	-data		data directory
	-h,-help    help for merge
`

type CmdOpt struct {
	indexDir, column, table, dir, query, output string
	first, help, nomerge, qstdin                bool
}

func main() {

	if len(os.Args) < 2 {
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
	}
	opt := CmdOpt{}

	flagCmd.StringVar(&opt.indexDir, "index", "./vfs", "directory of index")
	flagCmd.StringVar(&opt.column, "column", "id", "column name")
	flagCmd.StringVar(&opt.table, "table", "table", "table name")
	flagCmd.StringVar(&opt.dir, "data", "./", "datadir")

	flagCmd.StringVar(&opt.query, "query", "", "search query")
	flagCmd.StringVar(&opt.query, "q", "", "search query"+"(shorthand)")
	flagCmd.BoolVar(&opt.qstdin, "qstdin", false, "string search by stdin")

	flagCmd.BoolVar(&opt.first, "only-first", false, "only output one record")
	flagCmd.BoolVar(&opt.first, "f", false, "only output one record"+"()shorthand")

	flagCmd.BoolVar(&opt.nomerge, "nomerge", false, "not merge index on search")

	flagCmd.StringVar(&opt.output, "output", "json", "output format")
	flagCmd.StringVar(&opt.output, "o", "json", "output format"+"()shorthand")

	flagCmd.BoolVar(&opt.help, "help", false, "help")
	flagCmd.BoolVar(&opt.help, "h", false, "help (shorthand)")

	flagCmd.Parse(os.Args[2:])

	switch flagCmd.Name() {
	case "index":
		if opt.help || len(os.Args) < 3 {
			fmt.Println(UsageIndex)
			return
		}
		indexing(opt)
	case "search":
		if opt.help || len(os.Args) < 3 {
			fmt.Println(UsageSearch)
			return
		}
		if opt.qstdin {
			line, _, _ := bufio.NewReader(os.Stdin).ReadLine()
			if strs := strings.Fields(string(line)); len(strs) > 0 {
				for i, str := range strs {
					nstr := fmt.Sprintf("%s.search(\"%s\")", opt.column, str)
					if i == 0 {
						opt.query = nstr
					} else {
						opt.query += ` && ` + nstr
					}
				}
			} else {
				opt.query = fmt.Sprintf("%s.search(\"%s\")", opt.column, line)
			}
		}
		search(opt)
	case "merge":
		if opt.help || len(os.Args) < 3 {
			fmt.Println(UsageMerge)
			return
		}
		merge(opt)
	default:
		if opt.help {
			fmt.Println(Usage)
			return
		}
	}

	return
}

func indexing(opt CmdOpt) {
	vfs.CurrentLogLoevel = vfs.LOG_WARN
	//vfs.CurrentLogLoevel = vfs.LOG_DEBUG

	idx, e := vfs.Open(opt.dir, vfs.RootDir(opt.indexDir), vfs.RegitConcurrent(8))

	if e != nil {
		fmt.Printf("E: Open(%s) fail errpr=%s\n", opt.dir, e)
	}
	e = idx.Regist(opt.table, opt.column)

}

func merge(opt CmdOpt) {
	vfs.CurrentLogLoevel = vfs.LOG_WARN
	//vfs.CurrentLogLoevel = vfs.LOG_DEBUG

	idx, e := vfs.Open(opt.dir, vfs.RootDir(opt.indexDir))

	if e != nil {
		fmt.Printf("E: Open(%s) fail errpr=%s\n", opt.dir, e)
	}
	sCond := idx.On(opt.table, vfs.ReaderColumn(opt.column),
		vfs.MergeDuration(1*time.Minute),
		vfs.MergeOnSearch(true))

	sCond.StartMerging()
	time.Sleep(1 * time.Minute)
	sCond.CancelAndWait()

}

func search(opt CmdOpt) {
	//vfs.CurrentLogLoevel = vfs.LOG_DEBUG
	vfs.CurrentLogLoevel = vfs.LOG_ERROR

	var b strings.Builder
	vfs.LogWriter = &b

	if len(opt.query) == 0 {
		fmt.Fprint(os.Stderr, "Search: query is empty\n")
		return
	}

	q, e := expr.GetExpr(opt.query)
	if e != nil {
		fmt.Fprintf(os.Stderr, "Search: fail parse query\n")
		return
	}
	if len(q.Column) > 0 {
		opt.column = q.Column
	}

	idx, e := vfs.Open(opt.dir, vfs.RootDir(opt.indexDir))
	if e != nil {
		fmt.Printf("E: Open(%s) fail errpr=%s\n", opt.dir, e)
	}

	sCond := idx.On(opt.table, vfs.ReaderColumn(opt.column), vfs.MergeOnSearch(!opt.nomerge))

	if opt.first {
		result := sCond.Query(opt.query).First(vfs.ResultOutput(opt.output))
		fmt.Printf("%s\n", result)
	} else {
		results := sCond.Query(opt.query).All(vfs.ResultOutput(opt.output))
		fmt.Printf("%s\n", results)
	}
	sCond.CancelAndWait()

	out := os.Stderr
	out.WriteString(b.String())
}
