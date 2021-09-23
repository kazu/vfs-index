package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
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
	info        infomation of index file
	clean       clean index directory

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
const UsageInfo string = `get information index files

Usage:
	vfs-index info

Flags:	
	-index  	directory for index data
	-column		column name for index  
	-table		table name for index. prefix name in data file
	-data		data directory
	-info       indexfile
	-h,-help    help for merge
`

var Cmds = []Cmd{
	Cmd{
		Name:  "index",
		Usage: UsageIndex,
		Flag:  flag.NewFlagSet("index", flag.ExitOnError),
		Fn:    indexing,
	},
	Cmd{
		Name:  "search",
		Usage: UsageSearch,
		Flag:  flag.NewFlagSet("search", flag.ExitOnError),
		Fn:    search,
	},
	Cmd{
		Name:  "merge",
		Usage: UsageMerge,
		Flag:  flag.NewFlagSet("merge", flag.ExitOnError),
		Fn:    merge,
	},
	Cmd{
		Name:  "info",
		Usage: UsageInfo,
		Flag:  flag.NewFlagSet("info", flag.ExitOnError),
		Fn:    info,
	},
	Cmd{
		Name:  "clean",
		Usage: UsageSearch,
		Flag:  flag.NewFlagSet("clean", flag.ExitOnError),
		Fn:    clean,
	},
	Cmd{
		Name:  "cleantest",
		Usage: UsageSearch,
		Flag:  flag.NewFlagSet("cleantest", flag.ExitOnError),
		Fn:    cleantest,
	},
}

type CmdOpt struct {
	name, indexDir, column, table, dir, query, output, info string
	first, help, nomerge, qstdin, noclean                   bool
}

type Cmd struct {
	Name     string
	Usage    string
	Flag     *flag.FlagSet
	Fn       func(CmdOpt)
	Validate func(CmdOpt) bool
}

func loadConfigfile(odir string, cparam *CmdOpt) *vfs.ConfigFile {
	confdir := odir

	var conf *vfs.ConfigFile
	var err error

	stringNoError := func(s string, e error) string {
		return s
	}

	if confdir == "" {
		confdir, _ = os.UserHomeDir()
		confdir = filepath.Join(confdir, ".vfx-index")
		goto LOAD_FILE
	}

CONF_PWD:

	confdir, _ = os.Getwd()

LOAD_FILE:
	if conf, err = vfs.LoadCmdConfig(confdir); err == nil {
		goto ENSURE
	}

	if confdir == stringNoError(os.UserHomeDir()) {
		goto CONF_PWD
	}

	conf = &vfs.ConfigFile{}
	conf.Name2Index = map[string]*vfs.ConfigIndex{}
	conf.Name2Index[cparam.name] = &vfs.ConfigIndex{}

	conf.Name2Index[cparam.name].IndexDir = cparam.indexDir
	conf.Name2Index[cparam.name].Table = cparam.table
	conf.Name2Index[cparam.name].Dir = cparam.dir

ENSURE:

	if conf.Name2Index[cparam.name] == nil {
		for k, _ := range conf.Name2Index {
			cparam.name = k
		}
	}
	if cparam.indexDir == "./vfs" {
		cparam.indexDir = conf.Name2Index[cparam.name].IndexDir
	}

	if cparam.table == "table" {
		cparam.table = conf.Name2Index[cparam.name].Table
	}

	if cparam.dir == "./" {
		cparam.dir = conf.Name2Index[cparam.name].Dir
	}
	conf.Name2Index[cparam.name].IndexDir = cparam.indexDir
	conf.Name2Index[cparam.name].Table = cparam.table
	conf.Name2Index[cparam.name].Dir = cparam.dir

	return conf

}

func main() {

	if len(os.Args) < 2 {
		fmt.Println(Usage)
		return
	}

	var subCmd string = os.Args[1]
	var cmd Cmd

	cmd.Name = "help"

	for _, c := range Cmds {
		if c.Name == subCmd {
			cmd = c
			break
		}
	}
	if cmd.Name == "help" {
		fmt.Println(Usage)
		return
	}

	opt := CmdOpt{}
	confdir := ""

	cmd.Flag.StringVar(&confdir, "config", "", "configfile directory ")
	cmd.Flag.StringVar(&opt.name, "name", "", "index name")
	cmd.Flag.StringVar(&opt.indexDir, "index", "./vfs", "directory of index")
	cmd.Flag.StringVar(&opt.column, "column", "id", "column name")
	cmd.Flag.StringVar(&opt.table, "table", "table", "table name")
	cmd.Flag.StringVar(&opt.dir, "data", "./", "datadir")

	cmd.Flag.StringVar(&opt.query, "query", "", "search query")
	cmd.Flag.StringVar(&opt.query, "q", "", "search query"+"(shorthand)")
	cmd.Flag.BoolVar(&opt.qstdin, "qstdin", false, "string search by stdin")

	cmd.Flag.BoolVar(&opt.first, "only-first", false, "only output one record")
	cmd.Flag.BoolVar(&opt.first, "f", false, "only output one record"+"()shorthand")

	cmd.Flag.BoolVar(&opt.nomerge, "nomerge", false, "not merge index on search")

	cmd.Flag.StringVar(&opt.output, "output", "json", "output format")
	cmd.Flag.StringVar(&opt.output, "o", "json", "output format"+"()shorthand")

	cmd.Flag.StringVar(&opt.info, "info", "", "get infomation indexfile")
	cmd.Flag.BoolVar(&opt.noclean, "noclean", false, "cleaning empty directory")

	cmd.Flag.BoolVar(&opt.help, "help", false, "help")
	cmd.Flag.BoolVar(&opt.help, "h", false, "help (shorthand)")

	cmd.Flag.Parse(os.Args[2:])

	if !validate(os.Args, opt) {
		fmt.Println(cmd.Usage)
		return
	}
	conf := loadConfigfile(confdir, &opt)

	vfs.SaveCmdConfig(confdir, conf)

	cmd.Fn(opt)

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
	opts := []vfs.Option{}
	opts = append(opts, vfs.ReaderColumn(opt.column))
	opts = append(opts, vfs.MergeDuration(1*time.Minute))
	opts = append(opts, vfs.MergeOnSearch(true))
	opts = append(opts, vfs.EnableCleanAfterMerge(!opt.noclean))

	sCond := idx.On(opt.table, opts...)

	sCond.StartMerging()
	time.Sleep(1 * time.Minute)
	sCond.CancelAndWait()
}

func setup_command(opt CmdOpt) (*vfs.SearchCond, error) {
	vfs.CurrentLogLoevel = vfs.LOG_WARN
	idx, e := vfs.Open(opt.dir, vfs.RootDir(opt.indexDir))
	if e != nil {
		return nil, e
	}
	// FIXME: should implement Close()
	//defer idx.Close()
	sCond := idx.On(opt.table, vfs.ReaderColumn(opt.column),
		vfs.MergeDuration(1*time.Minute),
		vfs.MergeOnSearch(true))
	return sCond, nil

}

func info(opt CmdOpt) {
	sCond, e := setup_command(opt)

	vfs.CurrentLogLoevel = vfs.LOG_WARN
	if e != nil {
		fmt.Printf("E: Open(%s) fail errpr=%s\n", opt.dir, e)
		return
	}

	if opt.info == "" {
		return
	}
	f := vfs.NewIndexFile(sCond.IndexFile().Column(), opt.info)
	f.Init()

	switch f.Ftype {
	case vfs.IdxFileType_Merge:
		for _, kr := range f.KeyRecords().All() {
			fmt.Printf("key=0x%0x count=%d\n", kr.Key().Uint64(), kr.Records().Count())
		}
		return
	case vfs.IdxFileType_Write:
		kr := f.KeyRecord()
		fmt.Printf("key=0x%0x count=%d\n", kr.Key().Uint64(), 1)
		return
	}

}

func clean(opt CmdOpt) {
	sCond, e := setup_command(opt)

	if e != nil || sCond == nil {
		fmt.Printf("E: Open(%s) fail errpr=%s\n", opt.dir, e)
		return
	}
	f := sCond.IndexFile()
	f.Column().CleanDirs()
	return
}

func cleantest(opt CmdOpt) {
	sCond, e := setup_command(opt)

	if e != nil || sCond == nil {
		fmt.Printf("E: Open(%s) fail errpr=%s\n", opt.dir, e)
		return
	}
	f := sCond.IndexFile()
	f.Column().CleanDirTest(1)
	return
}

func validate(cmdArtgs []string, opt CmdOpt) bool {
	if opt.help || len(cmdArtgs) < 3 {
		//fmt.Println(UsageSearch)
		return false
	}
	return true
}

func prepare_search(opt CmdOpt) {
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
	return
}

func search(opt CmdOpt) {
	prepare_search(opt)

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
