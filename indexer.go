package vfsindex

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/kazu/loncha"
)

type LogLevel int

const (
	LOG_ERROR LogLevel = iota
	LOG_WARN
	LOG_DEBUG
)

var CurrentLogLoevel LogLevel = LOG_DEBUG
var LogWriter io.StringWriter = os.Stderr

func LogIsDebug() bool {
	return CurrentLogLoevel == LOG_DEBUG
}

func Log(l LogLevel, f string, args ...interface{}) {

	if CurrentLogLoevel < l {
		return
	}

	var b strings.Builder
	_ = b
	switch l {
	case LOG_DEBUG:
		b.WriteString("D: ")
	case LOG_WARN:
		b.WriteString("W: ")
	case LOG_ERROR:
		b.WriteString("E: ")
	}
	fmt.Fprintf(&b, f, args...)
	LogWriter.WriteString(b.String())
	return
}

type Indexer struct {
	Root string
	Cols map[string]*Column
	opt  optionState
}

func TrimFilePathSuffix(path string) string {

	return path[0 : len(path)-len(filepath.Ext(path))]

}

// Open ... open index. dpath is data directory,
func Open(dpath string, opts ...Option) (*Indexer, error) {
	mergeOpt(&Opt, opts...)
	//mergeOpt(&Opt, RootDir(dpath))

	idx := &Indexer{Root: dpath, Cols: make(map[string]*Column)}
	idx.setCsvDecoder()
	return idx, nil
}

// Regist ... indexing specified table , col (column)
func (idx *Indexer) Regist(table, col string) error {

	if len(table) == 0 || table[0] == '.' {
		return ErrInvalidTableName
	}

	flist, err := idx.openFileList(table)
	flist.Update()
	flist.Reload()

	idxCol := idx.OpenCol(flist, table, col)
	_ = idxCol
	idxCol.Update(Opt.mergeDuration)
	idx.Cols[col] = idxCol

	return err
}

// On ... return SearchCond(Search Element) , table is table name, column is set by ReaderColumn("column name")
func (idx *Indexer) On(table string, opts ...Option) *SearchCond {
	flist, err := idx.openFileList(table)
	if err != nil {
		return &SearchCond{idx: idx, Err: err}
	}
	mergeOpt(&idx.opt, opts...)
	cond := &SearchCond{idx: idx, flist: flist, table: table, column: ""}
	if len(idx.opt.column) > 0 {
		opt := &idx.opt
		cond.startCol(opt.column)
		cond.idxCol.isMergeOnSearch = opt.idxMergeOnSearch
	}

	return cond
}

func (idx *Indexer) setCsvDecoder() {

	var csvHeaders []string

	setDecuoder("csv",
		Decoder{
			FileType: "csv",
			Encoder: func(ov interface{}) ([]byte, error) {
				header := []string{}
				if len(csvHeaders) > 0 {
					header = csvHeaders
				}
				getHeader := func(data map[string]interface{}) (head []string) {
					for key, _ := range data {
						head = append(head, key)
					}
					return
				}

				writeLine := func(w *csv.Writer, head []string, data map[string]interface{}) error {
					line := make([]string, len(head))

					for key, infv := range data {
						idx, e := loncha.IndexOf(head, func(i int) bool {
							return head[i] == key
						})
						if e != nil {
							continue
						}
						if v, ok := infv.(string); ok {
							line[idx] = strconv.QuoteToGraphic(v)
							//line[idx] = strings.Replace(v, "\n", "\\n", -1)
						}
						if v, ok := infv.(uint64); ok {
							line[idx] = fmt.Sprintf("%d", v)
						}
					}
					w.Write(line)

					return nil
				}
				b := bytes.NewBuffer([]byte{})
				w := csv.NewWriter(b)

				switch data := ov.(type) {
				case []interface{}:
					if len(data) > 0 && len(header) == 0 {
						header = getHeader(data[0].(map[string]interface{}))
					}
					w.Write(header)

					for _, line := range data {
						d, ok := line.(map[string]interface{})
						if !ok {
							continue
						}
						writeLine(w, header, d)
					}
					w.Flush()
					return b.Bytes(), nil
				case map[string]interface{}:
					if len(header) == 0 {
						header = getHeader(data)
					}
					w.Write(header)
					writeLine(w, header, data)
					w.Flush()
					return b.Bytes(), nil
				}

				return nil, ErrNotSupported //json.Marshal(v)
			},
			Decoder: func(raw []byte, v interface{}) error {
				if len(csvHeaders) == 0 {
					return ErrMustCsvHeader
				}

				value, ok := v.(*(map[string]interface{}))
				if !ok {
					return ErrNotSupported
				}
				r := csv.NewReader(bytes.NewBuffer(raw))
				data, e := r.Read()
				if e != nil && e != io.EOF {
					return e
				}
				for i := range data {
					//FIXME; type check in this
					iv, err := strconv.Atoi(data[i])
					if err == nil {
						(*value)[csvHeaders[i]] = uint64(iv)
					} else {
						(*value)[csvHeaders[i]] = data[i]
					}
				}
				return nil
			},
			Tokenizer: func(ctx context.Context, rio io.Reader, f *File) <-chan *Record {
				ch := make(chan *Record, 5)
				go func() {
					//buf, err := ioutil.ReadAll(rio)
					var err error
					offset := int64(0)
					//size := int64(0)
					defer close(ch)
					scanner := bufio.NewScanner(rio)
					if scanner.Scan() {
						text := scanner.Text()
						csvHeaders, err = csv.NewReader(strings.NewReader(text)).Read()
						if err != nil {
							return
						}
						offset = int64(len(text)) + 1
					}

					for scanner.Scan() {
						text := scanner.Text()
						ch <- &Record{fileID: f.id, offset: offset, size: int64(len(text))}
						offset += int64(len(text)) + 1
						select {
						case <-ctx.Done():
							return
						default:
						}
					}
				}()
				return ch
			},
		},
	)
}

func (idx *Indexer) openFileList(table string) (flist *FileList, err error) {

	dir := filepath.Dir(table)
	tableDir := filepath.Join(idx.Root, dir)

	if _, e := os.Stat(FileListPath(tableDir)); os.IsNotExist(e) {
		flist, err = CreateFileList(tableDir)
		if err != nil {
			return nil, err
		}
	}
	flist = OpenFileList(tableDir)
	return
}
