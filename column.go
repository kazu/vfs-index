package vfsindex

import (
	"context"
	"regexp"

	//"encoding/csv"

	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	query "github.com/kazu/vfs-index/qeury"

	"github.com/kazu/loncha"
	"github.com/kazu/vfs-index/vfs_schema"
	"github.com/schollz/progressbar/v3"
	"github.com/vbauerster/mpb/v5"
	"github.com/vbauerster/mpb/v5/decor"
)

const (
	RECORDS_INIT = 64
)

const (
	RECORD_WRITING byte = iota
	RECORD_WRITTEN
	RECORD_MERGING
	RECORD_MERGED
)

type Column struct {
	Table   string
	Name    string
	Dir     string
	Flist   *FileList
	IsNum   bool
	Dirties Records

	cache *IdxCaches

	ctx             context.Context
	ctxCancel       context.CancelFunc
	done            chan bool
	isMergeOnSearch bool
}

type Record struct {
	fileID uint64
	offset int64
	size   int64
	cache  map[string]interface{}
}

type Records []*Record

type Decoder struct {
	FileType  string
	Decoder   func([]byte, interface{}) error
	Encoder   func(interface{}) ([]byte, error)
	Tokenizer func(io.Reader, *File) <-chan *Record
}

var CsvHeader string

var DefaultDecoder []Decoder = []Decoder{
	Decoder{
		FileType: "csv",
		Encoder: func(v interface{}) ([]byte, error) {
			return json.Marshal(v)
		},
		Decoder: func(raw []byte, v interface{}) error {
			// header := strings.Split(CsvHeader, ",")
			// dec, err := csvutil.NewDecoder(bytes.NewReader(raw), header...)
			// if err != nil {
			// 	return err
			// }
			// dec.Map = func(field, column string, v interface{}) string {
			// 	if _, ok := v.(float64); ok && field == "n/a" {
			// 		return "NaN"
			// 	}
			// 	return field
			return nil
		},
		Tokenizer: func(rio io.Reader, f *File) <-chan *Record {
			ch := make(chan *Record, 5)

			go func() {
				buf, err := ioutil.ReadAll(rio)

				if err != nil {
					defer close(ch)

				}

				s := string(buf)

				lines := strings.Split(s, "\n")
				CsvHeader = lines[0]
				lines = lines[1:]
				cur := len(CsvHeader) + 1
				for _, line := range lines {
					ch <- &Record{fileID: f.id, offset: int64(cur), size: int64(len(line))}
					cur += len(line) + 1
				}
				close(ch)
			}()
			return ch
		},
	},
	Decoder{
		FileType: "json",
		Encoder: func(v interface{}) ([]byte, error) {
			return json.Marshal(v)
		},
		Decoder: func(raw []byte, v interface{}) error {
			e := json.Unmarshal(raw, v)
			if e != nil {
				return e
			}
			if value, ok := v.(*(map[string]interface{})); ok {
				for key, v := range *value {
					if f64, ok := v.(float64); ok {
						(*value)[key] = uint64(f64)
					}
				}
			}
			return nil

		},
		Tokenizer: func(rio io.Reader, f *File) <-chan *Record {
			ch := make(chan *Record, 5)
			go func() {
				dec := json.NewDecoder(rio)

				var rec *Record

				nest := int(0)
				for {
					token, err := dec.Token()
					if err == io.EOF {
						break
					}
					switch token {
					case json.Delim('{'):
						nest++
						if nest == 1 {
							rec = &Record{fileID: f.id, offset: dec.InputOffset() - 1}
						}

					case json.Delim('}'):
						nest--
						if nest == 0 {
							rec.size = dec.InputOffset() - rec.offset
							ch <- rec
						}
					}
				}
				close(ch)
			}()
			return ch
		},
	},
}

// GetDecoder ... return format Decoder/Encoder from fname(file name)
func GetDecoder(fname string) (dec Decoder, e error) {
	if len(fname) < 1 {
		return dec, ErrInvalidTableName
	}

	ext := filepath.Ext(fname)[1:]

	idx, e := loncha.IndexOf(DefaultDecoder, func(i int) bool {
		return DefaultDecoder[i].FileType == ext
	})
	if e != nil {
		Log(LOG_ERROR, "cannot find %s decoder\n", ext)
		return dec, e
	}
	return DefaultDecoder[idx], nil

}

func NewRecords(n int) Records {
	return make(Records, 0, n)
}

func NewRecord(id uint64, offset, size int64) *Record {
	return &Record{fileID: id, offset: offset, size: size}
}

func (recs Records) Add(r *Record) Records {
	return append(recs, r)
}

func ColumnPath(tdir, col string, isNum bool) string {
	if isNum {
		return filepath.Join(Opt.rootDir, tdir, col+".num.idx")

	}

	return filepath.Join(Opt.rootDir, tdir, col+".gram.idx")
}

func JoinExt(s ...string) string {

	return strings.Join(s, ".")

}

func AddingDir(s string, n int) string {

	if s == "*" {
		//return "**/"
		return "*/*/*/"
	}

	if n < 1 {
		n = 2
	}
	var b strings.Builder
	for i := 0; i < len(s); i += n {
		if len(s[i:]) < n {
			b.WriteString(s[i:])
		} else {
			b.WriteString(s[i : i+n])
		}
		b.WriteString("/")
	}
	return b.String()
}

func ColumnPathWithStatus(tdir, col string, isNum bool, s, e string, status byte) string {
	if status == RECORD_WRITING {

	}
	switch status {
	case RECORD_WRITING:
		// base.adding.process id.start-end
		// <column name>.<index type>.idx.adding.<pid>.<start>-<end>
		//   <index type> ...  num or tri ?
		//   <start>,<end>  ... value
		return fmt.Sprintf("%s.adding.%d.%s-%s", ColumnPath(tdir, col, isNum), os.Getgid(), s, e)
	case RECORD_WRITTEN:
		// <column name>.<index type>.<start>-<end>.<inode number>.<offset>
		return fmt.Sprintf("%s.%s-%s", ColumnPath(tdir+"/"+AddingDir(s, 4), col, isNum), s, e)

	case RECORD_MERGING:
		return fmt.Sprintf("%s.merging.%d.%s-%s", ColumnPath(tdir, col, isNum), os.Getgid(), s, e)

	case RECORD_MERGED:
		return fmt.Sprintf("%s.merged.%s-%s", ColumnPath(tdir, col, isNum), s, e)

	}
	return ""

}

func (idx *Indexer) OpenCol(flist *FileList, table, col string) *Column {

	return NewColumn(flist, table, col)
}

func NewColumn(flist *FileList, table, col string) *Column {
	//if _, e := os.Stat(ColumnPath(tableDir)); os.IsNotExist(e) {
	return &Column{
		Table:   table,
		Name:    col,
		Flist:   flist,
		Dirties: NewRecords(RECORDS_INIT),
		cache:   NewIdxCaches(),
	}
}

func (c *Column) Update(d time.Duration) error {

	idxDir := filepath.Join(Opt.rootDir, c.Table)
	err := os.MkdirAll(idxDir, os.ModePerm)
	if err != nil {
		return err
	}

	for _, f := range c.Flist.Files {
		if len(f.name) <= len(filepath.Base(c.Table)) {
			continue
		}
		if f.name[0:len(filepath.Base(c.Table))] == c.Table {
			c.updateFile(f)
		}
	}
	c.WriteDirties()
	//Log(LOG_WARN, "Called WriteDirtues \n")
	// FIXME
	ctx, cancel := context.WithTimeout(context.Background(), d)
	//defer cancel()
	go c.MergingIndex(ctx)
	time.Sleep(d)
	cancel()
	<-c.done

	return nil
}

func (c *Column) updateFile(f *File) {

	for r := range f.Records(c.Flist.Dir) {
		c.Dirties = c.Dirties.Add(r)
	}
}

func (c *Column) WriteDirties() {
	if Opt.cntConcurrent < 1 {
		Opt.cntConcurrent = 1
	}
	s := time.Now()
	Log(LOG_WARN, "Indexing %d concurrent\n", Opt.cntConcurrent)
	ch := make(chan int, len(c.Dirties))
	chDone := make(chan bool, Opt.cntConcurrent)

	bar := progressbar.Default(int64(len(c.Dirties)))
	writeRecord := func(ch chan int) {
		for {
			i, ok := <-ch
			if !ok || i < 0 {
				break
			}
			r := c.Dirties[i]
			e := r.Write(c)
			if e == nil {
				c.Dirties[i] = nil
				bar.Add(1)
			}
		}
		chDone <- true
	}
	for i := 0; i < Opt.cntConcurrent; i++ {
		go writeRecord(ch)
	}
	go func() {
		for i := range c.Dirties {
			ch <- i
		}
		for i := 0; i < Opt.cntConcurrent; i++ {
			ch <- -1
		}
	}()

	for i := 0; i < Opt.cntConcurrent; i++ {
		<-chDone
	}
	loncha.Delete(&c.Dirties, func(i int) bool {
		return c.Dirties[i] == nil
	})
	d := time.Now().Sub(s)
	Log(LOG_WARN, "WriteDiries() elapsed %s %d\n", d, len(c.Dirties))
}

func (c *Column) cancelAndWait() {
	if c.ctx != nil {
		c.ctxCancel()
		<-c.done
	}
}

func (c *Column) MergingIndex(ctx context.Context) error {

	var idxWriter IdxWriter

	if c.IsNumViaIndex() {
		c.IsNum = true
	}

	if !c.IsNum {
		idxWriter = IdxWriter{
			IsNum: false,
			ValueEncoder: func(r *Record) (results []string) {
				//return []string{toFname(r.Uint64Value(c))}
				return EncodeTri(r.StrValue(c))
			},
		}
	} else {
		idxWriter = IdxWriter{
			IsNum: true,
			ValueEncoder: func(r *Record) (results []string) {
				return []string{toFname(r.Uint64Value(c))}
			},
		}
	}
	return c.mergingIndex(idxWriter, ctx)

}

// Write ... write column index
func (r *Record) Write(c *Column) error {

	var idxWriter IdxWriter
	r.caching(c)

	if _, ok := r.cache[c.Name].(string); ok {
		idxWriter = IdxWriter{
			IsNum: false,
			ValueEncoder: func(r *Record) (results []string) {
				//return []string{toFname(r.Uint64Value(c))}
				return EncodeTri(r.StrValue(c))
			},
		}
	} else {
		idxWriter = IdxWriter{
			IsNum: true,
			ValueEncoder: func(r *Record) (results []string) {
				return []string{toFname(r.Uint64Value(c))}
			},
		}
	}
	return r.write(c, idxWriter)

}

func (c *Column) noMergedFPath() (idxfiles <-chan string, err error) {
	path := ColumnPathWithStatus(c.TableDir(), c.Name, c.IsNum, "*", "*", RECORD_WRITTEN)
	pat := fmt.Sprintf("%s.*.*", path)

	return paraGlob(pat), nil
}

func (c *Column) noMergedFPathWithPat() (idxfiles <-chan string, pat string, err error) {
	path := ColumnPathWithStatus(c.TableDir(), c.Name, c.IsNum, "*", "*", RECORD_WRITTEN)
	pat = fmt.Sprintf("%s.*.*", path)

	return paraGlob(pat), pat, nil
}

func idxPath2Info(idxpath string) (fileID uint64, offset int64, first uint64, last uint64) {

	strs := strings.Split(filepath.Base(idxpath), ".")
	if len(strs) < 5 {
		return
	}
	if len(strs) == 5 {
		return idxPath2InfoMerge(idxpath)
	}

	sRange := strs[3]

	fileID, _ = strconv.ParseUint(strs[4], 16, 64)
	offset, _ = strconv.ParseInt(strs[5], 16, 64)

	strs = strings.Split(sRange, "-")
	first, _ = strconv.ParseUint(strs[0], 16, 64)
	last, _ = strconv.ParseUint(strs[1], 16, 64)

	return

}

func idxPath2InfoMerge(idxpath string) (fileID uint64, offset int64, first uint64, last uint64) {

	strs := strings.Split(filepath.Base(idxpath), ".")
	if len(strs) != 5 {
		return
	}
	sRange := strs[4]

	strs = strings.Split(sRange, "-")
	first, _ = strconv.ParseUint(strs[0], 16, 64)
	last, _ = strconv.ParseUint(strs[1], 16, 64)

	return

}

type IndexPathInfo struct {
	fileID uint64
	offset int64
	first  uint64
	last   uint64
}

func NewIndexInfo(fileID uint64, offset int64, first uint64, last uint64) IndexPathInfo {
	return IndexPathInfo{
		fileID: fileID,
		offset: offset,
		first:  first,
		last:   last,
	}
}

func (c *Column) IsNumViaIndex() bool {

	path := ColumnPath(c.TableDir(), c.Name, true)
	pat := fmt.Sprintf("%s*", path)
	Log(LOG_WARN, "finding %s files\n", pat)
	idxfiles := paraGlob(pat)
	_, ok := <-idxfiles
	if !ok {
		path = ColumnPath(c.TableDir()+"/"+AddingDir("*", 4), c.Name, true)
		pat = fmt.Sprintf("%s*", path)
		Log(LOG_WARN, "finding %s files\n", pat)
		idxfiles = paraGlob(pat)
		_, ok := <-idxfiles
		if !ok {
			Log(LOG_WARN, "not found %s files\n", pat)
			return false
		}
		Log(LOG_WARN, "found %s files\n", pat)
	}
	return true
}

func (c *Column) loadIndex() error {
	if c.IsNumViaIndex() {
		c.IsNum = true
	}

	path := ColumnPathWithStatus(c.TableDir(), c.Name, c.IsNum, "*", "*", RECORD_MERGED)
	pat := fmt.Sprintf("%s", path)
	idxfilesCh := paraGlob(pat)
	cnt := 0
	for file := range idxfilesCh {
		cnt++
		first := NewIndexInfo(idxPath2Info(file)).first
		last := NewIndexInfo(idxPath2Info(file)).last
		c.cache.infos = append(c.cache.infos,
			&IdxInfo{
				path:  file,
				first: first,
				last:  last,
			})
	}

	if cnt == 0 {
		Log(LOG_WARN, "loadIndex() %s is not found. force creation ctx=%v\n", pat, c.ctx)
		if c.ctx == nil {
			c.ctx, c.ctxCancel = context.WithTimeout(context.Background(), 1*time.Minute)
		}
		go c.MergingIndex(c.ctx)
		return ErrNotFoundFile
	}

	return nil

}
func (c *Column) mergingIndex(w IdxWriter, ctx context.Context) error {

	c.done = make(chan bool, 2)
	defer close(c.done)

	noMergeIdxCh, e := c.noMergedFPath()
	if e != nil {
		return e
	}
	//noMergeIdxFiles = noMergeIdxFiles[0:20]

	vname := func(key uint64) string {

		if c.IsNum {
			return toFname(key)
		}
		return fmt.Sprintf("%012x", key)
	}

	root := query.NewRoot()
	root.SetVersion(query.FromInt32(1))
	root.WithHeader()
	root.SetIndexType(query.FromByte(byte(vfs_schema.IndexIndexNum)))

	idxNum := query.NewIndexNum()
	keyrecords := query.NewKeyRecordList()

	var keyRecord *query.KeyRecord
	var recs *query.RecordList
	total := 10000 //len(noMergeIdxFiles)
	p := mpb.New(mpb.WithWidth(64))
	bar := p.AddBar(int64(total),
		mpb.PrependDecorators(
			decor.Name("index merging", decor.WC{W: len("index merging") + 1, C: decor.DidentRight}),
			decor.OnComplete(
				decor.AverageETA(decor.ET_STYLE_GO, decor.WC{W: 4}), "done",
			),
		),
		mpb.AppendDecorators(decor.CountersNoUnit("%d / %d")),
	)

	LastIdx := 0 //len(noMergeIdxFiles) - 1
	noMergeIdxFiles := []string{}

	i := 0
	for noMergeIdxFile := range noMergeIdxCh {
		noMergeIdxFiles = append(noMergeIdxFiles, noMergeIdxFile)
		bar.Increment()

		rio, e := os.Open(noMergeIdxFile)

		if e != nil {
			Log(LOG_WARN, "mergingIndex(): %s column index file not found\n", noMergeIdxFile)
			continue
		}
		defer rio.Close()
		record := fbsRecord(rio)
		if keyRecord == nil ||
			NewIndexInfo(idxPath2Info(noMergeIdxFile)).first != keyRecord.Key().Uint64() {

			if keyRecord != nil {
				cnt := keyrecords.Count()
				keyRecord.SetRecords(recs.CommonNode)
				keyRecord.Merge()
				keyrecords.SetAt(cnt, keyRecord)

				// if i < 10 {
				// 	//FIXME debug only remove later
				// 	kr, _ := keyrecords.At(cnt)
				// 	r, _ := kr.Records().At(0)
				// 	fmt.Printf("keyRecords[%d] = {Key: %d, len(Records)=%d records[0].file_id=%d}\n",
				// 		cnt, kr.Key().Uint64(), kr.Records().Count(), r.Offset().Int64())
				// 	fmt.Printf("record.file_id=%d\n", record.Offset().Int64())
				// }
			}
			keyRecord = query.NewKeyRecord()
			keyRecord.SetKey(query.FromUint64(NewIndexInfo(idxPath2Info(noMergeIdxFile)).first))
		}

		cnt := keyRecord.Records().Count()

		if cnt == 0 {
			recs = query.NewRecordList()
		} else {
			recs = keyRecord.Records()
		}
		recs.SetAt(cnt, record)

		select {
		case <-ctx.Done():
			LastIdx = i
			Log(LOG_WARN, "mergingIndex cancel() last_merge=%s\n", noMergeIdxFile)
			bar.IncrBy(len(noMergeIdxFiles) - i)
			goto FINISH
		default:
		}
		i++
	}

FINISH:
	cnt := keyrecords.Count()
	if cnt == 0 {
		Log(LOG_WARN, "mergingIndex no write\n")
		return nil
	}

	if query.KeyRecordSingle(keyrecords.At(cnt-1)).Key().Uint64() != keyRecord.Key().Uint64() {
		keyRecord.SetRecords(recs.CommonNode)
		keyRecord.Merge()
		keyrecords.SetAt(cnt, keyRecord)
	}
	keyrecords.Merge()
	idxNum.SetIndexes(keyrecords.CommonNode)

	root.SetIndex(idxNum.CommonNode)
	root.Merge()

	noMergeIdxFiles = noMergeIdxFiles[0 : LastIdx+1]

	lastPos := len(noMergeIdxFiles) - 1
	first := NewIndexInfo(idxPath2Info(noMergeIdxFiles[0])).first
	last := NewIndexInfo(idxPath2Info(noMergeIdxFiles[lastPos])).last
	// FIXME old index merge
	wIdxPath := ColumnPathWithStatus(c.TableDir(), c.Name, w.IsNum, vname(first), vname(last), RECORD_MERGING)
	path := ColumnPathWithStatus(c.TableDir(), c.Name, w.IsNum, vname(first), vname(last), RECORD_MERGED)

	io, e := os.Create(wIdxPath)
	if e != nil {
		Log(LOG_WARN, "F:mergingIndex() cannot create... %s\n", wIdxPath)
		return e
	}
	defer io.Close()

	io.Write(root.R(0))
	Log(LOG_DEBUG, "S: written %s \n", wIdxPath)
	e = SafeRename(wIdxPath, path)
	if e != nil {
		os.Remove(wIdxPath)
		Log(LOG_DEBUG, "F: rename %s -> %s \n", wIdxPath, path)
		return e
	}

	Log(LOG_DEBUG, "S: renamed %s -> %s \n", wIdxPath, path)

	// remove merged file
	for _, noMergeIdxFile := range noMergeIdxFiles {
		os.Remove(noMergeIdxFile)
	}
	Log(LOG_DEBUG, "S: remove merged files count=%d \n", len(noMergeIdxFiles))

	c.cache.infos = append(c.cache.infos,
		&IdxInfo{
			path:  path,
			first: first,
			last:  last,
			buf:   root.R(0),
		})

	return nil
}

func (r *Record) write(c *Column, w IdxWriter) error {

	for _, vName := range w.ValueEncoder(r) {

		wPath := ColumnPathWithStatus(c.TableDir(), c.Name, w.IsNum, vName, vName, RECORD_WRITING)
		path := ColumnPathWithStatus(c.TableDir(), c.Name, w.IsNum, vName, vName, RECORD_WRITTEN)
		path = fmt.Sprintf("%s.%010x.%010x", path, r.fileID, r.offset)

		//Log(LOG_DEBUG, "path=%s %s \n", wPath, c.TableDir(), c)
		if FileExist(path) {
			//Log(LOG_WARN, "F: skip create %s. %s is already exists \n", wPath, path)
			continue
		}
		io, e := os.Create(wPath)
		if e != nil {
			Log(LOG_WARN, "F: create...%s err=%s\n", wPath, e)
			return e
		}
		io.Write(r.ToFbs(r.Uint64Value(c)))

		io.Close()
		Log(LOG_DEBUG, "S: written %s \n", wPath)
		os.MkdirAll(filepath.Dir(path), os.ModePerm)
		if w.IsNum && r.Uint64Value(c) == 0 {
			//spew.Dump(r)
		}

		e = SafeRename(wPath, path)
		if e != nil {
			os.Remove(wPath)
			Log(LOG_DEBUG, "F: rename %s -> %s \n", wPath, path)
			return e
		}
		Log(LOG_DEBUG, "S: renamed%s -> %s \n", wPath, path)

	}

	return nil
}

func (r *Record) parse(raw []byte, dec Decoder) {

	e := dec.Decoder(raw, &r.cache)

	if e != nil {
		Log(LOG_ERROR, "e=%s Raw='%s' Record=%+v\n", e, strings.ReplaceAll(string(raw), "\n", ""), r.cache)
		return
	}

	return
}

func (c *Column) RecordEqInt(v int) (record *Record) {

	path := ColumnPathWithStatus(c.TableDir(), c.Name, true, toFname(uint64(v)), toFname(uint64(v)), RECORD_WRITTEN)
	rio, e := os.Open(path)
	if e == nil {
		record = RecordFromFbs(rio)
		record.caching(c)
		rio.Close()
		return
	}

	return nil
}

func toFname(i interface{}) string {
	return fmt.Sprintf("%010x", i)
}

func toFnameTri(i interface{}) string {
	return fmt.Sprintf("%012x", i)
}

func (c *Column) TableDir() string {

	return filepath.Join(c.Dir, c.Table)
}

type IdxWriter struct {
	IsNum        bool
	ValueEncoder func(r *Record) []string
}

func EncodeTri(s string) (result []string) {

	runes := []rune(s)

	for idx := range runes {
		if idx > len(runes)-3 {
			break
		}
		Log(LOG_DEBUG, "tri-gram %s\n", string(runes[idx:idx+3]))
		result = append(result,
			fmt.Sprintf("%04x%04x%04x", runes[idx], runes[idx+1], runes[idx+2]))
	}

	return
}

func (r *Record) caching(c *Column) {

	if r.cache != nil {
		return
	}

	data := r.Raw(c)
	if data == nil {
		Log(LOG_WARN, "fail got data r=%+v\n", r)
		return
	}
	fname, _ := c.Flist.FPath(r.fileID)
	decoder, e := GetDecoder(fname)
	if e != nil {
		Log(LOG_ERROR, "Record.caching():  cannot find %s decoder\n", fname)
		return
	}

	r.parse(data, decoder)

}

func (r *Record) Raw(c *Column) (data []byte) {

	path, err := c.Flist.FPath(r.fileID)
	if err != nil {
		Log(LOG_ERROR, "Flist.FPath fail e=%s r=%+v\n", err.Error(), r)
		return nil
	}

	f, err := os.Open(path)
	if err != nil {
		Log(LOG_WARN, "%s should remove from Column.Dirties \n", path)
		// FIXME: remove from Column.Dirties
		return nil
	}
	data = make([]byte, r.size)
	if n, e := f.ReadAt(data, r.offset); e != nil || int64(n) != r.size {
		Log(LOG_WARN, "%s is nvalid data: should remove from Column.Dirties \n", path)
		return nil
	}

	return data

}

func (r *Record) Uint64Value(c *Column) uint64 {
	v, ok := r.cache[c.Name].(uint64)
	_ = ok
	return uint64(v)
}

func (r *Record) StrValue(c *Column) string {
	v, ok := r.cache[c.Name].(string)
	_ = ok
	return v
}

func (r *Record) ToFbs(inf interface{}) []byte {

	key, ok := inf.(uint64)
	if !ok {
		return nil
	}

	root := query.NewRoot()
	root.SetVersion(query.FromInt32(1))
	root.WithHeader()

	inv := query.NewInvertedMapNum()
	inv.SetKey(query.FromInt64(int64(key)))

	rec := query.NewRecord()
	rec.SetFileId(query.FromUint64(r.fileID))
	rec.SetOffset(query.FromInt64(r.offset))
	rec.SetSize(query.FromInt64(r.size))
	rec.SetOffsetOfValue(query.FromInt32(0))
	rec.SetValueSize(query.FromInt32(0))

	inv.SetValue(rec.CommonNode)

	root.SetIndexType(query.FromByte(byte(vfs_schema.IndexInvertedMapNum)))
	root.SetIndex(inv.CommonNode)

	// FIXME return io writer ?
	root.Merge()
	return root.R(0)
}

func fbsRecord(r io.Reader) *query.Record {
	root := query.Open(r, 512)
	return root.Index().InvertedMapNum().Value()
}

func RecordFromFbs(r io.Reader) *Record {

	rec := fbsRecord(r)

	return &Record{fileID: rec.FileId().Uint64(),
		offset: rec.Offset().Int64(),
		size:   rec.Size().Int64(),
	}
}

func (c *Column) caching() (e error) {
	e = c.loadIndex()
	// if e == nil {
	// 	return
	// }

	if c.IsNumViaIndex() {
		c.IsNum = true
	}
	if c.IsNum {
		e = c.cachingNum()
	} else {
		e = c.cachingTri()
		return
	}
	return
}
func (c *Column) cachingTri() (e error) {
	idxCh, pat, e := c.noMergedFPathWithPat()
	if e != nil {
		return e
	}
	cnt := 0
	for _ = range idxCh {
		cnt++
	}
	if cnt == 0 {
		Log(LOG_WARN, "cachingTri(): %s is not found\n", pat)
		return ErrNotFoundFile
	}

	c.cache.caches.pat = pat

	c.cache.caches.cnt = cnt
	c.cache.caches.datas = make([]*IdxCache, c.cache.caches.cnt)
	datafirst, e := c.cachingTriBy(0)
	if e != nil {
		return e
	}
	c.cache.caches.datas[0] = datafirst
	last := c.cache.caches.cnt - 1
	c.cache.caches.datas[last], _ = c.cachingTriBy(last)

	return nil
}

func (c *Column) cachingTriBy(n int) (ic *IdxCache, e error) {

	idxCh, e := c.noMergedFPath()
	if e != nil {
		return nil, e
	}
	var idxpath string
	cnt := 0
	for s := range idxCh {
		if cnt == n {
			idxpath = s
			break
		}
		cnt++
	}
	if len(idxpath) == 0 {
		Log(LOG_WARN, "cachingTriBy(%d):  not found\n", n)
		return nil, ErrNotFoundFile
	}

	//FIXME: use idxPath2Info()
	strs := strings.Split(filepath.Base(idxpath), ".")
	if len(strs) != 6 {
		return nil, ErrInvalidIdxName
	}
	sRange := strs[3]
	fileID, _ := strconv.ParseUint(strs[4], 16, 64)
	offset, _ := strconv.ParseInt(strs[5], 16, 64)

	strs = strings.Split(sRange, "-")
	first, _ := strconv.ParseUint(strs[0], 16, 64)
	last, _ := strconv.ParseUint(strs[1], 16, 64)

	return &IdxCache{FirstEnd: Range{first: first, last: last},
		Pos: RecordPos{fileID: fileID, offset: offset}}, nil

}

func (c *Column) cachingNum() (e error) {

	path := ColumnPathWithStatus(c.TableDir(), c.Name, true, "*", "*", RECORD_WRITTEN)
	pat := fmt.Sprintf("%s.*.*", path)
	//idxfiles, err := filepath.Glob(pat)
	idxCh := paraGlob(pat)
	cnt := 0
	for _ = range idxCh {
		cnt++
	}
	if cnt == 0 {
		Log(LOG_WARN, "%s is not found\n", pat)
		return ErrNotFoundFile
	}

	c.cache.caches.pat = pat
	c.cache.caches.cnt = cnt
	c.cache.caches.datas = make([]*IdxCache, c.cache.caches.cnt)
	if c.cache.caches.datas[0], e = c.cachingNumBy(0); e != nil {
		return e
	}
	last := c.cache.caches.cnt - 1
	c.cache.caches.datas[last], _ = c.cachingNumBy(last)

	return nil
}

func (c *Column) cachingNumBy(n int) (ic *IdxCache, e error) {

	pat := c.cache.caches.pat
	//fmt.Sprintf("%s.*.*", path)

	idxCh := paraGlob(pat)
	var idxpath string
	cnt := 0
	for s := range idxCh {
		if cnt == n {
			idxpath = s
			break
		}
		cnt++
	}
	if len(idxpath) == 0 {
		Log(LOG_WARN, "%s is not found\n", pat)
		return nil, ErrNotFoundFile
	}

	strs := strings.Split(filepath.Base(idxpath), ".")
	if len(strs) != 6 {
		return nil, ErrInvalidIdxName
	}
	sRange := strs[3]
	fileID, _ := strconv.ParseUint(strs[4], 16, 64)
	offset, _ := strconv.ParseInt(strs[5], 16, 64)

	strs = strings.Split(sRange, "-")
	first, _ := strconv.ParseUint(strs[0], 16, 64)
	last, _ := strconv.ParseUint(strs[1], 16, 64)

	return &IdxCache{FirstEnd: Range{first: first, last: last},
		Pos: RecordPos{fileID: fileID, offset: offset}}, nil

}

func (c *Column) keys(n int) (uint64, uint64) {
	if len(c.cache.infos) > 0 {
		pos := c.head(n)
		return pos, pos
	}
	return c.getRowCache(n).FirstEnd.first, c.getRowCache(n).FirstEnd.last
	//return c.cache.caches[n].FirstEnd.first, c.cache.caches[n].FirstEnd.last
}

func (c *Column) cacheToRecords(n int) (record []*Record) {

	if len(c.cache.infos) == 0 {
		return []*Record{c.cacheToRecord(n)}
	}

	if c.cache.countInInfos() <= n {
		return []*Record{c.cacheToRecord(n - c.cache.countInInfos())}
	}

	//first := c.cache.head(n)
	return c.cache.records(n)

}
func (c *Column) cacheToRecord(n int) *Record {

	first := c.getRowCache(n).FirstEnd.first
	last := c.getRowCache(n).FirstEnd.last

	path := ColumnPathWithStatus(c.TableDir(), c.Name, c.IsNum, toFname(first), toFname(last), RECORD_WRITTEN)
	if !c.IsNum {
		path = ColumnPathWithStatus(c.TableDir(), c.Name, c.IsNum, toFnameTri(first), toFnameTri(last), RECORD_WRITTEN)
	}

	path = fmt.Sprintf("%s.%010x.%010x", path, c.getRowCache(n).Pos.fileID, c.getRowCache(n).Pos.offset)

	rio, e := os.Open(path)
	if e != nil {
		//spew.Dump(c.cache.caches)
		Log(LOG_WARN, "Column.cacheToRecord(): %s column index file not found\n", path)
		return nil
	}
	record := RecordFromFbs(rio)
	rio.Close()
	return record
}

type RecordPos struct {
	fileID uint64
	offset int64
}

type IdxCache struct {
	FirstEnd Range
	Pos      RecordPos
}

type IdxCaches struct {
	infos     []*IdxInfo
	caches    RowIndex
	negatives []*Range
}

type RowIndex struct {
	cnt      int
	pat      string
	firstEnd Range
	datas    []*IdxCache
}

type IdxInfo struct {
	path  string
	first uint64
	last  uint64
	buf   []byte
}

func (c *Column) getRowCache(n int) *IdxCache {
	r := &c.cache.caches
	if r.datas[n] != nil {
		return r.datas[n]
	}
	if c.IsNum {
		r.datas[n], _ = c.cachingNumBy(n)
	} else {
		r.datas[n], _ = c.cachingTriBy(n)
	}
	return r.datas[n]
}

func (info *IdxInfo) load(force bool) {

	if !force && len(info.buf) > 0 {
		return
	}
	f, e := os.Open(info.path)
	if e != nil {
		return
	}
	info.buf, e = ioutil.ReadAll(f)
}

func (col *Column) head(n int) uint64 {
	c := col.cache
	if c.countInInfos() <= n {
		return col.getRowCache(n - c.countInInfos()).FirstEnd.first
	}
	cur := 0

	for _, info := range c.infos {
		info.load(false)
		root := query.OpenByBuf(info.buf)
		if cur+root.Index().IndexNum().Indexes().Count() <= n {
			cur += root.Index().IndexNum().Indexes().Count()
			continue
		}
		pos := n - cur //cur + root.Index().IndexNum().Indexes().Count() - n

		return query.KeyRecordSingle(root.Index().IndexNum().Indexes().At(pos)).Key().Uint64()
	}
	return 0
}

// func (c *Column) cntOfValue(info *InfoRange) int {
// 	// if c.cache.countInInfos() <= n {
// 	// 	return int(c.cache.caches[n-c.cache.countInInfos()].FirstEnd.last + 1 - c.cache.caches[n-c.cache.countInInfos()].FirstEnd.first)
// 	// }

// 	if info.end <= c.cache.countInInfos() {
// 		return c.cntOfValueOnMerged(info)
// 	} else if info.start > c.cache.countInInfos() {
// 		return info.end - info.start
// 	}

// 	return c.cntOfValue(&InfoRange{start: info.start, end: c.cache.countInInfos() - 1}) +
// 		info.end - c.cache.countInInfos()
// }

// func (c *Column) cntOfValueOnMerged(info *InfoRange) int {

// 	return 0

// }

func (c *Column) cRecordlist(n int) (records *query.RecordList) {
	if c.cache.countInInfos() <= n {
		i := n - c.cache.countInInfos()
		r := c.cacheToRecord(i)
		//cur := c.cache.caches[n-c.cache.countInInfos()]
		//r :=
		rec := query.NewRecord()
		rec.SetFileId(query.FromUint64(r.fileID))
		rec.SetOffset(query.FromInt64(r.offset))
		rec.SetSize(query.FromInt64(r.size))
		rec.SetOffsetOfValue(query.FromInt32(0))
		rec.SetValueSize(query.FromInt32(0))

		records := query.NewRecordList()
		records.SetAt(0, rec)
		return records
	}

	//i := n - c.cache.countInInfos()

	return c.cache.recordlist(n)

}

func (c *IdxCaches) recordlist(n int) (records *query.RecordList) {

	cur := 0
	for _, info := range c.infos {
		info.load(false)
		root := query.OpenByBuf(info.buf)
		if cur+root.Index().IndexNum().Indexes().Count() <= n {
			cur += root.Index().IndexNum().Indexes().Count()
			continue
		}
		//pos := cur + root.Index().IndexNum().Indexes().Count() - n
		pos := n - cur
		//recods = make([]*Record,
		return query.KeyRecordSingle(root.Index().IndexNum().Indexes().At(pos)).Records()
	}
	return nil

}

func (c *IdxCaches) records(n int) (records []*Record) {

	list := c.recordlist(n)
	records = make([]*Record, list.Count())

	for i, rec := range list.All() {
		//buf := rec.R(rec.Node.Pos)
		records[i] = &Record{
			fileID: rec.FileId().Uint64(),
			offset: rec.Offset().Int64(),
			size:   rec.Size().Int64(),
		}
	}
	return
}

func (c *IdxCaches) countInInfos() int {

	cnt := 0
	for _, info := range c.infos {
		info.load(false)
		root := query.OpenByBuf(info.buf)
		cnt += root.Index().IndexNum().Indexes().Count()
	}
	return cnt

}

func (c *IdxCaches) countOfKeys() int {
	if len(c.infos) == 0 {
		return c.caches.cnt
	}

	return c.countInInfos() + c.caches.cnt

}

func InitIdxCaches(i *IdxCaches) {
	//i.caches = make([]*IdxCache, 0, MAX_IDX_CACHE)
	i.negatives = make([]*Range, 0, MIN_NEGATIVE_CACHE)
}

func NewIdxCaches() *IdxCaches {
	i := &IdxCaches{}
	InitIdxCaches(i)
	return i
}

type SearchMode byte

const (
	SEARCH_INIT SearchMode = iota
	SEARCH_START
	SEARCH_ASC
	SEARCH_DESC
	SEARCH_ALL
	SEARCH_FINISH
)

// Searcher ... return Search object for search operation
func (c *Column) Searcher() *Searcher {
	//if len(c.cache.caches) == 0 {
	if c.cache.countOfKeys() == 0 || c.cache.caches.cnt == 0 {
		c.ctx, c.ctxCancel = context.WithTimeout(context.Background(), 1*time.Minute)
		c.caching()
	} else if c.ctx == nil && c.isMergeOnSearch {
		c.ctx, c.ctxCancel = context.WithTimeout(context.Background(), 1*time.Minute)
		go c.MergingIndex(c.ctx)
		time.Sleep(200 * time.Millisecond)
	}
	return &Searcher{
		c:    c,
		low:  0,
		high: c.cache.countOfKeys() - 1,
		mode: SEARCH_INIT,
	}

}

type Searcher struct {
	c    *Column
	low  int
	high int
	cur  int
	mode SearchMode
}

func (s *Searcher) Do() <-chan *Record {

	ch := make(chan *Record, 10)
	go func() {
		for s.low <= s.high {
			s.cur = (s.low + s.high) / 2

		}

	}()
	return ch

}

func (s *Searcher) start(fn func(*Record, uint64) bool) {

	firstKey, _ := s.c.keys(s.low)
	_, lastKey := s.c.keys(s.high)

	frec := s.c.cacheToRecords(s.low)[0]
	lrec := s.c.cacheToRecords(s.high)[len(s.c.cacheToRecords(s.high))-1]
	first := fn(frec, firstKey)
	last := fn(lrec, lastKey)

	if first && !last {
		s.mode = SEARCH_DESC
		s.cur = s.low
		return
	}
	if !first && last {
		s.mode = SEARCH_ASC
		s.cur = s.high
		return
	}
	s.mode = SEARCH_ALL
	return

}

type OldSearchResult map[string]interface{}
type SearchResult string

type KeyRecord struct {
	key    uint64
	record *Record
}

type ResultInfoRange struct {
	s     *Searcher
	start int
	end   int
}

func (info ResultInfoRange) Start() OldSearchResult {

	s := info.s
	recods := info.s.c.cacheToRecords(info.start)
	r := recods[0]

	r.caching(s.c)
	return r.cache
}

func (info ResultInfoRange) Last() OldSearchResult {

	s := info.s
	records := info.s.c.cacheToRecords(info.end)
	r := records[len(records)-1]

	r.caching(s.c)
	return r.cache
}

var GlobCache map[string][]string = map[string][]string{}

func paraGlobCache(pat string) <-chan string {

	ch := make(chan string, 100)
	go func() {
		for _, path := range GlobCache[pat] {
			ch <- path
		}
		close(ch)
	}()
	return ch
}

func paraGlob(pat string) <-chan string {
	if _, found := GlobCache[pat]; found {
		return paraGlobCache(pat)
	}

	ch := make(chan string, 100)
	go func() {
		GlobCache = map[string][]string{}
		GlobCache[pat] = make([]string, 0, 10)

		strs := strings.Split(pat, "/*/*/*/")
		var dir, after string
		if len(strs) < 2 {
			dir = filepath.Dir(pat)
			after = filepath.Base(pat)
		} else {
			dir = strs[0]
			after = strs[1]
		}
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}
			validname := regexp.MustCompile(after)
			if validname.MatchString(path) {
				GlobCache[pat] = append(GlobCache[pat], path)
				ch <- path
			}
			return nil
		})
		if err != nil {
			Log(LOG_WARN, "paraGlob() err=%s", err)
		}
		close(ch)
	}()
	return ch
}
