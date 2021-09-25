package vfsindex

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"crypto/sha1"

	"github.com/kazu/fbshelper/query/base"
	"github.com/kazu/loncha"
	"github.com/kazu/vfs-index/decompress"
	"github.com/kazu/vfs-index/query"
	"github.com/kazu/vfs-index/vfs_schema"
	"github.com/vbauerster/mpb/v5"
	"github.com/vbauerster/mpb/v5/decor"
)

type Range struct {
	first uint64
	last  uint64
}

type RangeCur struct {
	Range
	cur int
}

const (
	MAX_IDX_CACHE      = 512
	MIN_NEGATIVE_CACHE = 8
)

var GGlobCache map[string][]string = map[string][]string{}

func FileExist(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil
}

func FileMtime(filename string) time.Time {
	s, err := os.Stat(filename)
	if err != nil {
		return time.Time{}
	}
	return s.ModTime()
}

func EncodeTri(s string) (result []string) {

	runes := []rune(s)

	for idx := range runes {
		if idx > len(runes)-3 {
			break
		}
		//Log(LOG_DEBUG, "tri-gram %s\n", string(runes[idx:idx+3]))
		result = append(result,
			fmt.Sprintf("%04x%04x%04x", runes[idx], runes[idx+1], runes[idx+2]))
	}

	return
}

func DecodeTri(v uint64) (s string) {

	runes := make([]rune, 0, 3)
	for i := 4; i > -1; i -= 2 {
		runes = append(runes, rune((v>>(i*8))&0xffff))
	}
	return string(runes)
}

func toFname(i interface{}) string {
	return fmt.Sprintf("%010x", i)
}

func toFnameTri(i interface{}) string {
	return fmt.Sprintf("%012x", i)
}

type Decoder struct {
	FileType  string
	Decoder   func([]byte, interface{}) error
	Encoder   func(interface{}) ([]byte, error)
	Tokenizer func(context.Context, io.Reader, *File) <-chan *Record
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
		Tokenizer: func(ctx context.Context, rio io.Reader, f *File) <-chan *Record {
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
			b, e := json.Marshal(v)
			if e != nil {
				return b, e
			}
			var out bytes.Buffer
			json.Indent(&out, b, "", "\t")
			return out.Bytes(), e

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
		Tokenizer: func(ctx context.Context, rio io.Reader, f *File) <-chan *Record {
			ch := make(chan *Record, 100)
			go func() {
				dec := json.NewDecoder(rio)

				var rec *Record

				nest := int(0)
				defer close(ch)
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
}

// GetDecoder ... return format Decoder/Encoder from fname(file name)
func GetDecoder(fname string) (dec Decoder, e error) {
	if len(fname) < 1 {
		return dec, ErrInvalidTableName
	}
	ext, _ := decompress.GetFormat(fname)
	ext = ext[1:]

	cidx, e := loncha.IndexOf(Opt.customDecoders, func(i int) bool {
		return Opt.customDecoders[i].FileType == ext
	})
	if e == nil && cidx >= 0 {
		return Opt.customDecoders[cidx], nil
	}

	idx, e := loncha.IndexOf(DefaultDecoder, func(i int) bool {
		return DefaultDecoder[i].FileType == ext
	})
	if e != nil {
		Log(LOG_ERROR, "cannot find %s decoder\n", ext)
		return dec, e
	}
	return DefaultDecoder[idx], nil

}
func setDecoder(ext string, dec Decoder) (e error) {
	if len(ext) < 1 {
		return ErrParameterInvalid
	}

	idx, e := loncha.IndexOf(Opt.customDecoders, func(i int) bool {
		return Opt.customDecoders[i].FileType == ext
	})
	if e != nil {
		Opt.customDecoders[idx] = dec
		return nil
	} else {
		Opt.customDecoders = append(Opt.customDecoders, dec)
		return nil
	}
}

func globcachePath(pat string) string {
	strs := strings.Split(pat, "/*/*/*/")
	dir := strs[0]
	if len(strs) < 2 {
		dir = filepath.Dir(pat)
	}
	shasum := sha1.Sum([]byte(pat))

	hash := hex.EncodeToString(shasum[:])

	return filepath.Join(dir, hash+".globcache.fbs")

}

func storeGlobCache(pat string) error {
	path := globcachePath(pat)
	dataPath := globcachePath(pat) + ".dat"
	wPath := fmt.Sprintf("%s.%d", path, os.Getpid())
	wDataPath := fmt.Sprintf("%s.%d.dat", path, os.Getpid())

	var root query.Root
	if FileExist(path) {
		if e := SafeRename(path, wPath); e != nil {
			return e
		}
		f, e := os.Open(wPath)
		if e != nil {
			return e
		}
		//root = query.Open(f, 512)
		buf, _ := ioutil.ReadAll(f)
		f.Close()
		root = query.OpenByBuf(buf)
	} else {
		root = *(query.NewRoot())
		root.WithHeader()
		root.SetVersion(query.FromInt32(1))
	}

	dataf, _ := os.Create(wDataPath)
	defer dataf.Close()

	opathinfos := query.NewPathInfoList()
	if root.FieldAt(2).Node != nil && root.Index().IdxEntry().Pathinfos().Count() > 0 {
		opathinfos = root.Index().IdxEntry().Pathinfos()
	}

	opathinfo := query.NewPathInfo()
	opathinfo.SetPath(base.FromByteList([]byte(GGlobCache[pat][0])))
	opathinfos.SetAt(0, opathinfo)

	pathinfos := base.CommonList{}
	pathinfos.CommonNode = opathinfo.CommonNode
	pathinfos.SetDataWriter(dataf)
	pathinfos.WriteDataAll()

	for i, path := range GGlobCache[pat] {
		if i == 0 {
			continue
		}
		pathinfo := query.NewPathInfo()
		pathinfo.SetPath(base.FromByteList([]byte(path)))
		pathinfos.SetAt(i, pathinfo.CommonNode)
	}
	idxEntry := query.NewIdxEntry()
	pinfos := query.NewPathInfoList()
	pinfos.CommonNode = pathinfos.CommonNode
	//idxEntry.SetPathinfos(pathinfos)
	idxEntry.SetPathinfos(pinfos)
	root.SetIndexType(query.FromByte(byte(vfs_schema.IndexIdxEntry)))
	root.SetIndex(&query.Index{CommonNode: idxEntry.CommonNode})
	root.Merge()

	f, e := os.Create(wPath)
	if e != nil {
		return e
	}
	defer f.Close()
	f.Write(root.R(0))

	if e := SafeRename(wPath, path); e != nil {
		os.Remove(wPath)
		Log(LOG_DEBUG, "F: rename %s -> %s \n", wPath, path)
		return e
	}
	SafeRename(wDataPath, dataPath)

	return nil
}

func hasGlobCache(pat string) bool {
	path := globcachePath(pat)
	if !FileExist(path) {
		return false
	}
	return true
	// dt := FileMtime(tabledirFromPat(pat))
	// c, _ := getGlobCache(pat).At(0)
	// return FileMtime(string(c.Path().Bytes())).Before(dt)

}

func getGlobCache(pat string) *query.PathInfoList {
	if !hasGlobCache(pat) {
		return nil
	}
	path := globcachePath(pat)
	f, e := os.Open(path)
	if e != nil {
		return nil
	}
	buf, _ := ioutil.ReadAll(f)
	f.Close()
	root := query.OpenByBuf(buf)
	return root.Index().IdxEntry().Pathinfos()

}

func globCacheCh(pat string) <-chan string {
	c := getGlobCache(pat)
	Log(LOG_DEBUG, "globCacheCh(%s)\n", pat)
	ch := make(chan string, 100)
	go func() {
		for i := 0; i < c.Count(); i++ {
			pathinfo, _ := c.At(i)
			ch <- string(pathinfo.Path().Bytes())
		}
		close(ch)
	}()
	return ch

}

func paraGlobCache(pat string) <-chan string {

	Log(LOG_DEBUG, "paraGlobCache(%s)\n", pat)
	ch := make(chan string, 100)
	go func() {
		for _, path := range GGlobCache[pat] {
			ch <- path
		}
		close(ch)
	}()
	return ch
}
func paraGlob(pat string) <-chan string {
	return paraGlobDebug(pat, false)
}

func paraGlobDebug(pat string, isDebug bool) <-chan string {
	// if _, found := GlobCache[pat]; found {
	// 	return paraGlobCache(pat)
	// }
	if hasGlobCache(pat) {
		//return globCacheCh(pat)
		return globCacheInstance.GetCh(pat)
	}

	Log(LOG_DEBUG, "paraGlob(%s)\n", pat)

	ch := make(chan string, 512)
	go func() {
		// GlobCache = map[string][]string{}
		GGlobCache[pat] = make([]string, 0, 10)
		discoverChild := true

		strs := strings.Split(pat, "/*/*/*/")
		var dir, after string
		if len(strs) < 2 {
			dir = filepath.Dir(pat)
			after = filepath.Base(pat)
			discoverChild = false
		} else {
			dir = strs[0]
			after = strs[1]
			discoverChild = true
		}
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				if !discoverChild && dir != path {
					if isDebug {
						Log(LOG_DEBUG, "skip dir=%s path=%s\n", dir, path)
					}
					return filepath.SkipDir
				}
				return nil
			}
			validname := regexp.MustCompile(after)
			if validname.MatchString(path) {
				//GGlobCache[pat] = append(GGlobCache[pat], path)
				globCacheInstance.Add(pat, path)
				if isDebug {
					//Log(LOG_DEBUG, "paraGlob(%s) hit path=%s\n", pat, path)
				}
				ch <- path
			}
			return nil
		})
		if err != nil {
			Log(LOG_WARN, "paraGlob() err=%s\n", err)
		}
		//storeGlobCache(pat)
		globCacheInstance.Finish(pat)
		close(ch)
	}()
	return ch
}

func tableFromPat(pat string) string {
	return IdxPathInfo(pat).Table()
}

func tabledirFromPat(pat string) string {
	return IdxPathInfo(pat).TDir()
}

type IdxPathInfo string

func (p IdxPathInfo) Table() string {
	pat := string(p)
	top := Opt.rootDir
	rel, _ := filepath.Rel(top, pat)
	for {
		rel = filepath.Dir(rel)
		if rel == filepath.Base(rel) {
			break
		}
	}
	return rel
}

func (p IdxPathInfo) TDir() string {
	return filepath.Join(Opt.rootDir, p.Table())
}

func (p IdxPathInfo) IsMerged() bool {
	strs := strings.Split(filepath.Base(string(p)), ".")
	if strs[3] == "merged" {
		return true
	}
	return false
}

func (p IdxPathInfo) Info() (col string, isNum bool, first, last, fileID uint64, offset int64) {
	strs := strings.Split(filepath.Base(string(p)), ".")
	if len(strs) < 5 {
		return
	}

	col = strs[0]
	isNum = true
	if strs[1] != "num" {
		isNum = false
	}

	if p.IsMerged() {
		ranges := strings.Split(strs[4], "-")
		first, _ = strconv.ParseUint(ranges[0], 16, 64)
		last, _ = strconv.ParseUint(ranges[1], 16, 64)
	} else {
		fileID, _ = strconv.ParseUint(strs[4], 16, 64)
		offset, _ = strconv.ParseInt(strs[5], 16, 64)
		ranges := strings.Split(strs[3], "-")
		first, _ = strconv.ParseUint(ranges[0], 16, 64)
		last, _ = strconv.ParseUint(ranges[1], 16, 64)
	}
	return
}

// col, isNum, first, last, fileID , offset
func (s IdxPathInfo) Less(d IdxPathInfo) bool {
	col, isNum, first, last, fileID, offset := s.Info()
	_, _, _, _, _, _ = col, isNum, first, last, fileID, offset

	sfirst := first

	col, isNum, first, last, fileID, offset = s.Info()
	dfirst := first

	return sfirst < dfirst
}

func (s IdxPathInfo) Greater(d IdxPathInfo) bool {
	col, isNum, first, last, fileID, offset := s.Info()
	_, _, _, _, _, _ = col, isNum, first, last, fileID, offset
	slast := last
	col, isNum, first, last, fileID, offset = s.Info()
	dlast := last

	return slast > dlast
}

var Pbar ProgressBar = NewProgressBar()

type ProgressBar struct {
	wg   sync.WaitGroup
	base *mpb.Progress
}

func NewProgressBar(opts ...mpb.ContainerOption) (bar ProgressBar) {

	newopts := []mpb.ContainerOption{}

	newopts = append(newopts, mpb.WithWaitGroup(&bar.wg))
	newopts = append(newopts, mpb.WithOutput(os.Stderr))
	newopts = append(newopts, opts...)

	bar.base = mpb.New(newopts...)
	return
}

func (p *ProgressBar) Add(name string, total int) (bar *mpb.Bar) {
	p.wg.Add(1)
	return p.base.AddBar(int64(total),
		mpb.PrependDecorators(
			decor.Name(name, decor.WC{W: len(name) + 1, C: decor.DidentRight}),
			decor.OnComplete(
				decor.AverageETA(decor.ET_STYLE_GO, decor.WC{W: 4}), "done",
			),
		),
		mpb.AppendDecorators(decor.CountersNoUnit("%d / %d")),
	)
}

func (p *ProgressBar) Done() {
	p.wg.Done()
}

type BufWriterIO struct {
	*os.File
	buf []byte
	len int
	cap int
	pos int64
	l   int
}

func NewBufWriterIO(o *os.File, n int) *BufWriterIO {

	return &BufWriterIO{
		File: o,
		buf:  make([]byte, 0, n),
		pos:  int64(0),
		len:  0,
		cap:  n,
		l:    n,
	}
}

func (b *BufWriterIO) w() *os.File {
	return b.File
}

func (b *BufWriterIO) Write(p []byte) (n int, e error) {

	defer func() {
		b.len = len(b.buf)
		b.cap = cap(b.buf)
	}()

	b.buf = append(b.buf, p[:len(p):len(p)]...)
	if len(b.buf) >= b.l {
		n, e = b.w().WriteAt(b.buf, b.pos)
		if n < 0 || e != nil {
			return
		}
		b.pos += int64(n)
		b.buf = b.buf[0:0:cap(b.buf)]
	}
	return len(p), nil

}

func (b *BufWriterIO) WriteAt(p []byte, offset int64) (n int, e error) {

	defer func() {
		b.len = len(b.buf)
		b.cap = cap(b.buf)
	}()

	if (offset-b.pos)+int64(len(p)) > int64(cap(b.buf)) {
		nbuf := make([]byte, (offset-b.pos)+int64(len(p)))
		copy(nbuf[:len(b.buf)], b.buf)
		b.buf = nbuf
	}
	wpos := offset - b.pos
	copy(b.buf[wpos:wpos+int64(len(p))], p[:len(p):cap(p)])
	b.buf = b.buf[:wpos+int64(len(p))]

	if len(b.buf) >= b.l {
		n, e = b.w().WriteAt(b.buf, b.pos)
		if n < 0 || e != nil {
			return
		}
		b.pos += int64(n)
		b.buf = b.buf[0:0:cap(b.buf)]
	}
	return len(p), nil
}

func (b *BufWriterIO) Flush() (e error) {

	defer func() {
		b.len = len(b.buf)
		b.cap = cap(b.buf)
	}()
	var n int
	if len(b.buf) > 0 {
		n, e = b.w().WriteAt(b.buf, b.pos)
		b.pos += int64(n)
		b.buf = b.buf[n:n]
	}
	return
}
