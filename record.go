package vfsindex

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kazu/fbshelper/query/base"
	"github.com/kazu/vfs-index/decompress"
	"github.com/kazu/vfs-index/query"
	"github.com/kazu/vfs-index/vfs_schema"
)

type Record struct {
	fileID uint64
	offset int64
	size   int64
	cache  map[string]interface{}
}

type Records []*Record

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

func (r *Record) IsExist(c *Column) bool {

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
	//return r.write(c, idxWriter)

	if len(idxWriter.ValueEncoder(r)) == 0 {
		return true
	}

	w := idxWriter
	vName := w.ValueEncoder(r)[0]
	path := ColumnPathWithStatus(c.TableDir(), c.Name, w.IsNum, vName, vName, RECORD_WRITTEN)
	path = fmt.Sprintf("%s.%010x.%010x", path, r.fileID, r.offset)
	if FileExist(path) {
		return true
	}

	return false
}

// Write ... write column index
func (r *Record) Write(c *Column) error {

	var idxWriter IdxWriter
	r.caching(c)

	if r.cache[c.Name] == nil {
		return ErrNotHasColumn
	}

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

type IdxWriter struct {
	IsNum        bool
	ValueEncoder func(r *Record) []string
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

	f, e := decompress.Open(decompress.LocalFile(path))
	if e != nil {
		Log(LOG_ERROR, "Flist.FPath fail e=%s r=%+v\n", err.Error(), r)
		return nil
	}

	dec, _ := GetDecoder(path)
	if dec.FileType == "csv" {
		dummy := make(map[string]interface{})

		if e := dec.Decoder(data, &dummy); e == ErrMustCsvHeader {
			ctx, cancel := context.WithCancel(context.Background())
			<-dec.Tokenizer(ctx, f,
				&File{id: 123, name: path, index_at: time.Now().UnixNano()})
			cancel()
		}
		dummy = nil
		f.Close()
		f, _ = decompress.Open(decompress.LocalFile(path))
	}

	data = make([]byte, r.size)
	if n, e := f.ReadAt(data, r.offset); e != nil || int64(n) != r.size {
		Log(LOG_WARN, "%s is nvalid data: should remove from Column.Dirties \n", path)
		return nil
	}

	return data

}

func (r *Record) write(c *Column, w IdxWriter) error {

	for i, vName := range w.ValueEncoder(r) {

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
		debugStr := ""
		if c.IsNum {
			io.Write(r.ToFbs(r.Uint64Value(c)))
			if CurrentLogLoevel == LOG_DEBUG {
				debugStr = fmt.Sprintf("%d", r.Uint64Value(c))
			}
		} else {
			io.Write(r.ToFbs(TriKeys(r.StrValue(c))[i]))
			if CurrentLogLoevel == LOG_DEBUG {
				debugStr = fmt.Sprintf("%s(%s)",
					DecodeTri(TriKeys(r.StrValue(c))[i]), r.StrValue(c))

			}
		}
		io.Close()
		if !w.IsNum {
			Log(LOG_DEBUG, "S: record(id=%v, %d) %s(%v)(%d)=%s  written %s \n", r.fileID, r.offset,
				c.Name, TriKeys(r.StrValue(c))[i], i, debugStr,
				wPath)
		}
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
		Log(LOG_DEBUG, "S: renamed %s -> %s \n", wPath, path)

	}

	return nil
}

func (r *Record) parse(raw []byte, dec Decoder) {

	r.cache = make(map[string]interface{})

	e := dec.Decoder(raw, &r.cache)

	if e != nil {
		Log(LOG_ERROR, "e=%s Raw='%s' Record=%+v\n", e, strings.ReplaceAll(string(raw), "\n", ""), r.cache)
		return
	}

	return
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

	inv.SetValue(rec)

	root.SetIndexType(query.FromByte(byte(vfs_schema.IndexInvertedMapNum)))

	dupFn := func(src *base.BaseImpl, srcOff int, size int) []byte {

		invBuf := make([]byte, size)

		for srcPtr := srcOff; srcPtr < srcOff+size; {
			data := src.R(srcPtr, base.Size(size-(srcPtr-srcOff)))
			if len(data) == 0 {
				panic("hoge")
			}
			copy(invBuf[srcPtr-srcOff:], data)

			srcPtr += len(data)
		}
		return invBuf
	}

	_ = dupFn

	root.SetIndex(&query.Index{CommonNode: inv.CommonNode})

	root.Flatten()
	return root.R(0, base.Size(root.LenBuf()))
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

func IsEqQRecord(src, dst *query.Record) bool {
	return src.FileId().Uint64() == dst.FileId().Uint64() &&
		src.Offset().Int64() == dst.Offset().Int64()
}
