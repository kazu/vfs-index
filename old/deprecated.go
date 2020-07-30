package vfsindex

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/kazu/loncha"
	. "github.com/kazu/vfs-index"
	"github.com/kazu/vfs-index/expr"
	"github.com/kazu/vfs-index/query"
)

// Init ... initialize Match
func (m Match) Init() {
	// m.col = ""
	// m.mapInf = make(map[string]interface{})
	// m.FirstCol = make(chan string, 2)
}

// Get ... get attribute/field .. for use search filter.
func (m Match) Get(k string) interface{} {

	if m.s.c.Name == k {
		return m.rec.key
	}
	m.rec.record.caching(m.s.c)

	return m.rec.record.cache[k]
}

// Uint64 ...  return key on field/attribute is index key
func (m Match) Uint64(k string) uint64 {

	if m.s.c.Name == k {
		return m.rec.key
	}
	m.rec.record.caching(m.s.c)

	return m.rec.record.cache[k].(uint64)
}

type MatchOps map[string]func(uint64, uint64) bool

var matchOps MatchOps = MatchOps{
	"==": func(s, d uint64) bool { return s == d },
	"<=": func(s, d uint64) bool { return s <= d },
	"<":  func(s, d uint64) bool { return s < d },
	">=": func(s, d uint64) bool { return s >= d },
	">":  func(s, d uint64) bool { return s > d },
}

func (m Match) Op(col, op, v string) bool {

	sv := m.Uint64(col)
	dv, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return false
	}
	return matchOps[op](sv, dv)
}

func (m Match) opByUint64(col, op string, dv uint64) bool {
	sv := m.Uint64(col)
	return matchOps[op](sv, dv)
}

// SearchVal ... convert tri-utf8 to uint64 ( mb4 not supported)
func SearchVal(s string) uint64 {
	tri, _ := strconv.ParseUint(EncodeTri(s)[0], 16, 64)
	return tri
}

// SearchInfo ... search result information. this is use by conditon chain.
type SearchInfo struct {
	s       *Searcher
	befores []InfoRange
	infos   []InfoRange
}

type InfoRange struct {
	start int
	end   int
}

// Select ...  set condition for searching
func (sinfo *SearchInfo) Select(fn func(Match) bool) (result *SearchInfo) {

	s := sinfo.s
	s.mode = SEARCH_INIT
	result = &SearchInfo{s: s}

	sinfo.befores = sinfo.infos

	if s.mode == SEARCH_INIT {
		s.mode = SEARCH_START
		s.start(func(r *Record, key uint64) bool {
			if r == nil {
				Log(LOG_ERROR, "cannot load record!!!\n")
				return false
			}
			r.caching(s.c)
			return fn(Match{s: s, rec: KeyRecord{key: key, record: r}})
		})
	}

	switch s.mode {
	case SEARCH_ALL:
		result.infos = append(result.infos, sinfo.findall(fn)...)
		min, max := -1, 0

		for _, info := range result.infos {
			if max < info.end {
				max = info.end
			}
			if min == -1 || min > info.start {
				min = info.start
			}
		}
		result.s.low = min
		result.s.high = max

	case SEARCH_ASC, SEARCH_DESC:
		result.infos = append(result.infos, sinfo.bsearch(fn))
		result.befores = sinfo.infos
		result.s.low = result.infos[0].start
		result.s.high = result.infos[0].end
	}
	//FIXME set s  and s.high s.low

	return result
}

func (sinfo *SearchInfo) findall(fn func(Match) bool) (result []InfoRange) {

	s := sinfo.s
	if s.mode != SEARCH_ALL {
		return nil
	}

	for s.cur = s.low; s.cur <= s.high; s.cur++ {
		IsInclude := func() bool {
			return loncha.Contain(sinfo.befores, func(i int) bool {
				return sinfo.befores[i].start <= s.cur && s.cur <= sinfo.befores[i].end
			})
		}

		if len(sinfo.befores) > 0 && !IsInclude() {
			continue
		}

		if s.mode == SEARCH_FINISH {
			break
		}
		key, _ := s.c.keys(s.cur)
		for _, r := range s.c.cacheToRecords(s.cur) {
			kr := KeyRecord{key: key, record: r}
			if fn(Match{s: sinfo.s, rec: kr}) {
				result = append(result,
					InfoRange{start: s.cur, end: s.cur})
				break
			}
		}

	}
	s.mode = SEARCH_FINISH
	return
}

func (sinfo *SearchInfo) bsearch(fn func(Match) bool) (info InfoRange) {

	s := sinfo.s
	checked := map[int]bool{}
	checked[s.cur] = true

	for {
		if s.mode == SEARCH_FINISH {
			break
		}

		s.cur = (s.low + s.high) / 2
		//Log(LOG_DEBUG, "ASC low=%d cur=%d high=%d\n", s.low, s.cur, s.high)
		r := s.c.cacheToRecords(s.cur)[0]
		key, _ := s.c.keys(s.cur)

		if fn(Match{s: s, rec: KeyRecord{key: key, record: r}}) {

			checked[s.cur] = true
			if s.mode == SEARCH_ASC {
				s.high = s.cur
			}
			if s.mode == SEARCH_DESC {
				s.low = s.cur
			}
		} else {
			checked[s.cur] = false
			if s.mode == SEARCH_ASC {
				s.low = s.cur
			}
			if s.mode == SEARCH_DESC {
				s.high = s.cur
			}
		}
		if s.high <= s.low+1 {
			//s.mode = SEARCH_FINISH
			break
		}
	}

	//info := ResultInfoRange{}
	if s.mode == SEARCH_ASC {
		info.start = s.high
		info.end = sinfo.befores[len(sinfo.befores)-1].end
	}
	if s.mode == SEARCH_DESC {
		info.start = sinfo.befores[0].start
		info.end = s.low
	}
	return info

}

// ToMapInf ... convert Record to map[string]interface . this is used mainly by search result.
func (s *SearchCond) ToMapInf(r *Record) map[string]interface{} {

	r.caching(s.idxCol)
	return r.cache
}

// ToJsonStr ... convert Record to json string . this is used mainly by search result.
func (s *SearchCond) ToJsonStr(r *Record) string {

	c := s.idxCol

	r.caching(s.idxCol)
	fname, _ := c.Flist.FPath(r.fileID)
	encoder, _ := GetDecoder(fname)

	raw, _ := encoder.Encoder(c.cache)

	return string(raw)
}

// ToJsonStrs ... convert slice of Record to slice of json string . this is used mainly by search result.
func (s *SearchCond) ToJsonStrs(rlist []*Record) (result []string) {

	c := s.idxCol
	r := rlist[0]
	r.caching(s.idxCol)
	fname, _ := c.Flist.FPath(r.fileID)
	enc, _ := GetDecoder(fname)

	for _, r := range rlist {
		r.caching(s.idxCol)
		raw, _ := enc.Encoder(r.cache)
		result = append(result, string(raw))
	}
	return

}

// ToMapInfs ... convert slice of Record to slice of map[string]interface . this is used mainly by search result.
func (s *SearchCond) ToMapInfs(rlist []*Record) (result []map[string]interface{}) {

	for _, r := range rlist {
		r.caching(s.idxCol)
		result = append(result, r.cache)
	}
	return
}

// All ... return records of all result
func (sinfo *SearchInfo) All() (result []*Record) {

	result = []*Record{}

	s := sinfo.s
	for _, info := range sinfo.infos {
		for cur := info.start; cur <= info.end; cur++ {
			for _, r := range s.c.cacheToRecords(cur) {
				//r.caching(s.c)
				result = append(result, r)

			}
		}
	}
	return
}

// Keys ... return keys of all result
func (sinfo *SearchInfo) Keys() (result []uint64) {

	s := sinfo.s
	for _, info := range sinfo.infos {
		for cur := info.start; cur <= info.end; cur++ {
			fKey, lKey := s.c.keys(cur)
			result = append(result, fKey)
			if fKey != lKey {
				result = append(result, lKey)
			}
		}
	}
	return
}

// FIXME
// func (sinfo *SearchInfo) Limit(n int) (result *SearchInfo) {
// 	result = sinfo.Copy()
// 	result.befores = sinfo.infos
// }

// First ... return first match Record
func (sinfo *SearchInfo) First() (result *Record) {

	s := sinfo.s
	cur := sinfo.infos[0].start
	r := s.c.cacheToRecords(cur)[0]
	//r.caching(s.c)

	return r
}

func (sinfo *SearchInfo) last() (result *Record) {

	// defer func() {
	// 	s := sinfo.s
	// 	if s.c.ctx != nil {
	// 		s.c.ctxCancel()
	// 	}
	// }()

	s := sinfo.s
	cur := sinfo.infos[len(sinfo.infos)-1].end
	//key, _ := s.c.keys(cur)
	records := s.c.cacheToRecords(cur)
	r := records[len(records)-1]
	//r.caching(s.c)

	return r
}

// And ... and boolean operation between search results
func (sinfo *SearchInfo) And(dinfo *SearchInfo) *SearchInfo {

	s := sinfo.s

	expandInfo := func(sinfo *SearchInfo) {
		ninfos := []InfoRange{}
		for _, info := range sinfo.infos {
			for i := info.start; i <= info.end; i++ {
				ninfos = append(ninfos, InfoRange{start: i, end: i})
			}
		}
		sinfo.infos = ninfos
	}

	fileIDIsContains := func(src, dst *query.RecordList) bool {
		exists := map[uint64]bool{}
		for i := 0; i < int(src.VLen()); i++ {
			//for i := 0; i < src.Count(); i++ {
			exists[query.RecordSingle(src.At(i)).FileId().Uint64()] = true
		}

		for i := 0; i < int(dst.VLen()); i++ {
			//for i := 0; i < dst.Count(); i++ {
			if _, found := exists[query.RecordSingle(dst.At(i)).FileId().Uint64()]; found {
				return true
			}
		}
		return false
	}
	expandInfo(sinfo)
	expandInfo(dinfo)

	loncha.Delete(&sinfo.infos, func(i int) bool {
		icur := sinfo.infos[i].start
		return !loncha.Contain(dinfo.infos, func(j int) bool {
			jcur := dinfo.infos[j].start
			return fileIDIsContains(s.c.cRecordlist(icur), s.c.cRecordlist(jcur))
		})
	})

	//sinfo.infos = infos
	return sinfo
}

func (sinfo *SearchInfo) smallerMatch(s string) *SearchInfo {

	var b strings.Builder
	var n strings.Builder
	runes := []rune(s)
	for i := 0; i < 3; i++ {
		if i < len([]rune(s)) {
			fmt.Fprintf(&b, "%04x", runes[i])
			fmt.Fprintf(&n, "%04x", runes[i])
			continue
		}
		fmt.Fprintf(&b, "%04x", 0)
		fmt.Fprintf(&n, "ffff")
	}
	sval, _ := strconv.ParseUint(b.String(), 16, 64)
	nval, _ := strconv.ParseUint(n.String(), 16, 64)

	return sinfo.Copy().Select(func(m Match) bool {
		return m.Uint64(sinfo.s.c.Name) <= nval
	}).Select(func(m Match) bool {
		return m.Uint64(sinfo.s.c.Name) > sval
	})

}

/* Query .. Search by query string

to search by number. (id is column/attribute name)
  "id == 1234"
  "id <= 2234"
to search string (exp. name is attribute name)
   "name.search(hogehoge)"

*/
func (sinfo *SearchInfo) Query(s string) *SearchInfo {
	q, err := expr.GetExpr(s)

	if err != nil {
		return sinfo.Select(func(m Match) bool {
			return false
		})
	}

	if q.Op == "search" && sinfo.s.c.Name == q.Column && !sinfo.s.c.IsNum {
		return sinfo.Match(q.Value)
	}

	if sinfo.s.c.Name == q.Column && !sinfo.s.c.IsNum {
		return sinfo.Match(q.Value)
	}
	if q.Op == "==" {
		return sinfo.Select(func(m Match) bool {
			return m.Op(q.Column, ">=", q.Value)
		}).Select(func(m Match) bool {
			return m.Op(q.Column, "==", q.Value)
		})
	}
	return sinfo.Select(func(m Match) bool {
		return m.Op(q.Column, q.Op, q.Value)
	})
}

func (sinfo *SearchInfo) FindByKey(k uint64) *SearchInfo {
	col := sinfo.s.c.Name
	return sinfo.Select(func(m Match) bool {
		return m.opByUint64(col, ">=", k)
	}).Select(func(m Match) bool {
		return m.opByUint64(col, "==", k)
	})
}

// Match ... set condition for string match
func (sinfo *SearchInfo) Match(s string) *SearchInfo {

	strs := []string{}

	if len([]rune(s)) < 3 {
		// var b strings.Builder
		// runes := []rune(s)
		// for i := 0; i < 3; i++ {
		// 	if i < len([]rune(s)) {
		// 		fmt.Fprintf(&b, "%04x", runes[i])
		// 		continue
		// 	}
		// 	fmt.Fprintf(&b, "%04x", 0)
		// }
		return sinfo.smallerMatch(s)
	} else {
		strs = EncodeTri(s)
	}

	rinfo := sinfo
	for i, str := range strs {
		sval, _ := strconv.ParseUint(str, 16, 64)
		tinfo := sinfo.Copy().Select(func(m Match) bool {
			return m.Uint64(sinfo.s.c.Name) < sval+1
		}).Select(func(m Match) bool {
			return m.Uint64(sinfo.s.c.Name) > sval-1
		})
		if i == 0 {
			rinfo = tinfo
		} else {
			rinfo = rinfo.And(tinfo)
		}
	}

	return rinfo
}

// Copy ... copy SearchInfo
func (info *SearchInfo) Copy() *SearchInfo {

	sinfo := &SearchInfo{}
	c := info.s.c
	sinfo.s = &Searcher{
		c:    c,
		low:  0,
		high: c.cache.countOfKeys() - 1,
		mode: SEARCH_INIT,
	}
	//s := sinfo.s
	sinfo.infos = []InfoRange{
		InfoRange{
			start: 0,
			end:   c.cache.countOfKeys() - 1,
		},
	}
	sinfo.befores = sinfo.infos
	sinfo.s.low = sinfo.befores[0].start
	sinfo.s.high = sinfo.befores[0].end
	return sinfo
}

func (cond *SearchCond) Select(fn func(SearchCondElem) bool) (sinfo *SearchFinder) {

	c := cond.idxCol
	idxFinder := OpenIndexFile(c) // c.TableDir()) //, c.Name, c.IsNumViaIndex()))
	//_ = idxFinder
	first := idxFinder.First()
	last := idxFinder.Last()

	sinfo = &SearchFinder{
		c:     c,
		idxs:  []*IndexFile{idxFinder},
		start: first.IdxInfo().first,
		last:  last.IdxInfo().last,
		mode:  SEARCH_INIT,
	}
	frec := first.FirstRecord()
	frec.caching(c)
	lrec := last.LastRecord()
	lrec.caching(c)

	keys := []uint64{}

	keyState := make(chan int, 2)

	setKey := func(origk interface{}) []uint64 {
		switch k := origk.(type) {
		case uint64:
			keys = append(keys, k)
		case int:
			keys = append(keys, uint64(k))
		case string:
			keys = append(keys, TriKeys(k)...)
		}
		keyState <- KeyStateGot
		return keys
	}
	getCol := func() *Column {
		return cond.idxCol
	}

	gotVal := func(col string, op CondOp) (sinfo *SearchFinder) {
		switch op {
		case CondOpEq:
			sinfo = cond.findBy(col, keys)
		case CondOpLe, CondOpLt:
			sinfo = cond.findBy(col, keys, findNearestFn(true))
		case CondOpGe, CondOpGt:
			sinfo = cond.findBy(col, keys, findNearestFn(true))
		}

		return sinfo
	}

	econd := SearchCondElem{setKey: setKey, Column: getCol, getValue: gotVal}

	var isTrue bool
	go func(cond SearchCondElem) {
		isTrue = fn(cond)
		keyState <- KeyStateFinish
	}(econd)

	for {
		state, ok := <-keyState
		if !ok || state == KeyStateFinish {
			break
		}
	}

	if isTrue {
		return
	}

	return EmptySearchFinder()
}

func (cond *SearchCond) Searcher() *SearchInfo {
	if cond.column == "" {
		return nil
	}
	sinfo := SearchInfo{}
	sinfo.s = cond.idxCol.Searcher()
	s := sinfo.s
	sinfo.infos = []InfoRange{
		InfoRange{
			start: 0,
			end:   s.c.cache.countOfKeys() - 1, ////len(s.c.cache.caches) - 1,
		},
	}
	sinfo.befores = sinfo.infos
	sinfo.s.low = sinfo.befores[0].start
	sinfo.s.high = sinfo.befores[0].end

	return &sinfo
}

// Deprecated: repplace SearchCondElem2
type SearchCondElem struct {
	setKey   SetKey
	Column   GetCol
	getValue GetValue
}

// Deprecated: repplace SearchCondElem2
func (cond SearchCondElem) Op(col, op string, v interface{}) (result bool) {

	if col != cond.Column().Name {
		return false
	}

	if op != "==" {
		return false
	}

	keys := cond.setKey(v)
	_ = keys
	finder := cond.getValue(col, StringOp[op])

	return finder != nil
}

// Deprecated:  no caching index in memory.
func (c *Column) loadIndex() error {
	if c.IsNumViaIndex() {
		c.IsNum = true
	}

	path := ColumnPathWithStatus(c.TableDir(), c.Name, c.IsNum, "*", "*", RECORD_MERGED)
	pat := fmt.Sprintf("%s", path)
	if !hasGlobCache(pat) {
		ch := paraGlob(pat)
		for range ch {
		}
	}

	//	cnt := globCacheInstance.Get(pat).Count()
	gCache := globCacheInstance.Get(pat)
	cnt := gCache.Count()

	for i := 0; i < cnt; i++ {
		file := string(query.PathInfoSingle(gCache.At(i)).Path().Bytes())
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

// Deprecated:
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
	Log(LOG_DEBUG, "cachingTri() start\n")
	defer Log(LOG_DEBUG, "cachingTri() done\n")
	pat := c.noMergedPat()

	if !hasGlobCache(pat) {
		ch, e := c.noMergedFPath()
		if e != nil {
			return e
		}
		for range ch {

		}
	}
	list := globCacheInstance.Get(pat)

	cnt := list.Count()
	if cnt == 0 {
		Log(LOG_WARN, "cachingTri(): %s is not found\n", pat)
		return ErrNotFoundFile
	}
	Log(LOG_WARN, "cachingTri(): %s is found, len=%d\n", pat, cnt)
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

	idxpath := string(query.PathInfoSingle(globCacheInstance.Get(c.noMergedPat()).At(n)).Path().Bytes())
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
	if !hasGlobCache(pat) {
		ch := paraGlob(pat)
		for range ch {

		}
	}
	cnt := globCacheInstance.Get(pat).Count()

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
	idxpath := string(query.PathInfoSingle(globCacheInstance.Get(c.noMergedPat()).At(n)).Path().Bytes())

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

// Match ... instance for match condition
type Match struct {
	s   *Searcher
	rec KeyRecord
}
