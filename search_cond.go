package vfsindex

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/kazu/loncha"
	"github.com/kazu/vfs-index/expr"
	"github.com/kazu/vfs-index/query"
)

// SearchCond .. saerch condition object.
type SearchCond struct {
	Err error

	idx    *Indexer
	flist  *FileList
	idxCol *Column

	m      Match
	table  string
	column string
	out    chan *Record
}

type mapInf map[string]interface{}

// type Match struct {
// 	key uint64
// 	col string
// 	mapInf
// 	FirstCol chan string
// 	done     chan string
// }

// Match ... instance for match condition
type Match struct {
	s   *Searcher
	rec KeyRecord
}

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

func (cond *SearchCond) startCol(col string) {
	cond.idxCol = cond.idx.OpenCol(cond.flist, cond.table, col)
	// e := cond.idxCol.caching()
	// if e != nil {
	// 	cond.flist.Reload()
	// 	cond.idxCol.Update(1 * time.Minute)
	// 	cond.idxCol.caching()
	// }
	cond.column = col
	cond.idxCol.IsNum = cond.idxCol.IsNumViaIndex()
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

// CancelAndWait ... wait for canceld backgraound routine( mainly merging index)
func (cond *SearchCond) CancelAndWait() {
	cond.idxCol.cancelAndWait()
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

func TriKeys(s string) (result []uint64) {

	//	var strs []string

	if len([]rune(s)) > 2 {
		result = make([]uint64, 0, len([]rune(s))-2)
		//strs = make([]string, 0. len([]rune(s)) -2)
	} else {
		result = make([]uint64, 0, 3)
		//strs = make([]string, 0. 3)
	}

	strs := EncodeTri(s)

	for _, str := range strs {
		val, _ := strconv.ParseUint(str, 16, 64)
		result = append(result, val)
	}

	return

}

func (cond *SearchCond) FindBy(col string, kInf interface{}) (sinfo *SearchFinder) {

	var keys []uint64
	switch k := kInf.(type) {
	case uint64:
		keys = append(keys, k)
	case string:
		keys = TriKeys(k)
	}

	return cond.findBy(col, keys)
}

func (cond *SearchCond) findNearest(col string, key []uint64, less bool) (sinfo *SearchFinder) {

	c := cond.idxCol
	idxFinder := OpenIndexFile(c)
	if c.Name != col {
		// FIXME: findAll()
		return EmptySearchFinder()
	}
	idxs := []*IndexFile{}
	for i := range key {
		idxs = append(idxs, idxFinder.FindNearByKey(key[i], less)...)
	}
	loncha.Delete(&idxs, func(i int) bool {
		return idxs[i] == nil
	})

	return
}

type findByOption func(*IndexFile, uint64) []*IndexFile

// FIXME: add routine for removing equal key
func findNearestFn(less bool) findByOption {
	return func(f *IndexFile, key uint64) []*IndexFile {
		results := f.FindNearByKey(key, less)
		// if len(results) == 0 || results[0] == nil {
		// 	return []*IndexFile{}
		// }
		// if less {
		// 	results[0].first = f.First().IdxInfo().first
		// }else{
		// 	results[0].
		// }
		return results
	}
}

func findEqFn() findByOption {
	return func(f *IndexFile, key uint64) []*IndexFile {
		return f.FindByKey(key)
	}
}

func (cond *SearchCond) findBy(col string, keys []uint64, fns ...findByOption) (sinfo *SearchFinder) {
	fn := findEqFn()
	if len(fns) > 0 {
		fn = fns[0]
	}

	c := cond.idxCol
	idxFinder := OpenIndexFile(c)

	if c.Name != col {
		// FIXME: findAll()
		return EmptySearchFinder()
	}

	key2searchFinder := func(key uint64) *SearchFinder {
		//idxs := idxFinder.FindByKey(key)
		idxs := fn(idxFinder, key)
		if len(idxs) == 0 || idxs[0] == nil {
			return nil
		}
		if idxs[0].IsType(IdxFileType_Write) {
			return &SearchFinder{
				c:     c,
				idxs:  idxs,
				mode:  SEARCH_ALL,
				start: key,
				last:  key,
			}
		}

		if idxs[0].IsType(IdxFileType_Merge) {
			if idxs[0].KeyRecords().Find(func(kr *query.KeyRecord) bool {
				return kr.Key().Uint64() == key
			}) == nil {
				return nil
			}
			return &SearchFinder{
				c:     c,
				idxs:  idxs,
				mode:  SEARCH_ALL,
				start: key,
				last:  key,
			}
		}
		return nil
	}

	for _, key := range keys {
		sinfo2 := key2searchFinder(key)
		if sinfo2 == nil {
			return EmptySearchFinder()
		}
		if sinfo == nil {
			sinfo = sinfo2
			continue
		}
		sinfo = sinfo.And(sinfo2)
	}
	if sinfo == nil {
		return &SearchFinder{
			isEmpty: true,
		}
	}
	if len(sinfo.matches) == 0 && len(keys) > 1 {
		sinfo.isEmpty = true
	}
	return sinfo
}

const (
	KeyStateGot int = 1
	// KeyStateRun int = 2
	// KeyStateFlase int = 3
	KeyStateFinish int = 4
)

func (f *SearchCond) Query(s string) (r *SearchFinder) {
	q, err := expr.GetExpr(s)
	c := f.idxCol
	c.IsNum = c.IsNumViaIndex()

	if err != nil {
		return EmptySearchFinder()
	}

	if q.Op == "search" && c.Name == q.Column && !c.IsNum {
		return f.FindBy(q.Column, q.Value)
	}
	if q.Op == "==" {
		if f.idxCol.IsNum {
			uintVal, _ := strconv.ParseUint(q.Value, 10, 64)
			return f.FindBy(q.Column, uintVal)
		}
		return f.FindBy(q.Column, q.Value)
	}
	return EmptySearchFinder()
}

func (f *SearchCond) Match(s string) *SearchFinder {
	c := f.idxCol
	return f.FindBy(c.Name, s)
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

func (cond *SearchCond) Select2(fn func(SearchCondElem2) bool) (sfinder *SearchFinder2) {

	c := cond.idxCol
	idxFinder := OpenIndexFile(c)
	first := idxFinder.First()
	last := idxFinder.Last()
	_, _ = first, last

	sfinder2 := NewSearchFinder2(cond.Column())
	// frec := first.FirstRecord()
	// frec.caching(c)
	// lrec := last.LastRecord()
	// lrec.caching(c)

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

	gotVal := func(col string, op CondOp) (sfind *SearchFinder2) {
		sfind = NewSearchFinder2(cond.Column())
		switch op {
		case CondOpEq:
			for i, key := range keys {
				if i == 0 {
					sfind.recordFns = append(sfind.recordFns, idxFinder.RecordByKey(key))
					sfind.skipdFns = append(sfind.skipdFns, EmptySkip)
				}
				lastIdx := len(sfind.recordFns) - 1
				sfind.skipdFns[lastIdx] = sfind.And(lastIdx, key)
			}
			//sinfo = cond.findBy(col, keys)
		case CondOpLe, CondOpLt:
			//sinfo = cond.findBy(col, keys, findNearestFn(true))
		case CondOpGe, CondOpGt:
			//sinfo = cond.findBy(col, keys, findNearestFn(true))
		}
		sfinder = sfind
		return sfind
	}

	econd := SearchCondElem2{setKey: setKey, Column: getCol, getValue: gotVal}

	var isTrue bool
	go func(cond SearchCondElem2) {
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

	return sfinder2
}

type CondOp byte

const (
	CondOpEq CondOp = iota
	CondOpLe
	CondOpLt
	CondOpGe
	CondOpGt
)

var StringOp map[string]CondOp = map[string]CondOp{
	"==": CondOpEq,
	"<=": CondOpLe,
	"<":  CondOpLt,
	">=": CondOpGe,
	">":  CondOpGe,
}

type SetKey func(interface{}) []uint64
type GetCol func() *Column
type GetValue func(string, CondOp) *SearchFinder
type GetValue2 func(string, CondOp) *SearchFinder2

// Deprecated: repplace SearchCondElem2
type SearchCondElem struct {
	setKey   SetKey
	Column   GetCol
	getValue GetValue
}

type SearchCondElem2 struct {
	setKey   SetKey
	Column   GetCol
	getValue GetValue2
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

func (cond SearchCondElem2) Op(col, op string, v interface{}) (result bool) {

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

func (cond *SearchCond) Column() *Column {
	return cond.idxCol
}

func (cond *SearchCond) StartMerging() {
	c := cond.idxCol
	//c.IsNum = c.IsNumViaIndex()
	if c.ctx == nil && c.isMergeOnSearch {
		c.ctx, c.ctxCancel = context.WithTimeout(context.Background(), Opt.mergeDuration)
		go c.MergingIndex(c.ctx)
		time.Sleep(200 * time.Millisecond)
	}
}

type ResultOpt func(*Column, *query.Record) interface{}

func ResultOutput(name string) ResultOpt {

	return func(c *Column, rec *query.Record) interface{} {
		r := &Record{
			fileID: rec.FileId().Uint64(),
			offset: rec.Offset().Int64(),
			size:   rec.Size().Int64(),
		}
		r.caching(c)
		if name == "json" {
			fname, _ := c.Flist.FPath(r.fileID)
			enc, _ := GetDecoder(fname)
			raw, _ := enc.Encoder(r.cache)
			return string(raw)
		}
		return r.cache
	}
}
