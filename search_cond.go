package vfsindex

import (
	"strconv"

	"github.com/kazu/loncha"
)

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

type Match struct {
	s   *Searcher
	rec KeyRecord
}

func (m Match) Init() {
	// m.col = ""
	// m.mapInf = make(map[string]interface{})
	// m.FirstCol = make(chan string, 2)
}

func (m Match) Get(k string) interface{} {

	if m.s.c.Name == k {
		return m.rec.key
	}
	m.rec.record.caching(m.s.c)

	return m.rec.record.cache[k]
}

func (m Match) Uint64(k string) uint64 {

	if m.s.c.Name == k {
		return m.rec.key
	}
	m.rec.record.caching(m.s.c)

	return m.rec.record.cache[k].(uint64)
}

// func (m Match) SearchEq(k string, tri uint64) bool {

// 	return m.key == tri
// }

// func (m Match) Search(k string, tri uint64) bool {

// 	return m.key <= tri
// }

func SearchVal(s string) uint64 {
	tri, _ := strconv.ParseUint(EncodeTri(s)[0], 16, 64)
	return tri
}

func (cond *SearchCond) StartCol(col string) {
	cond.idxCol = cond.idx.OpenCol(cond.flist, cond.table, col)
	cond.idxCol.caching()
	cond.column = col
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
			end:   len(s.c.cache.caches) - 1,
		},
	}
	sinfo.befores = sinfo.infos
	sinfo.s.low = sinfo.befores[0].start
	sinfo.s.high = sinfo.befores[0].end

	return &sinfo
}

type SearchInfo struct {
	s       *Searcher
	befores []InfoRange
	infos   []InfoRange
}

type InfoRange struct {
	start int
	end   int
}

func (sinfo *SearchInfo) Select(fn func(Match) bool) (result *SearchInfo) {

	s := sinfo.s
	s.mode = SEARCH_INIT
	result = &SearchInfo{s: s}

	sinfo.befores = sinfo.infos

	if s.mode == SEARCH_INIT {
		s.mode = SEARCH_START
		s.Start(func(r *Record, key uint64) bool {
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
		r := s.c.cacheToRecord(s.cur)
		kr := KeyRecord{key: key, record: r}
		if fn(Match{s: sinfo.s, rec: kr}) {
			result = append(result,
				InfoRange{start: s.cur, end: s.cur})
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
		r := s.c.cacheToRecord(s.cur)
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

func (sinfo *SearchInfo) All() (result []SearchResult) {

	result = []SearchResult{}
	s := sinfo.s

	for _, info := range sinfo.infos {
		for cur := info.start; cur <= info.end; cur++ {
			//key, _ := s.c.keys(cur)
			r := s.c.cacheToRecord(cur)
			r.caching(s.c)
			result = append(result, r.cache)
		}
	}
	return
}

func (sinfo *SearchInfo) First() (result SearchResult) {

	s := sinfo.s
	cur := sinfo.infos[0].start
	//key, _ := s.c.keys(cur)
	r := s.c.cacheToRecord(cur)
	r.caching(s.c)
	return r.cache
}

func (sinfo *SearchInfo) last() (result SearchResult) {

	s := sinfo.s
	cur := sinfo.infos[len(sinfo.infos)-1].end
	//key, _ := s.c.keys(cur)
	r := s.c.cacheToRecord(cur)
	r.caching(s.c)
	return r.cache
}

func (sinfo *SearchInfo) Match(s string) *SearchInfo {

	sval := SearchVal(s)
	return sinfo.Select(func(m Match) bool {
		return m.Uint64(sinfo.s.c.Name) <= sval
	}).Select(func(m Match) bool {
		return m.Uint64(sinfo.s.c.Name) >= sval
	})

}
