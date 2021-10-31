package vfsindex

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	fbsio "github.com/kazu/fbshelper/query/base"
	"github.com/kazu/loncha"
	"github.com/kazu/vfs-index/cache"
	"github.com/kazu/vfs-index/expr"
	"github.com/kazu/vfs-index/query"
	query2 "github.com/kazu/vfs-index/query"
)

// SearchCond .. saerch condition object.
type SearchCond struct {
	Err error

	idx    *Indexer
	flist  *FileList
	idxCol *Column

	//m      Match
	table  string
	column string
	out    chan *Record

	wg *sync.WaitGroup

	// mu            sync.Mutex
	// cacheKey2Recs resultCache
	enableStats bool
	stats       []CondStat
}

type CondStat struct {
	Mes string
	Dur time.Duration
}

type mapInf map[string]interface{}

func NewSearchCond() *SearchCond {

	return &SearchCond{
		enableStats: false,
		stats:       nil,
	}
}
func (cond *SearchCond) addStat(m string, d time.Duration) {

	if cond.stats == nil {
		cond.stats = []CondStat{}
	}
	cond.stats = append(cond.stats, CondStat{Mes: m, Dur: d})
}

func (cond *SearchCond) EnableStats(t bool) {
	cond.enableStats = t
	if !t {
		cond.stats = nil
	}

}

func (cond *SearchCond) Stats() []CondStat {
	if !cond.enableStats {
		return nil
	}

	return cond.stats

}

func (cond *SearchCond) IndexFile() *IndexFile {
	//cond.indexColo
	return OpenIndexFile(cond.idxCol)

}

func (cond *SearchCond) startCol(col string) {
	cond.idxCol = cond.idx.openCol(cond.flist, cond.table, col)
	// e := cond.idxCol.caching()
	// if e != nil {
	// 	cond.flist.Reload()
	// 	cond.idxCol.Update(1 * time.Minute)
	// 	cond.idxCol.caching()
	// }
	cond.column = col
	cond.idxCol.IsNum = cond.idxCol.validateIndexType()
}

// CancelAndWait ... wait for canceld backgraound routine( mainly merging index)
func (cond *SearchCond) CancelAndWait() {
	go func() {
		time.Sleep(cond.idx.opt.mergeDuration)
		cond.idxCol.cancelAndWait()
	}()
	if cond.wg != nil {
		cond.wg.Wait()
	}
}

func (cond *SearchCond) ReloadFileList() {
	cond.flist.Reload()
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

type findByOption func(*IndexFile, uint64) []*IndexFile

const (
	KeyStateGot int = 1
	// KeyStateRun int = 2
	// KeyStateFlase int = 3
	KeyStateFinish int = 4
)

func (f *SearchCond) Query(s string) (r *SearchFinder) {
	q, err := expr.GetExpr(s)
	c := f.idxCol
	c.IsNum = c.validateIndexType()

	if len(q.Ands) == 1 && err == nil {
		return f.query(&q.Expr)
	}

	if err != nil {
		return NewSearchFinder(f.idxCol)
	}

	for i, expr := range q.Ands {
		if i == 0 {
			r = f.query(&expr)
			continue
		}
		r.MergeAsAnd(f.query(&expr))
	}

	return
}

// ScoreOfDistance ... distance infomation between strings.
type ScoreOfDistance struct {
	Distance  float64
	KeyScores map[uint64]map[uint64]KeyDistance
}

func (f *SearchCond) CountOfKey(key uint64, cname string) (cnt int) {

	cnt = 0
	cnt = cache.Counter.Cache(cname, key, cnt)
	if cnt > 0 {
		return cnt
	}
	sStart := time.Now()
	defer func() {
		if f.enableStats {
			f.addStat("CountOfKey() no cache", time.Now().Sub(sStart))
		}
	}()
	c := f.idxCol
	idxFinder := OpenIndexFile(c)

	cnt = 0
	for _, idx := range idxFinder.FindByKey(key) {
		if idx.IsType(IdxFileTypeWrite) {
			cnt++
			continue
		}
		if idx.IsType(IdxFileTypeMerge) {
			kr := idx.KeyRecords().Find(func(kr *query.KeyRecord) bool {
				return kr.Key().Uint64() == key
			})
			if kr != nil && kr.CommonNode != nil {
				cnt += kr.Records().Count()
			}
		}
	}
	return cache.Counter.Cache(cname, key, cnt)

	// finder := f.Select(func(cond SearchElem) bool {
	// 	return cond.Op(cname, "==", key)
	// })
	// recs, _ := finder.All(OptQueryUseChan(true)).([]interface{})
	//return cache.Counter.Cache(cname, key, len(recs))

}

func (f *SearchCond) sortKeys(keys []uint64, fn func(i, j int) bool) []uint64 {

	movelist := make([]int, len(keys))
	for i := range movelist {
		movelist[i] = i
	}

	sort.Slice(movelist, func(i, j int) bool {
		return fn(movelist[i], movelist[j])
	})

	swap := reflect.Swapper(keys)

	for i, j := range movelist {
		if j < i {
			continue
		}
		swap(i, j)
	}
	return keys

}

func (f *SearchCond) filterByAvg(keys []uint64, cname string) []uint64 {

	sStart := time.Now()
	defer func() {
		if f.enableStats {
			f.addStat("filterByAvg()", time.Now().Sub(sStart))
		}
	}()

	if len(keys) <= 2 {
		return keys
	}
	loncha.Delete(&keys, func(i int) bool {
		return f.CountOfKey(keys[i], cname) == 0
	})
	if len(keys) <= 2 {
		return keys
	}

	keys = f.sortKeys(keys, func(i, j int) bool {
		return f.CountOfKey(keys[i], cname) < f.CountOfKey(keys[j], cname)
	})

	avg := 0
	if len(keys) > 10 {
		avg = f.CountOfKey(keys[len(keys)/2], cname)
		goto FILTER
	}

	for _, k := range keys {
		avg += f.CountOfKey(k, cname)
	}
	avg = avg / len(keys)

FILTER:

	loncha.Delete(&keys, func(i int) bool {
		return f.CountOfKey(keys[i], cname) > int(avg)
	})
	return keys
}

type recKey struct {
	FileID uint64
	Offset int64
}

type reckey2Record map[recKey]*Record

func (m reckey2Record) Cache(rec *Record) *Record {
	if m[recKey{FileID: rec.fileID, Offset: rec.offset}] != nil {
		return m[recKey{FileID: rec.fileID, Offset: rec.offset}]
	}
	m[recKey{FileID: rec.fileID, Offset: rec.offset}] = rec
	return m[recKey{FileID: rec.fileID, Offset: rec.offset}]
}

func (f *SearchCond) Nears(src string, cname string) ([]*Record, []ScoreOfDistance) {
	sStart := time.Now()
	defer func() {
		if f.enableStats {
			f.addStat("Nears()", time.Now().Sub(sStart))
		}
	}()
	keys := []uint64{}
	for _, str := range strings.Split(src, " ") {
		keys = append(keys, TriKeys(str)...)
	}

	key2recs := map[uint64][]*Record{}
	reckey2rec := reckey2Record{}
	_ = reckey2rec
	for _, key := range keys {
		finder := f.Select(func(cond SearchElem) bool {
			return cond.Op(cname, "==", key)
		})

		irecs := finder.All(
			OptQueryUseChan(true),
			ResultFnOutput(func(c *Column, qrecs ...*query2.Record) interface{} {
				return finder.queryRecordsToRecords(qrecs...)

			}, func(c *Column, qrecCh <-chan *query2.Record, limit int) (out chan interface{}) {
				out = make(chan interface{}, 10)
				go func(c *Column, in <-chan *query2.Record) {
					defer recoverOnWriteClosedChan()
					cnt := -1
					for qrec := range in {
						cnt++
						if qrec == nil {
							break
						}
						out <- finder.queryRecordsToRecords(qrec)[0]
						if limit > 0 && cnt >= limit {
							break
						}
					}
					out <- nil
				}(c, qrecCh)
				return out
			}))
		recs := irecs.([]*Record)
		key2recs[key] = recs
		for _, rec := range recs {
			reckey2rec.Cache(rec)
		}
	}
	for key, recs := range key2recs {
		for i := range recs {
			s, _ := recs[i].cache[cname].(string)
			dkeys := str2keys(s)
			found := loncha.Contain(&dkeys, func(i int) bool {
				return dkeys[i] == key
			})
			if !found {
				continue
			}
			rec := reckey2rec.Cache(recs[i])
			score := float64(0)
			_ = score
			if _, ok := rec.cache["Score"].(float64); ok {
				score = rec.cache["Score"].(float64)
			}
			score += 1.0 / float64(len(key2recs[key]))
			rec.cache["Score"] = score
		}
	}
	resultRecs := make([]*Record, 0, len(reckey2rec))
	resultScoreOfDistances := make([]ScoreOfDistance, 0, len(reckey2rec))
	for _, rec := range reckey2rec {
		resultRecs = append(resultRecs, rec)
		dist := ScoreOfDistance{Distance: 1.0}
		if rec.cache["Score"] != nil {
			score := rec.cache["Score"].(float64)
			dist.Distance = 1.0 - score
		}
		resultScoreOfDistances = append(resultScoreOfDistances, dist)
	}

	return sortNearsResult(resultRecs, resultScoreOfDistances)
}

func sortNearsResult(recs []*Record, scores []ScoreOfDistance) ([]*Record, []ScoreOfDistance) {

	movelist := make([]int, len(recs), len(recs))
	for i := range movelist {
		movelist[i] = i
	}
	sort.Slice(movelist, func(i, j int) bool {
		return scores[movelist[i]].Distance < scores[movelist[j]].Distance
	})
	swapR := reflect.Swapper(recs)
	swapS := reflect.Swapper(movelist)

	for i, j := range movelist {
		if i > j {
			continue
		}
		swapR(i, j)
		swapS(i, j)
	}

	return recs, scores

}

func (f *SearchCond) OldNears(src string, cname string) ([]*Record, []ScoreOfDistance) {

	sStart := time.Now()
	defer func() {
		if f.enableStats {
			f.addStat("Nears()", time.Now().Sub(sStart))
		}
	}()

	keys := []uint64{}
	for _, str := range strings.Split(src, " ") {
		keys = append(keys, TriKeys(str)...)
	}

	keys = f.filterByAvg(keys, cname)

	return sortNearsResult(f.nearsByKeys(cname, keys...))

}

type outNearsByQuery func(t NearsByQueryType, result interface{})

type NearsByQueryType byte

const (
	ScoreOnly  NearsByQueryType = 1
	WithRecord NearsByQueryType = 2
)

func GotNearsByQuery(t NearsByQueryType, result interface{}) {

}

func (f *SearchCond) nearsByKeys(cname string, keys ...uint64) (recs []*Record, scores []ScoreOfDistance) {

	recs = []*Record{}
	sStart := time.Now()

	for _, key := range keys {

		finder := f.Select(func(cond SearchElem) bool {
			return cond.Op(cname, "==", key)
		})
		irecs := finder.All(
			OptQueryUseChan(true),
			ResultFnOutput(func(c *Column, qrecs ...*query2.Record) interface{} {
				return finder.queryRecordsToRecords(qrecs...)

			}, func(c *Column, qrecCh <-chan *query2.Record, limit int) (out chan interface{}) {
				out = make(chan interface{}, 10)
				go func(c *Column, in <-chan *query2.Record) {
					defer recoverOnWriteClosedChan()
					cnt := -1
					for qrec := range in {
						cnt++
						if qrec == nil {
							break
						}
						out <- finder.queryRecordsToRecords(qrec)[0]
						if limit > 0 && cnt >= limit {
							break
						}
					}
					out <- nil
				}(c, qrecCh)
				return out
			}))
		srecs := irecs.([]*Record)
		recs = append(recs, srecs...)
	}
	if f.enableStats {
		f.addStat("nearsByKeys() fetch records by keys", time.Now().Sub(sStart))
	}

	scores = make([]ScoreOfDistance, len(recs), len(recs))
	cache := map[uint64]map[uint64]KeyDistance{}

	optCache := OptCache(cache)

	sort.Slice(recs, func(i, j int) bool {
		if recs[i].fileID < recs[j].fileID {
			return true
		}
		if recs[i].fileID > recs[j].fileID {
			return false
		}
		if recs[i].offset < recs[j].offset {
			return true
		}
		return false
	})
	loncha.Uniq2(&recs, func(i, j int) bool {
		return recs[i].fileID == recs[j].fileID &&
			recs[i].offset == recs[j].offset
	})

	for i, rec := range recs {
		key2val := rec.cache
		val := key2val[cname]
		s, ok := val.(string)
		_ = s
		if !ok {
			continue
		}
		scores[i] = f.DistanceOfKeys(keys, s, optCache)
		optCache = OptCache(optCache(nil))
	}
	// type intScoreOfDistance struct {
	// 	idx   int
	// 	score ScoreOfDistance
	// }
	// goDistanceOfString := func(ch chan<- intScoreOfDistance, i int, src, s string) {
	// 	result := f.DistanceOfString(src, s, optCache)
	// 	optCache = OptCache(optCache(nil))
	// 	ch <- intScoreOfDistance{idx: i, score: result}
	// }

	// ch := make(chan intScoreOfDistance, 10)
	// cnt := len(recs)
	// for i, rec := range recs {
	// 	key2val := rec.cache
	// 	val := key2val[cname]
	// 	s, ok := val.(string)
	// 	_ = s
	// 	if !ok {
	// 		continue
	// 	}
	// 	go goDistanceOfString(ch, i, src, s)
	// }
	// for s := range ch {
	// 	cnt--
	// 	if cnt <= 0 {
	// 		break
	// 	}
	// 	scores[s.idx] = s.score
	// }

	return
}

type DistanceOfStringArg func(s map[uint64]map[uint64]KeyDistance) map[uint64]map[uint64]KeyDistance

func OptCache(cache map[uint64]map[uint64]KeyDistance) DistanceOfStringArg {
	var mu sync.Mutex

	mu.Lock()
	cacheMap := cache
	mu.Unlock()

	return func(s map[uint64]map[uint64]KeyDistance) map[uint64]map[uint64]KeyDistance {

		mu.Lock()
		if s != nil {
			cacheMap = s
		}
		mu.Unlock()

		mu.Lock()
		result := cacheMap
		mu.Unlock()

		return result
	}
}
func (f *SearchCond) DistanceOfString(src, dst string, cachFns ...DistanceOfStringArg) ScoreOfDistance {

	skeys := str2keys(src)
	return f.DistanceOfKeys(skeys, dst, cachFns...)
}

func (f *SearchCond) DistanceOfKeys(skeys []uint64, dst string, cachFns ...DistanceOfStringArg) ScoreOfDistance {

	sStart := time.Now()
	defer func() {
		if f.enableStats {
			f.addStat("DistanceOfKeys()", time.Now().Sub(sStart))
		}
	}()

	cacheMap := cachFns[0](nil)

	var mu sync.Mutex

	key2keyDist := func(skey, dkey uint64) KeyDistance {
		fkey := skey
		lkey := dkey
		if lkey > fkey {
			fkey, lkey = lkey, fkey
		}
		mu.Lock()
		if cacheMap[fkey] == nil {

			cacheMap[fkey] = map[uint64]KeyDistance{}
		}
		mu.Unlock()

		var found bool
		mu.Lock()
		_, found = cacheMap[fkey][lkey]
		mu.Unlock()
		if !found {
			result := f.distance(fkey, lkey, f.idxCol.Name)
			mu.Lock()
			cacheMap[fkey][lkey] = result
			mu.Unlock()
		}
		mu.Lock()
		result := cacheMap[fkey][lkey]
		mu.Unlock()
		return result
	}

	//skeys := str2keys(src)
	dkeys := str2keys(dst)
	dkeys = f.filterByAvg(dkeys, f.idxCol.Name)

	score := float64(0.0)

	gokey2keyDist := func(ch chan<- KeyDistance, skey, dkey uint64) {
		ch <- key2keyDist(skey, dkey)
	}
	_ = gokey2keyDist

	for _, skey := range skeys {
		sScore := float64(0.0)

		// for _, dkey := range dkeys {
		// 	s := key2keyDist(skey, dkey)
		// 	sScore += s.Score
		// }
		ch := make(chan KeyDistance, 10)
		cnt := len(dkeys)
		for _, dkey := range dkeys {
			go gokey2keyDist(ch, skey, dkey)
		}
		for s := range ch {
			if cnt <= 1 {
				break
			}
			sScore += s.Score
			cnt--
		}

		score += sScore / float64(len(dkeys))
	}
	score /= float64(len(skeys))

	cachFns[0](cacheMap)

	return ScoreOfDistance{
		Distance:  score,
		KeyScores: cacheMap,
	}
}

func str2keys(src string) (keys []uint64) {
	sstrs := strings.Split(src, " ")

	for _, str := range sstrs {
		keys = append(keys, TriKeys(str)...)
	}
	return
}

type KeyDistance struct {
	Score float64
	Cnt   int
}

func (f *SearchCond) distance(skey, dkey uint64, cname string) KeyDistance {

	cntFn := func(skey, dkey uint64, isAnd bool, cname string) (cnt int) {

		cnt = f.CountOfKey(skey, cname)
		if isAnd {
			cnt = cache.Counter.Cache(fmt.Sprintf("%s%x", cname, skey), dkey, 0)
		}

		if cnt > 0 {
			return
		}

		if !isAnd {
			return cnt
		}
		sStart := time.Now()
		defer func() {
			if f.enableStats {
				f.addStat("distance() no cache And", time.Now().Sub(sStart))
			}
		}()

		sfinder2 := f.Select(func(cond SearchElem) bool {
			return cond.Op(cname, "==", skey)
		})
		dfinder2 := f.Select(func(cond SearchElem) bool {
			return cond.Op(cname, "==", dkey)
		})

		sfinder2.MergeAsAnd(dfinder2)
		ands := sfinder2.All(OptQueryUseChan(true)).([]interface{})
		if len(ands) == 0 {
			return 0
		}
		cnt = cache.Counter.Cache(fmt.Sprintf("%s%x", cname, skey), dkey, len(ands))
		return cnt
	}

	scnt := cntFn(skey, dkey, false, cname)
	if skey == dkey {
		return KeyDistance{Score: 0, Cnt: 0}
	}
	dcnt := cntFn(dkey, skey, false, cname)
	acnt := int(0)
	if scnt > dcnt {
		acnt = cntFn(dkey, skey, true, cname)
	} else {
		acnt = cntFn(skey, dkey, true, cname)
	}

	if acnt == 0 || scnt == 0 || dcnt == 0 {
		return KeyDistance{Score: 1.0, Cnt: 0}
	}

	return KeyDistance{
		Score: float64(1.0) - (float64(acnt) / float64(fbsio.MaxInt(scnt, dcnt))),
		Cnt:   fbsio.MaxInt(scnt, dcnt),
	}

}

func (f *SearchCond) changeCol(col string) (cond *SearchCond) {
	cond = NewSearchCond()
	cond.idx = f.idx
	cond.flist = f.flist
	cond.table = f.table
	cond.column = col
	cond.startCol(cond.column)
	cond.idxCol.isMergeOnSearch = f.idx.opt.idxMergeOnSearch

	return
}

func (of *SearchCond) query(q *expr.Expr) (r *SearchFinder) {
	c := of.idxCol
	f := of
	if c.Name != q.Column {
		f = of.changeCol(q.Column)
		c = f.idxCol
		c.IsNum = c.validateIndexType()
	}

	if q.Op == "search" && c.Name == q.Column && !c.IsNum {
		return f.Select(func(cond SearchElem) bool {
			return cond.Op(q.Column, "==", q.Value)
		})
	}
	var uintVal uint64
	if f.idxCol.IsNum {
		uintVal, _ = strconv.ParseUint(q.Value, 10, 64)
		return f.Select(func(cond SearchElem) bool {
			return cond.Op(q.Column, q.Op, uintVal)
		})
	}

	return f.Select(func(cond SearchElem) bool {
		return cond.Op(q.Column, q.Op, q.Value)
	})
}

func (f *SearchCond) Match(s string) *SearchFinder {
	c := f.idxCol
	return f.FindBy(c.Name, s)
}

func (cond *SearchCond) FindBy(col string, kInf interface{}) (sfinder *SearchFinder) {

	return cond.Select(func(e SearchElem) bool {
		return e.Op(col, "==", kInf)
	})
}

func (cond *SearchCond) Select(fn func(SearchElem) bool) (sfinder *SearchFinder) {

	c := cond.idxCol
	idxFinder := OpenIndexFile(c)

	sfinder2 := NewSearchFinder(cond.Column())

	keys := []uint64{}

	keyState := make(chan int, 2)

	setKey := func(origk interface{}) []uint64 {
		switch k := origk.(type) {
		case uint64:
			keys = append(keys, k)
		case int:
			keys = append(keys, uint64(k))
		case string:
			tmps := TriKeys(k)
			cnts := make([]int, len(tmps))
			for i := range cnts {
				cnts[i] = -1
			}
			// if !idxFinder.useChan {
			// 	sort.Slice(tmps, func(i, j int) bool {
			// 		if cnts[i] == -1 {
			// 			cnts[i] = idxFinder.countBy(tmps[i])
			// 		}
			// 		if cnts[j] == -1 {
			// 			cnts[j] = idxFinder.countBy(tmps[j])
			// 		}
			// 		return cnts[i] < cnts[j]
			// 	})
			// }
			keys = append(keys, tmps...)
		}
		keyState <- KeyStateGot
		return keys
	}
	getCol := func() *Column {
		return cond.idxCol
	}

	gotVal := func(col string, op CondOp) (sfind *SearchFinder) {
		sfind = NewSearchFinder(cond.Column())
		switch op {
		case CondOpEq:
			for i, key := range keys {
				if i == 0 {
					sfind.recordFns = append(sfind.recordFns,
						SearchFn{
							RecFn: idxFinder.recordByKeyFn(key),
							CntFn: idxFinder.countFnBy(key)})

					sfind.skipdFns = append(sfind.skipdFns, EmptySkip)
					sfind.addRecordChFn(idxFinder.recordByKeyChFn(key))
				}
				lastIdx := len(sfind.recordFns) - 1
				if i > 0 {
					sfind.skipdFns[lastIdx] = sfind.And(lastIdx, key)
					sfind.addRecordChFn(idxFinder.recordByKeyChFn(key))
				}
			}
		case CondOpLe, CondOpLt:
			for i, key := range keys {
				if i == 0 {
					sfind.recordFns = append(sfind.recordFns, idxFinder.commonNearFnByKey(key, true))
					sfind.skipdFns = append(sfind.skipdFns, EmptySkip)
					sfind.addRecordChFn(idxFinder.commonNearFnByKey(key, true).RecChFn)

				}
				lastIdx := len(sfind.recordFns) - 1
				if i > 0 {
					sfind.skipdFns[lastIdx] = sfind.And(lastIdx, key)
					sfind.addRecordChFn(idxFinder.commonNearFnByKey(key, true).RecChFn)
				}
			}
		case CondOpGe, CondOpGt:
			for i, key := range keys {
				if i == 0 {
					sfind.recordFns = append(sfind.recordFns, idxFinder.commonNearFnByKey(key, false))
					sfind.skipdFns = append(sfind.skipdFns, EmptySkip)
					sfind.addRecordChFn(idxFinder.commonNearFnByKey(key, false).RecChFn)
				}
				lastIdx := len(sfind.recordFns) - 1
				if i > 0 {
					sfind.skipdFns[lastIdx] = sfind.And(lastIdx, key)
					sfind.addRecordChFn(idxFinder.commonNearFnByKey(key, false).RecChFn)
				}
			}
		}
		sfind.keys = keys
		sfinder = sfind
		return sfind
	}

	econd := SearchElem{setKey: setKey, Column: getCol, getValue: gotVal}

	var isTrue bool
	go func(cond SearchElem) {
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

type KeySetter func(interface{}) []uint64
type ColGetter func() *Column

type ValueGetter func(string, CondOp) *SearchFinder

type SearchElem struct {
	setKey   KeySetter
	Column   ColGetter
	getValue ValueGetter
}

func (cond SearchElem) Op(col, op string, v interface{}) (result bool) {

	if col != cond.Column().Name {
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
		cond.wg = &sync.WaitGroup{}
		cond.wg.Add(1)
		go c.mergeIndex(c.ctx, cond.wg)
	}
}

// func DebugOutput(isDebug bool) ResultOpt {

// 	return func(c *Column, qrecs []*query.Record) interface{} {

// 	}

// }
