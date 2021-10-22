package vfsindex

import (
	"fmt"
	"time"

	"github.com/kazu/loncha"
	"github.com/kazu/vfs-index/query"
)

//go:generate go run golang.org/x/tools/cmd/stringer -type=SkipType

type SkipType byte

const (
	SkipFalse SkipType = iota
	SkipTrue
	SkipFinish
)

type GetColumn func() *Column

// SearchFinder ... FInder for query search
type SearchFinder struct {
	column    GetColumn
	recordFns []SearchFn
	skipdFns  []SkipFn
	keys      []uint64
	writer    func(c *Column, qrecs []*query.Record) interface{}

	recordFnChs []RecordChFn
	recordChs   []chan *query.Record
	writerCh    func(c *Column, qrecCh <-chan *query.Record, isFirstOnly bool) chan interface{}

	useStats  bool
	useChan   bool
	useStream bool
	stats     []*SearchFinderStat
}

// SearchFinderStat ... stats for SearchFinder
type SearchFinderStat struct {
	Name    string
	elapsed time.Duration
}

func EmptySkip(i int) SkipType {
	return SkipFalse
}

func NewSearchFinder(c *Column) *SearchFinder {

	return &SearchFinder{
		column:    func() *Column { return c },
		useChan:   false,
		useStream: false,
	}
}
func (sf *SearchFinder) addRecordChFn(fns ...RecordChFn) {
	for _, fn := range fns {
		sf.recordFnChs = append(sf.recordFnChs, fn)
		sf.recordChs = append(sf.recordChs, make(chan *query.Record, 10))
	}
}

func (sf *SearchFinder) Stop() {
	if len(sf.recordChs) > 0 {
		sf.recordChs[0] <- nil
	}
}

func (sf *SearchFinder) Limit(n int) *SearchFinder {
	size := len(sf.skipdFns)
	sf.skipdFns[size-1] = sf.limit(n)
	return sf
}

func (sf *SearchFinder) limit(n int) SkipFn {

	skiped := map[int]bool{}
	idx := len(sf.skipdFns) - 1
	oFn := EmptySkip
	if idx > 0 {
		oFn = sf.skipdFns[idx]
	}
	return func(k int) SkipType {
		if n+len(skiped) <= k {
			return SkipFinish
		}
		if oFn(k) == SkipTrue {
			skiped[k] = true
		}
		if skiped[k] {
			return SkipTrue
		}
		return SkipFalse
	}

}

func (sf *SearchFinder) MergeAsAnd(src *SearchFinder) {
	for _, key := range src.keys {
		lastIdx := len(sf.recordFns) - 1
		sf.skipdFns[lastIdx] = sf.AndWithColumn(lastIdx, key, src.column())
	}
	if len(sf.recordFnChs) > 0 {
		//sf.recordFnChs = append(sf.recordFnChs, src.recordFnChs...)
		sf.addRecordChFn(src.recordFnChs...)
	}

}

func (sf *SearchFinder) And(i int, key uint64) (result SkipFn) {
	return sf.AndWithColumn(i, key, sf.column())
}

func (sf *SearchFinder) AndWithColumn(i int, key uint64, col *Column) (result SkipFn) {

	var records []*query.Record
	var records2 []*query.Record

	found := map[int]bool{}

	isCached := false

	idx := len(sf.skipdFns) - 1
	oFn := EmptySkip
	if idx >= 0 {
		oFn = sf.skipdFns[idx]
	}

	return func(k int) (r SkipType) {
		s := time.Now()

		defer func() {
			return
			if CurrentLogLoevel != LOG_DEBUG {
				return
			}
			Log(LOG_DEBUG, "eval AND(%d,%s) type=%s dur=%s\n", k, DecodeTri(key), r, time.Now().Sub(s))
			if len(records) > k && records[k] != nil {
				ResultOutput("")
				data := sf.writer(sf.column(), []*query.Record{records[k]}).([]interface{})[0].(map[string]interface{})
				Log(LOG_DEBUG, "\traw=A%s\n", data[sf.column().Name])
			}
		}()

		if isCached {
			goto RESULT
		}

		if len(records) == 0 {
			records = sf.recordFns[i].RecFn(EmptySkip)
		}
		if oFn != nil && oFn(k) != SkipFalse {
			return oFn(k)
		}

		if len(records2) == 0 {
			idx := OpenIndexFile(col)
			records2 = idx.recordByKeyFn(key)(EmptySkip)
		}
		if len(records) == 0 || len(records2) == 0 {
			return SkipFinish
		}
		if !isCached {
			for j := range records {
				for l := range records2 {
					if IsEqQRecord(records[j], records2[l]) {
						found[j] = true
						break
					}
				}
			}
			isCached = true
		}

	RESULT:
		if found[k] {
			return SkipFalse
		}

		return SkipTrue

	}
}

func MesureElapsed() func(string) string {

	s := time.Now()

	return func(f string) string {
		return fmt.Sprintf(f, time.Now().Sub(s))
	}
}

func MesureElapsedToStat() func(string) *SearchFinderStat {

	s := time.Now()

	return func(f string) *SearchFinderStat {
		return &SearchFinderStat{
			Name:    f,
			elapsed: time.Now().Sub(s),
		}
	}
}

func (sf *SearchFinder) mergeOpts(opts ...SearchFinderOpt) {
	for _, opt := range opts {
		opt(sf)
	}
}

func (sf *SearchFinder) All(opts ...SearchFinderOpt) interface{} {

	ResultOutput("")(sf)
	sf.mergeOpts(opts...)

	elapsed := MesureElapsedToStat()

	if sf.useStream && sf.writerCh != nil {
		return sf.recordsCh(false)
	}

	recs := sf.Records()

	if sf.useStats {
		if sf.stats == nil {
			sf.stats = []*SearchFinderStat{}
		}
		sf.stats = append(sf.stats, elapsed("SearchFinder.All(): Records()"))
	}

	loncha.Uniq(&recs, func(i int) interface{} {
		return fmt.Sprintf("0x%x0x%x", recs[i].FileId().Uint64(), recs[i].Offset().Int64())
	})

	return sf.writer(sf.column(), recs)

}

func (sf *SearchFinder) recordsCh(isFirceAndClose bool) (outCh chan interface{}) {

	//	var wg sync.WaitGroup
	//	wg.Add(1)

	if sf.useChan && len(sf.recordFnChs) > 0 {
		outCh = sf.writerCh(sf.column(), sf.recordChs[len(sf.recordChs)-1], isFirceAndClose)

		for i := range sf.recordFnChs {
			if i == 0 {
				//outs[i] = (chan *query.Record)(sf.recordFnChs[i](nil))
				sf.recordFnChs[i](nil, sf.recordChs[i])
				continue
			}
			sf.recordFnChs[i](sf.recordChs[i-1], sf.recordChs[i])
		}
		return
	}

	recCh := make(chan *query.Record, 10)
	outCh = sf.writerCh(sf.column(), recCh, isFirceAndClose)
	for i := range sf.recordFns {
		sf.recordFns[i].forRecord = true
		for _, rec := range sf.recordFns[i].RecFn(sf.skipdFns[i]) {
			recCh <- rec
		}
	}
	outCh <- nil
	return

}

func (sf *SearchFinder) Records() (recs []*query.Record) {
	if sf.useChan && len(sf.recordFnChs) > 0 {
		//outs := make([]chan *query.Record, len(sf.recordFnChs))
		for i := range sf.recordFnChs {
			if i == 0 {
				//outs[i] = (chan *query.Record)(sf.recordFnChs[i](nil))
				sf.recordFnChs[i](nil, sf.recordChs[i])
				continue
			}
			//outs[i] = sf.recordFnChs[i](outs[i-1])
			sf.recordFnChs[i](sf.recordChs[i-1], sf.recordChs[i])

		}
		for rec := range sf.recordChs[len(sf.recordFnChs)-1] {
			if rec == nil {
				break
			}
			recs = append(recs, rec)
		}
		for i := range sf.recordChs {
			close(sf.recordChs[i])
		}
		return
	}

	for i := range sf.recordFns {
		sf.recordFns[i].forRecord = true
		recs = append(recs, sf.recordFns[i].RecFn(sf.skipdFns[i])...)
	}
	return
}

func (sf *SearchFinder) Count() (cnt int) {
	cnt = 0
	for i := range sf.recordFns {
		//sf.recordFns[i].forRecord = true
		cnt = sf.recordFns[i].CntFn(sf.skipdFns[i])
	}
	return
}

func (sf *SearchFinder) First(opts ...SearchFinderOpt) interface{} {

	//opts = append(opts, ResultOutput(""))
	sf.mergeOpts(opts...)

	if sf.useStream && sf.writerCh != nil {
		ch := sf.recordsCh(true)
		var result interface{}
		for r := range ch {
			result = r
			close(ch)
			break
		}
		return result
	}

	if sf.Count() == 0 {
		return nil
	}

	recs := sf.recordFns[0].RecFn(sf.skipdFns[0])
	return sf.writer(sf.column(), []*query.Record{recs[0]})

}

func (sf *SearchFinder) Last(opts ...SearchFinderOpt) interface{} {

	ResultOutput("")
	sf.mergeOpts(opts...)
	if sf.Count() == 0 {
		return nil
	}

	idx := len(sf.recordFns) - 1
	recs := sf.recordFns[idx].RecFn(sf.skipdFns[idx])

	return sf.writer(sf.column(), []*query.Record{recs[len(recs)-1]})
}

func (sf *SearchFinder) Stats() []*SearchFinderStat {
	return sf.stats
}

// SearchFinderOpt ... Option for SearchFinder
type SearchFinderOpt func(*SearchFinder)

// OptQueryStat ... enable stats option in SearchFinder
func OptQueryStat(t bool) SearchFinderOpt {

	return func(sf *SearchFinder) {
		sf.useStats = t
	}
}

func OptQueryUseChan(t bool) SearchFinderOpt {
	return func(sf *SearchFinder) {
		sf.useChan = t
	}
}

func ResultStreamt(t bool) SearchFinderOpt {
	return func(sf *SearchFinder) {
		sf.useStream = t
	}
}

// ResultOutput ... output option for SearchFinder
func ResultOutput(name string) SearchFinderOpt {

	return func(sf *SearchFinder) {
		sf.writer = func(c *Column, qrecs []*query.Record) interface{} {
			result := make([]interface{}, len(qrecs))
			for i, qrec := range qrecs {
				rec := &Record{
					fileID: qrec.FileId().Uint64(),
					offset: qrec.Offset().Int64(),
					size:   qrec.Size().Int64(),
				}
				rec.caching(c)
				result[i] = rec.cache
			}
			if len(name) == 0 {
				return result
			}

			if name == "json" || name == "csv" {
				enc, _ := GetDecoder("." + name)
				raw, _ := enc.Encoder(result)
				return string(raw)
			}
			return result
		}
		sf.writerCh = func(c *Column, qrecCh <-chan *query.Record, onlyFirst bool) (out chan interface{}) {

			out = make(chan interface{}, 10)
			go func(c *Column, in <-chan *query.Record) {
				for qrec := range in {
					if qrec == nil {
						break
					}

					rec := &Record{
						fileID: qrec.FileId().Uint64(),
						offset: qrec.Offset().Int64(),
						size:   qrec.Size().Int64(),
					}
					rec.caching(c)
					if name == "json" || name == "csv" {
						enc, _ := GetDecoder("." + name)
						raw, _ := enc.Encoder(rec.cache)
						out <- string(raw)
						if onlyFirst {
							break
						}
						continue
					}
					out <- rec
					if onlyFirst {
						break
					}
				}
				out <- nil
			}(c, qrecCh)
			return out
		}
	}
}
