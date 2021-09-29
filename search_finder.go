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

type RecordFn func(SkipFn) []*query.Record
type SkipFn func(int) SkipType
type GetColumn func() *Column
type SearchFinder struct {
	column    GetColumn
	recordFns []RecordFn
	skipdFns  []SkipFn
	keys      []uint64
}

func EmptySkip(i int) SkipType {
	return SkipFalse
}

func NewSearchFinder(c *Column) *SearchFinder {

	return &SearchFinder{
		column: func() *Column { return c },
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
		sf.skipdFns[lastIdx] = sf.And(lastIdx, key)
	}
}

func (sf *SearchFinder) And(i int, key uint64) (result SkipFn) {

	var records []*query.Record
	var records2 []*query.Record

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
				out := ResultOutput("")
				data := out(sf.column(), []*query.Record{records[k]}).([]interface{})[0].(map[string]interface{})
				Log(LOG_DEBUG, "\traw=A%s\n", data[sf.column().Name])
			}
		}()

		if len(records) == 0 {
			records = sf.recordFns[i](EmptySkip)
		}
		if oFn != nil && oFn(k) != SkipFalse {
			return oFn(k)
		}
		idx := OpenIndexFile(sf.column())
		if len(records2) == 0 {
			records2 = idx.recordByKeyFn(key)(EmptySkip)
		}
		if len(records) == 0 || len(records2) == 0 {
			return SkipFinish
		}

		for j := range records {
			found := false
			for i := range records2 {
				if IsEqQRecord(records[j], records2[i]) {
					found = true
					break
				}
			}
			if found && j == k {
				r = SkipFalse
				return
			}
			if j == k {
				break
			}
		}
		r = SkipTrue
		return
	}
}

func MesureElapsed() func(string) string {

	s := time.Now()

	return func(f string) string {
		return fmt.Sprintf(f, time.Now().Sub(s))
	}
}

func (sf *SearchFinder) All(opts ...ResultOpt) interface{} {

	elapsed := MesureElapsed()

	opts = append(opts, ResultOutput(""))

	recs := sf.Records()
	if LogIsDebug() {
		Log(LOG_DEBUG, "SearchFinder.All():Records() %s\n", elapsed("%s"))
	}
	loncha.Uniq(&recs, func(i int) interface{} {
		return fmt.Sprintf("0x%x0x%x", recs[i].FileId().Uint64(), recs[i].Offset().Int64())
	})
	// for i := range recs {
	// 	result = append(result, opts[0](sf.column(), recs[i]))
	// }

	return opts[0](sf.column(), recs)

}
func (sf *SearchFinder) Records() (recs []*query.Record) {
	for i := range sf.recordFns {
		recs = append(recs, sf.recordFns[i](sf.skipdFns[i])...)
	}
	return
}

func (sf *SearchFinder) Count() int {
	return len(sf.recordFns)
}

func (sf *SearchFinder) First(opts ...ResultOpt) interface{} {

	opts = append(opts, ResultOutput(""))

	if sf.Count() == 0 {
		return nil
	}
	fmt.Printf("sf.Count()=%d\n", sf.Count())

	recs := sf.recordFns[0](sf.skipdFns[0])
	return opts[0](sf.column(), []*query.Record{recs[0]})

}

func (sf *SearchFinder) Last(opts ...ResultOpt) interface{} {

	opts = append(opts, ResultOutput(""))

	if sf.Count() == 0 {
		return nil
	}

	idx := len(sf.recordFns) - 1
	recs := sf.recordFns[idx](sf.skipdFns[idx])

	return opts[0](sf.column(), []*query.Record{recs[len(recs)-1]})
}
