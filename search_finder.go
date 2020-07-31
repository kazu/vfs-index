package vfsindex

import (
	"fmt"

	"github.com/kazu/fbshelper/query/base"
	"github.com/kazu/loncha"
	"github.com/kazu/vfs-index/query"
)

type SearchFinder struct {
	c       *Column
	idxs    []*IndexFile
	mode    SearchMode
	start   uint64
	last    uint64
	matches []*query.Record
	isEmpty bool
}

func EmptySearchFinder() *SearchFinder {

	return &SearchFinder{
		isEmpty: true,
	}
}

func (s *SearchFinder) KeyRecord() *query.KeyRecord {

	if s.idxs[0].IsType(IdxFileType_Write) {
		kr := query.NewKeyRecord()
		kr.SetKey(query.FromUint64(s.start))
		rlist := query.NewRecordList()
		rlist.Base = base.NewNoLayer(rlist.Base)
		for i := range s.idxs {
			r := s.idxs[i].KeyRecord().Value()
			rlist.SetAt(i, r)
		}
		kr.SetRecords(rlist.CommonNode)

		kr.Flatten()
		return kr
	}

	if s.idxs[0].IsType(IdxFileType_Merge) {
		//s.idx.KeyRecords()
		kr := s.idxs[0].KeyRecords().Find(func(kr *query.KeyRecord) bool {
			return kr.Key().Uint64() == s.start
		})
		return kr
	}
	return nil
}

func (s *SearchFinder) Records() []*query.Record {

	if len(s.matches) > 0 {
		return s.matches
	}

	kr := s.KeyRecord()
	if kr == nil {
		return nil
	}

	return kr.Records().All()
}

func (s1 *SearchFinder) And(s2 *SearchFinder) (s *SearchFinder) {

	s = &SearchFinder{
		c:       s1.c,
		mode:    s1.mode,
		matches: s1.Records(),
	}
	//last_match := 0

	records2 := s2.Records()
	ret, err := loncha.Select(&s.matches, func(j int) bool {
		for i := 0; i < len(records2); i++ {
			r1 := s.matches[j]
			r2 := records2[i]
			if r1.FileId().Uint64() == r2.FileId().Uint64() &&
				r1.Offset().Int64() == r2.Offset().Int64() {
				//last_match = i + 1
				return true
			}
		}
		//last_match = len(records2) - 1
		return false
	})
	if err != nil {
		s.matches = nil
	}
	s.matches = ret.([]*query.Record)

	return s
}

func (s1 *SearchFinder) All(opts ...ResultOpt) []interface{} {
	if s1.isEmpty {
		return nil
	}
	opts = append(opts, ResultOutput(""))

	result := []interface{}{}
	for _, rec := range s1.Records() {
		result = append(result, opts[0](s1.c, rec))
	}

	return result
}

func (s1 *SearchFinder) First(opts ...ResultOpt) interface{} {
	if s1.isEmpty {
		return nil
	}
	opts = append(opts, ResultOutput(""))
	recs := s1.Records()
	if recs == nil {
		return nil
	}
	return opts[0](s1.c, recs[0])
}

func (s1 *SearchFinder) Last(opts ...ResultOpt) interface{} {
	if s1.isEmpty {
		return nil
	}
	opts = append(opts, ResultOutput(""))
	recs := s1.Records()
	if recs == nil {
		return nil
	}
	return opts[0](s1.c, recs[len(recs)-1])
}

type RecordFn func(map[int]bool) []*query.Record
type SkipFn func(map[int]bool) map[int]bool
type GetColumn func() *Column
type SearchFinder2 struct {
	column    GetColumn
	recordFns []RecordFn
	skipdFns  []SkipFn
}

func EmptySkip(o map[int]bool) map[int]bool {
	return map[int]bool{}
}

func NewSearchFinder2(c *Column) *SearchFinder2 {

	return &SearchFinder2{
		column: func() *Column { return c },
	}
}

func (sf *SearchFinder2) And(i int, key uint64) SkipFn {

	return func(oskiped map[int]bool) (skiped map[int]bool) {
		skiped = oskiped
		records := sf.recordFns[i](skiped)
		idx := OpenIndexFile(sf.column())
		records2 := idx.RecordByKey(key)(map[int]bool{})

		for j := range records {
			if skiped[j] {
				continue
			}
			found := false
			for i := range records2 {
				if records[j].FileId().Uint64() == records2[i].FileId().Uint64() {
					found = true
					break
				}
			}
			if !found {
				skiped[j] = true
			}
		}
		return skiped
	}
}

func (sf *SearchFinder2) All(opts ...ResultOpt) []interface{} {

	opts = append(opts, ResultOutput(""))

	result := []interface{}{}

	recs := sf.Records()
	loncha.Uniq(&recs, func(i int) interface{} {
		return fmt.Sprintf("0x%x0x%x", recs[i].FileId().Uint64(), recs[i].Offset().Int64())
	})
	for i := range recs {
		result = append(result, opts[0](sf.column(), recs[i]))
	}

	return result

}
func (sf *SearchFinder2) Records() (recs []*query.Record) {
	for i := range sf.recordFns {
		recs = append(recs, sf.recordFns[i](sf.skipdFns[i](map[int]bool{}))...)
	}
	return
}

func (sf *SearchFinder2) Count() int {
	return len(sf.recordFns)
}

func (sf *SearchFinder2) First(opts ...ResultOpt) interface{} {

	opts = append(opts, ResultOutput(""))

	if sf.Count() == 0 {
		return nil
	}

	recs := sf.recordFns[0](sf.skipdFns[0](map[int]bool{}))
	return opts[0](sf.column(), recs[0])

}

func (sf *SearchFinder2) Last(opts ...ResultOpt) interface{} {

	opts = append(opts, ResultOutput(""))

	if sf.Count() == 0 {
		return nil
	}

	idx := len(sf.recordFns) - 1
	recs := sf.recordFns[idx](sf.skipdFns[idx](map[int]bool{}))

	return opts[0](sf.column(), recs[len(recs)-1])
}
