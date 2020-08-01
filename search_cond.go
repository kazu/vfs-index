package vfsindex

import (
	"context"
	"strconv"
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

	//m      Match
	table  string
	column string
	out    chan *Record
}

type mapInf map[string]interface{}

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

// CancelAndWait ... wait for canceld backgraound routine( mainly merging index)
func (cond *SearchCond) CancelAndWait() {
	cond.idxCol.cancelAndWait()
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

// Deprecated: should use Query2
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

func (f *SearchCond) Query2(s string) (r *SearchFinder2) {
	q, err := expr.GetExpr(s)
	c := f.idxCol
	c.IsNum = c.IsNumViaIndex()

	if err != nil {
		return NewSearchFinder2(f.idxCol)
	}

	// if q.Op == "search" && c.Name == q.Column && !c.IsNum {
	// 	return f.FindBy(q.Column, q.Value)
	// }
	// if q.Op == "==" {
	// 	if f.idxCol.IsNum {
	// 		uintVal, _ := strconv.ParseUint(q.Value, 10, 64)
	// 		return f.FindBy(q.Column, uintVal)
	// 	}
	// 	return f.FindBy(q.Column, q.Value)
	// }
	// return EmptySearchFinder()

	if q.Op == "search" && c.Name == q.Column && !c.IsNum {
		return f.Select2(func(cond SearchCondElem2) bool {
			return cond.Op(q.Column, "==", q.Value)
		})
	}
	var uintVal uint64
	if f.idxCol.IsNum {
		uintVal, _ = strconv.ParseUint(q.Value, 10, 64)
		return f.Select2(func(cond SearchCondElem2) bool {
			return cond.Op(q.Column, q.Op, uintVal)
		})
	}

	return f.Select2(func(cond SearchCondElem2) bool {
		return cond.Op(q.Column, q.Op, q.Value)
	})
}

func (f *SearchCond) Match(s string) *SearchFinder {
	c := f.idxCol
	return f.FindBy(c.Name, s)
}

func (cond *SearchCond) Select2(fn func(SearchCondElem2) bool) (sfinder *SearchFinder2) {

	// s := time.Now()
	// defer func() {
	// 	fmt.Printf("Select2(): elapsed=%s", time.Now().Sub(s))
	// }()

	c := cond.idxCol
	idxFinder := OpenIndexFile(c)

	sfinder2 := NewSearchFinder2(cond.Column())

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
				if i > 0 {
					sfind.skipdFns[lastIdx] = sfind.And(lastIdx, key)
				}
			}
		case CondOpLe, CondOpLt:
			for i, key := range keys {
				if i == 0 {
					sfind.recordFns = append(sfind.recordFns, idxFinder.RecordNearByKey(key, true))
					sfind.skipdFns = append(sfind.skipdFns, EmptySkip)
				}
				lastIdx := len(sfind.recordFns) - 1
				if i > 0 {
					sfind.skipdFns[lastIdx] = sfind.And(lastIdx, key)
				}
			}
		case CondOpGe, CondOpGt:
			for i, key := range keys {
				if i == 0 {
					sfind.recordFns = append(sfind.recordFns, idxFinder.RecordNearByKey(key, false))
					sfind.skipdFns = append(sfind.skipdFns, EmptySkip)
				}
				lastIdx := len(sfind.recordFns) - 1
				if i > 0 {
					sfind.skipdFns[lastIdx] = sfind.And(lastIdx, key)
				}
			}
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

type SearchCondElem2 struct {
	setKey   SetKey
	Column   GetCol
	getValue GetValue2
}

func (cond SearchCondElem2) Op(col, op string, v interface{}) (result bool) {

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
		go c.MergingIndex(c.ctx)
		time.Sleep(200 * time.Millisecond)
	}
}

type ResultOpt func(*Column, []*query.Record) interface{}

func ResultOutput(name string) ResultOpt {

	return func(c *Column, qrecs []*query.Record) interface{} {

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
}
