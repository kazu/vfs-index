package vfsindex

import (
	"context"
	"strconv"
	"time"

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

type findByOption func(*IndexFile, uint64) []*IndexFile

const (
	KeyStateGot int = 1
	// KeyStateRun int = 2
	// KeyStateFlase int = 3
	KeyStateFinish int = 4
)

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

func (f *SearchCond) Match(s string) *SearchFinder2 {
	c := f.idxCol
	return f.FindBy(c.Name, s)
}

func (cond *SearchCond) FindBy(col string, kInf interface{}) (sfinder *SearchFinder2) {

	return cond.Select2(func(e SearchCondElem2) bool {
		return e.Op(col, "==", kInf)
	})
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

//type GetValue func(string, CondOp) *SearchFinder
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
		time.Sleep(20 * time.Millisecond)
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

// func DebugOutput(isDebug bool) ResultOpt {

// 	return func(c *Column, qrecs []*query.Record) interface{} {

// 	}

// }
