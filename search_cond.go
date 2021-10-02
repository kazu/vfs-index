package vfsindex

import (
	"context"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/kazu/vfs-index/expr"
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
}

type mapInf map[string]interface{}

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

func (f *SearchCond) changeCol(col string) (cond *SearchCond) {

	cond = &SearchCond{idx: f.idx, flist: f.flist, table: f.table, column: col}
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
			sort.Slice(tmps, func(i, j int) bool {
				return idxFinder.countBy(tmps[i]) < idxFinder.countBy(tmps[j])
			})
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
				}
				lastIdx := len(sfind.recordFns) - 1
				if i > 0 {
					sfind.skipdFns[lastIdx] = sfind.And(lastIdx, key)
				}
			}
		case CondOpLe, CondOpLt:
			for i, key := range keys {
				if i == 0 {
					sfind.recordFns = append(sfind.recordFns, idxFinder.commonNearFnByKey(key, true))
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
					sfind.recordFns = append(sfind.recordFns, idxFinder.commonNearFnByKey(key, false))
					sfind.skipdFns = append(sfind.skipdFns, EmptySkip)
				}
				lastIdx := len(sfind.recordFns) - 1
				if i > 0 {
					sfind.skipdFns[lastIdx] = sfind.And(lastIdx, key)
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
