package vfsindex

import (
	"strconv"
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

type Match struct {
	key uint64
	col string
	mapInf
	FirstCol chan string
	done     chan string
}

func (m Match) Init() {
	m.col = ""
	m.mapInf = make(map[string]interface{})
	m.FirstCol = make(chan string, 2)
}

func (m Match) Get(k string) interface{} {

	return m.mapInf[k]
}

func (m Match) SearchEq(k string, tri uint64) bool {

	return m.key == tri
}

func (m Match) Search(k string, tri uint64) bool {

	return m.key <= tri
}

func SearchVal(s string) uint64 {
	tri, _ := strconv.ParseUint(EncodeTri(s)[0], 16, 64)
	return tri
}

/*
func(m Match) Get(k string) interface{} {
	if m.col == "" {
		m.FirstCol <- k
		m.col = k
	}
	<-m.done

}
*/
/*
func (cond *SearchCond) IsFront() bool {
	if cond.column == "" {
		return true
	}
	return false
}
*/

func (cond *SearchCond) StartCol(col string) {
	cond.idxCol = cond.idx.OpenCol(cond.flist, cond.table, col)
	cond.idxCol.caching()
	cond.column = col
}

func (cond *SearchCond) Searcher() *Searcher {
	if cond.column == "" {
		return nil
	}
	return cond.idxCol.Searcher()
}

/*
func (cond *SearchCond) filterOnFirst(fn func(m Match) bool) *SearchCond {

	var result bool
	m := Match{}
	m.Init()

	go func(){
		result = fn(m)
	}()
	col := <- m.FirstCol
	cond.StartCol(col)

	for {




	}


	return nil
}

func (cond *SearchCond) filter(fn func(m Match) bool) *SearchCond {

}

func (cond *SearchCond) Filter(fn func(m Match) bool) *SearchCond {

	if IsFront() {
		return filterOnFirst(fn)
	}
	return filter(fn)
}


func(cond *SearchCond) Filter(fn func(m Match) bool) *SearchCond {

	if cond.column == "" {
		fn(cond.m)
		cond.column = cond.m.col
	}

	if cond.idxCol == nil {
		cond.idxCol = cond.idx.OpenCol(cond.flist, cond.table, cond.column)
	}



	return nil
}

func (m Match) GetInt64(k string) int64 {

	v, ok := m.Get(k).(int64)
	if ok {
		return v
	}
	return 0
}


func (m Match) Get(k string) interface{} {

	m.col = k
	return nil

}

func (m Match) EqInt64(k string, v int64) bool {

	m.result = m.idxCol.RecordEqInt(int(v))
	if m.result != nil {
		return true
	}
	return false
}

*/
