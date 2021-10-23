package vfsindex

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/kazu/fbshelper/query/base"
	"github.com/kazu/vfs-index/query"
	"github.com/kazu/vfs-index/vfs_schema"
)

// IndexMergeFileType .. type of Merge Index File
type IndexMergeFileType int

const (
	IdxMergedFileState_None    IndexMergeFileType = 0
	IdxMergedFileState_Writing IndexMergeFileType = 1 << iota
	IdxMergedFileState_Writen
	IdxMergedFileState_Dirty
)

// IndexFileMerged ... accessor for merge type index file
type IndexFileMerged struct {
	*IndexFile
	State      IndexMergeFileType
	root       *query.Root
	keyReocrds *query.KeyRecordList
	err        error
	opt        OptionMergeIndexFile
}

// ToIndexFileMerged ... convert to IndexFileMerged
func ToIndexFileMerged(l *IndexFile) *IndexFileMerged {

	if l.Ftype != IdxFileTypeMerge {
		return nil
	}

	return &IndexFileMerged{
		IndexFile: l,
		State:     IdxMergedFileState_Writen,
	}
}

// OpenIndexFileMerged ... return IndexFileMerged
func OpenIndexFileMerged(table, column, path string, idxer *Indexer) *IndexFileMerged {
	sCond := idxer.On(table, ReaderColumn(column),
		MergeDuration(1*time.Minute),
		MergeOnSearch(true))
	f := NewIndexFile(sCond.IndexFile().Column(), path)
	return ToIndexFileMerged(f)
}

type OptionMergeIndexFile struct {
	tableName, columnName, pathName string
	idxer                           *Indexer
	first, last                     uint64
	isNum                           bool
	root                            *query.Root
	column                          *Column
}

func OptDefaultMergeIndexFile() OptionMergeIndexFile {

	return OptionMergeIndexFile{
		isNum: false,
	}

}

type OptMergeIndexFile func(opt *OptionMergeIndexFile)

func OptRoot(root *query.Root, c *Column) OptMergeIndexFile {
	return func(opt *OptionMergeIndexFile) {
		opt.root = root
		opt.column = c
	}
}

func OptPath(table, column, path string) OptMergeIndexFile {
	return func(opt *OptionMergeIndexFile) {
		opt.tableName = table
		opt.columnName = column
		opt.pathName = path
	}
}

func OptMergefileRange(first, last uint64) OptMergeIndexFile {
	return func(opt *OptionMergeIndexFile) {
		opt.first = first
		opt.last = last
	}
}

func MakeMergedIndexFile(opts ...OptMergeIndexFile) *IndexFileMerged {

	opt := OptionMergeIndexFile{}
	for _, optFn := range opts {
		optFn(&opt)
	}

	root := opt.root

	if root == nil {
		root = query.NewRoot()
		root.Base = base.NewNoLayer(root.Base)
		root.SetVersion(query.FromInt32(1))
		root.WithHeader()
		root.SetIndexType(query.FromByte(byte(vfs_schema.IndexIndexNum)))
		root.Flatten()

		idxNum := query.NewIndexNum()
		idxNum.Base = base.NewNoLayer(idxNum.Base)
		keyrecords := query.NewKeyRecordList()
		keyrecords.Base = base.NewNoLayer(keyrecords.Base)

		idxNum.SetIndexes(keyrecords)
		root.SetIndex(&query.Index{CommonNode: idxNum.CommonNode})
	}

	return &IndexFileMerged{
		State:      IdxMergedFileState_None,
		root:       root,
		keyReocrds: root.Index().IndexNum().Indexes(),
		opt:        opt,
	}
}

type ChainProc func() error

func (m *IndexFileMerged) Chain(procs ...ChainProc) {

	for _, proc := range procs {
		m.err = proc()
		if m.err != nil {
			return
		}
	}
}

func (m *IndexFileMerged) Write() (e error) {
	if m.err != nil {
		return m.err
	}

	vname := func(key uint64) string {

		if m.opt.column.IsNum {
			return toFname(key)
		}
		return fmt.Sprintf("%012x", key)
	}
	// FXIME get params from IndexFile
	// src.FetchInfo

	writtingPath := ColumnPathWithStatus(m.opt.column.TableDir(), m.opt.column.Name, m.opt.column.IsNum, vname(m.opt.first), vname(m.opt.last), RECORD_MERGING)
	path := ColumnPathWithStatus(m.opt.column.TableDir(), m.opt.column.Name, m.opt.column.IsNum, vname(m.opt.first), vname(m.opt.last), RECORD_MERGED)

	var io *os.File
	defer io.Close()

	m.Chain(
		func() error {
			io, m.err = os.Create(writtingPath)
			if m.err != nil {
				Log(LOG_WARN, "F:mergeIndex() cannot create... %s\n", writtingPath)
			}
			return m.err
		},
		func() (e error) {
			if len(m.root.Base.GetDiffs()) > 0 {
				m.root.Flatten()
			}
			_, e = io.Write(m.root.R(0))
			return
		},
		func() error {
			e := SafeRename(writtingPath, path)
			if e != nil {
				os.Remove(writtingPath)
				Log(LOG_DEBUG, "F: rename %s -> %s \n", writtingPath, path)
			}
			return e
		},
		func() error {
			Log(LOG_DEBUG, "S: renamed %s -> %s \n", writtingPath, path)
			return nil
		},
		func() error { return nil },
	)
	return m.err
}

// CompareStatus undocumented
type CompareStatus byte

const (
	RecordLS  CompareStatus = 0
	RecordEQ  CompareStatus = 1
	RecordGT  CompareStatus = 2
	RecordErr CompareStatus = 3
)

// CompareRecord undocumented
func CompareRecord(src, dst *query.Record) (CompareStatus, error) {
	if src == nil || src.CommonNode == nil || dst == nil || dst.CommonNode == nil {
		return RecordErr, errors.New("compare empty record")
	}
	fmt.Printf("src 0x%x.0x%x dst 0x%x,0x%x\n",
		src.FileId().Uint64(), src.Offset().Int64(),
		dst.FileId().Uint64(), dst.Offset().Int64())

	if src.FileId().Uint64() < dst.FileId().Uint64() {
		return RecordLS, nil
	}
	if src.FileId().Uint64() > dst.FileId().Uint64() {
		return RecordGT, nil
	}

	if src.Offset().Int64() < dst.Offset().Int64() {
		return RecordLS, nil
	}

	if src.Offset().Int64() > dst.Offset().Int64() {
		return RecordGT, nil
	}

	return RecordEQ, nil
}

const (
	// OuterKeyRecord ... not found status to search KeyRecord
	OuterKeyRecord = 1 << iota
	// FoundSameKeyRecord undocumented
	FoundSameKeyRecord
	// InnerKeyRecord undocumented
	InnerKeyRecord
)

// StatusKeyRecord undocumented
type StatusKeyRecord uint8

// SearchIndexToStatus ... return status to searchIndex Record
func SearchIndexToStatus(krList *query.RecordList, rec *query.Record, idx int) StatusKeyRecord {

	if idx <= 0 || idx == krList.Count() {
		return OuterKeyRecord
	}
	orec := krList.AtWihoutError(idx)

	if s, e := CompareRecord(rec, orec); e == nil && s == RecordEQ {
		return FoundSameKeyRecord
	}

	return InnerKeyRecord

}

func (m *IndexFileMerged) keyRecords() *query.KeyRecordList {
	if m.IndexFile != nil {
		return m.IndexFile.KeyRecords()
	}

	return m.root.Index().IndexNum().Indexes()

}

// Split ... split merged index file.
func (m *IndexFileMerged) Split(cnt int) (dsts []*IndexFileMerged, e error) {

	mKeyRecords := m.keyRecords()

	if cnt == 0 {
		cnt = mKeyRecords.Count() / 2
	}

	if mKeyRecords.Count() <= cnt {
		cnt = mKeyRecords.Count()
	}

	dstsKeyRecords := make([]*query.KeyRecordList, 0, 2)
	dstsKeyRecords = append(dstsKeyRecords, mKeyRecords.Range(0, cnt-1))
	if cnt < mKeyRecords.Count() {
		dstsKeyRecords = append(dstsKeyRecords, mKeyRecords.Range(cnt, mKeyRecords.Count()-1))
	}
	for _, dstKr := range dstsKeyRecords {
		dst := MakeMergedIndexFile()
		dstKr.Base = base.NewNoLayer(dstKr.Base)
		dstKr.Flatten()
		dst.root.Index().IndexNum().SetIndexes(dstKr)
		dsts = append(dsts, dst)
	}

	return
}

func MergeKerRecordList(oKrLists ...*query.KeyRecordList) (dst *query.KeyRecordList, e error) {

	dst = query.NewKeyRecordList()
	dst.Base = base.NewNoLayer(dst.Base)

	krLists := make([]*query.KeyRecordList, 0, len(oKrLists))
	// remove root
	for i := range oKrLists {

		if !oKrLists[i].InRoot() {
			continue
		}
		oKrLists[i].NodeList.ValueInfo = base.ValueInfo(oKrLists[i].List().InfoSlice())
		nKrList := query.NewKeyRecordList()
		nKrList.Base = base.NewNoLayer(nKrList.Base)
		nKrList.Base.Impl().Copy(oKrLists[i].Base.Dup().Impl(), oKrLists[i].NodeList.ValueInfo.Pos-4,
			oKrLists[i].NodeList.ValueInfo.Size+4, 0, 0)

		nKrList.NodeList.ValueInfo = base.ValueInfo(nKrList.List().InfoSlice())
		krLists = append(krLists, nKrList)

	}

	SortKrList := func(krList *query.KeyRecordList) error {
		return krList.SortBy(func(i, j int) bool {
			ikr, e := krList.At(i)
			if e != nil {
				panic("invalid list")
			}
			jkr, e := krList.At(j)
			if e != nil {
				panic("invalid list")
			}
			val := ikr.Key().Uint64() < jkr.Key().Uint64()
			if !val {
				return val
			}
			return val
		})
	}
	_ = SortKrList

	for i := range krLists {
		//e := SortKrList(krLists[i])
		if e != nil {
			return nil, errors.New("fail sort")
		}
		if i == 0 {
			//SortKrList(krLists[i])
			continue
		}

		prevLast, e := krLists[i-1].Last()
		if e != nil {
			return nil, fmt.Errorf("IndexFileMerged.Merge(): fail cannot get midxs[%d].keyRecords().Last()", i-1)
		}

		curFirst, e := krLists[i].First()
		if e != nil {
			return nil, fmt.Errorf("IndexFileMerged.Merge(): fail cannot get midxs[%d].keyRecords().First()", i)
		}

		if prevLast.Key().Uint64() < curFirst.Key().Uint64() {

			if e := dst.Add(*krLists[i-1]); e != nil {
				return nil, e
			}
			if i == len(krLists)-1 {
				if e := dst.Add(*krLists[i]); e != nil {
					return nil, e
				}
			}
			continue
		}
		j := krLists[i-1].SearchIndex(func(kr *query.KeyRecord) bool {
			return kr.Key().Uint64() >= curFirst.Key().Uint64()
		})
		pKrlists := []*query.KeyRecordList{
			krLists[i-1].Range(0, j), krLists[i-1].Range(j+1, krLists[i-1].Count()-1)}

		k := krLists[i].SearchIndex(func(kr *query.KeyRecord) bool {
			return kr.Key().Uint64() >= prevLast.Key().Uint64()
		})

		cKrList := []*query.KeyRecordList{
			krLists[i].Range(0, k), krLists[i].Range(k+1, krLists[i].Count()-1)}

		pKrlists[0].Flatten()

		for _, cKr := range cKrList[0].All() {
			l := pKrlists[1].SearchIndex(func(kr *query.KeyRecord) bool {
				return kr.Key().Uint64() >= cKr.Key().Uint64()
			})
			if pKrlists[1].AtWihoutError(l).Key().Equal(cKr.Key()) {
				pKrlists[1].AtWihoutError(l).Records().Add(*cKr.Records())
				continue
			}
			pKrlists[1].SetAt(pKrlists[1].Count(), cKr)
		}
		if i == len(krLists)-1 {
			return MergeKerRecordList(
				append(krLists[0:i-1],
					[]*query.KeyRecordList{pKrlists[0], pKrlists[1], cKrList[1]}...)...)

		}

		return MergeKerRecordList(
			append(krLists[0:i-1],
				append([]*query.KeyRecordList{pKrlists[0], pKrlists[1], cKrList[1]}, krLists[i+1:]...)...)...)

	}

	return

}

// Merge ... Join multiple index merged file
func (m *IndexFileMerged) Merge(srcs ...*IndexFileMerged) (dst *IndexFileMerged, e error) {

	krLists := make([]*query.KeyRecordList, 0, len(srcs)+1)
	krLists = append(krLists, m.keyRecords())
	for _, src := range srcs {
		krLists = append(krLists, src.keyRecords())
	}

	sort.Slice(krLists, func(i, j int) bool {
		ikr, e := krLists[i].First()
		if e != nil {
			return false
		}
		jkr, e := krLists[j].First()
		if e != nil {
			return false
		}

		return ikr.Key().Uint64() < jkr.Key().Uint64()
	})
	dst = MakeMergedIndexFile()
	//dupBase := m.root.Base.Dup()

	krlist, e := MergeKerRecordList(krLists...)
	if e != nil {
		return nil, e
	}
	//m.root.Base = dupBase

	dst.root.Index().IndexNum().SetIndexes(krlist)
	return
}

// HookRecordList ... to extend query.RecordList in this package.
type HookRecordList struct {
	*query.RecordList
}

// InsertWithKeepSort ... insert query.Record with order of sort.
func (hl HookRecordList) InsertWithKeepSort(rec *query.Record, idx int) {

	//l := (*query.RecordList)(hl)
	l := hl.RecordList

	switch SearchIndexToStatus(l, rec, idx) {
	case FoundSameKeyRecord:
		return
	case OuterKeyRecord:
		l.SetAt(l.Count(), rec)
		return
	case InnerKeyRecord:
		l.SetAt(l.Count(), rec)
	}

	for i := l.Count() - 1; i > idx; i-- {
		l.SwapAt(i-1, i)
		if i == l.Count()-1 {
			for _, rec := range l.All() {
				fmt.Printf("on swap first rec.0x%x\n", rec.Offset().Int64())
			}
		}

	}
}
