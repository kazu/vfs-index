package vfsindex

import (
	"errors"
	"fmt"
	"os"
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

	if l.Ftype != IdxFileType_Merge {
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
	f.Init()
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
		keyReocrds: root.Next().Index().IndexNum().Indexes(),
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

// Merge ... Join multiple index merged file
func (src *IndexFileMerged) Merge(srcs ...*IndexFileMerged) (dst *IndexFileMerged, e error) {

	dst = MakeMergedIndexFile()
	//cnt := 0

	keyID2Records := map[uint64]*query.RecordList{}

	midxs := make([]*IndexFileMerged, 0, len(srcs)+1)
	midxs = append(midxs, src)
	midxs = append(midxs, srcs...)

	for _, midx := range midxs {
		for _, kr := range midx.KeyRecords().All() {
			if keyID2Records[kr.Key().Uint64()] == nil {
				keyID2Records[kr.Key().Uint64()] = query.NewRecordList()
			}
			for _, rec := range kr.Records().All() {
				idx := keyID2Records[kr.Key().Uint64()].SearchIndex(func(r *query.Record) bool {
					if status, e := CompareRecord(rec, r); status >= RecordEQ && e == nil {
						return true
					}
					return false
				})

				switch SearchIndexToStatus(keyID2Records[kr.Key().Uint64()], rec, idx) {
				case FoundSameKeyRecord:
					continue
				case OuterKeyRecord:
					keyID2Records[kr.Key().Uint64()].SetAt(keyID2Records[kr.Key().Uint64()].Count(), rec)
					continue
				case InnerKeyRecord:
					keyID2Records[kr.Key().Uint64()].SetAt(keyID2Records[kr.Key().Uint64()].Count(), rec)
				}

				keyID2Records[kr.Key().Uint64()].SortBy(func(i, j int) bool {
					ir := keyID2Records[kr.Key().Uint64()].AtWihoutError(i)
					jr := keyID2Records[kr.Key().Uint64()].AtWihoutError(j)

					if status, e := CompareRecord(ir, jr); status == RecordLS && e == nil {
						return true
					}

					return false
				})
			}
		}
	}

	id2RecordsToKeyRecordList(dst.keyReocrds, keyID2Records)

	//dst.root.SetIndex(&query.Index{CommonNode:
	//dst/root.Index().IndexNum().SetIndexes
	return dst, nil
}

type HookRecordList struct {
	*query.RecordList
}

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
	//l.SetAt(idx, rec)
}
