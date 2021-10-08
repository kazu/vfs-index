package vfsindex

import (
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

func MakeMergedIndexFile() *IndexFileMerged {

	root := query.NewRoot()
	root.SetVersion(query.FromInt32(1))
	root.WithHeader()
	root.SetIndexType(query.FromByte(byte(vfs_schema.IndexIndexNum)))
	root.Flatten()

	idxNum := query.NewIndexNum()
	idxNum.Base = base.NewNoLayer(idxNum.Base)
	keyrecords := query.NewKeyRecordList()
	keyrecords.Base = base.NewNoLayer(keyrecords.Base)

	return &IndexFileMerged{
		State:      IdxMergedFileState_None,
		root:       root,
		keyReocrds: keyrecords,
	}

}

// Merge ... Join multiple index merged file
func (src *IndexFileMerged) Merge(srcs ...*IndexFileMerged) (dst *IndexFileMerged, e error) {

	dst = MakeMergedIndexFile()
	//cnt := 0

	keyID2Records := map[uint64]*query.RecordList{}

	isSameRec := func(src, dst *query.Record) bool {
		if src == nil || src.CommonNode == nil || dst == nil || dst.CommonNode == nil {
			return false
		}
		if src.FileId().Uint64() != dst.FileId().Uint64() {
			return false
		}
		if src.Offset().Int64() != dst.Offset().Int64() {
			return false
		}
		return true
	}
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
					if rec.FileId().Uint64() > r.FileId().Uint64() {
						return true
					}
					if rec.FileId().Uint64() == r.FileId().Uint64() && rec.Offset().Int64() >= r.Offset().Int64() {
						return true
					}
					return false
				})
				if idx <= 0 || idx == keyID2Records[kr.Key().Uint64()].Count() {
					keyID2Records[kr.Key().Uint64()].SetAt(keyID2Records[kr.Key().Uint64()].Count(), rec)
					continue
				}

				if orec := keyID2Records[kr.Key().Uint64()].AtWihoutError(idx); isSameRec(rec, orec) {
					continue
				}
				keyID2Records[kr.Key().Uint64()].SortBy(func(i, j int) bool {
					ir := keyID2Records[kr.Key().Uint64()].AtWihoutError(i)
					jr := keyID2Records[kr.Key().Uint64()].AtWihoutError(j)

					if ir.FileId().Uint64() < jr.FileId().Uint64() {
						return true
					}

					if ir.FileId().Uint64() == jr.FileId().Uint64() && ir.Offset().Int64() < jr.Offset().Int64() {
						return true
					}
					return false
				})
			}
		}
	}
	return nil, nil
}
