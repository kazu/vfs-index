package indexer

type Range struct {
	first uint64
	last  uint64
}

type RangeCur struct {
	Range
	cur int
}

/*
func NewRangeCur(s, e uint64) Range {
	return &RangeCur{first: s, last: e, cur: int(s)}
}
*/

const (
	MAX_IDX_CACHE      = 512
	MIN_NEGATIVE_CACHE = 8
)
