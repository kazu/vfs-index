package vfsindex

type Range struct {
	first uint64
	last  uint64
}

type RangeCur struct {
	Range
	cur int
}

const (
	MAX_IDX_CACHE      = 512
	MIN_NEGATIVE_CACHE = 8
)
