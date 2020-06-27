package indexer

type Range struct {
	first int
	last  int
	cur   int
}

func NewRange(s, e int) Range {
	return Range{first: s, last: e, cur: s}
}
