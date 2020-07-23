package vfsindex

import "os"

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

func FileExist(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil
}
