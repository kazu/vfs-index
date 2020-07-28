package vfsindex

import "errors"

var (
	ErrInvalidTableName = errors.New("table name is invalid")
	ErrNotFoundFile     = errors.New("file not found")
	ErrInvalidIdxName   = errors.New("idx file name is invalid")
	ErrNotHasColumn     = errors.New("record dosent have this column")
	ErrNotIndexDir      = errors.New("Indexfile must be Index top directory")
)
