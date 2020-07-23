package vfsindex

import "errors"

var (
	ErrInvalidTableName = errors.New("table name is invalid")
	ErrNotFoundFile     = errors.New("file not found")
	ErrInvalidIdxName   = errors.New("idx file name is invalid")
)
