package vfsindex

import "errors"

var (
	ErrInvalidTableName = errors.New("table name is invalid")
	ErrNotFoundFile     = errors.New("file not found")
)
