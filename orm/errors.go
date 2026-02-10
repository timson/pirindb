package orm

import "errors"

var (
	ErrNotFound        = errors.New("orm: not found")
	ErrMultipleResults = errors.New("orm: multiple results")
)
