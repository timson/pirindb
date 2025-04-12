package main

import (
	"errors"
)

var (
	ErrConsistentHashNotFound = errors.New("consistent hash not found")
	ErrMaxTimeShiftExceeded   = errors.New("max time shift exceeded")
)
