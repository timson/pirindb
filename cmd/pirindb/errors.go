package main

import (
	"errors"
)

var (
	ErrConsistentHashNotFound = errors.New("consistent hash not found")
	ErrMaxTimeShiftExceeded   = errors.New("max time shift exceeded")
	ErrKeyNotFound            = errors.New("key not found")
	ErrInternalServerError    = errors.New("internal server error")
	ErrServiceUnavailable     = errors.New("service unavailable")
)
