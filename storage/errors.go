package storage

import (
	"errors"
)

var (
	ErrKeyTooLarge          = errors.New("key too large")
	ErrValueTooLarge        = errors.New("value too large")
	ErrNotEnoughSpace       = errors.New("not enough space to serialize node")
	ErrNoPagesLeft          = errors.New("no pages left")
	ErrBucketNotFound       = errors.New("bucket not found")
	ErrBucketExists         = errors.New("bucket already exists")
	ErrTxClosed             = errors.New("transaction closed")
	ErrWriteInRxTransaction = errors.New("write in read transaction")
	ErrNodeNotFound         = errors.New("node not found")
	ErrBlobTooLarge         = errors.New("blob too large")
	ErrUnknownItemType      = errors.New("unknown item type")
	ErrBadDbVersion         = errors.New("invalid db version")
	ErrBadDbName            = errors.New("invalid db name")
)
