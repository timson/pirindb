package storage

import "os"

type Options struct {
	FileMode       os.FileMode
	PageSize       uint64
	EnableRecovery bool
	TxLogPath      string
}

func DefaultOptions() *Options {
	return &Options{
		FileMode:       0600,
		PageSize:       BTreePageSize,
		EnableRecovery: true,
		TxLogPath:      "", // default to db basename + ".tlog"
	}
}

func (o *Options) WithRecovery(enable bool) *Options {
	o.EnableRecovery = enable
	return o
}

func (o *Options) WithPageSize(pageSize uint64) *Options {
	o.PageSize = pageSize
	return o
}

func (o *Options) WithTxLogPath(path string) *Options {
	o.TxLogPath = path
	return o
}

func (o *Options) WithFileMode(mode os.FileMode) *Options {
	o.FileMode = mode
	return o
}
