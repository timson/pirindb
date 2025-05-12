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

func (o *Options) MergeOptions(other *Options) *Options {
	if other == nil {
		return o
	}

	if other.FileMode != 0 {
		o.FileMode = other.FileMode
	}
	if other.PageSize != 0 {
		o.PageSize = other.PageSize
	}
	o.EnableRecovery = other.EnableRecovery // always override
	if other.TxLogPath != "" {
		o.TxLogPath = other.TxLogPath
	}

	return o
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
