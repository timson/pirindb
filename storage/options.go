package storage

import (
	"os"
	"time"
)

type Options struct {
	FileMode               os.FileMode
	PageSize               uint64
	EnableRecovery         bool
	TxLogPath              string
	CommitObserver         CommitObserver
	SyncPolicy             SyncPolicy
	CheckpointTxThreshold  int
	GroupCommitTxThreshold int
	GroupCommitWindow      time.Duration
}

func DefaultOptions() *Options {
	return &Options{
		FileMode:               0600,
		PageSize:               BTreePageSize,
		EnableRecovery:         true,
		TxLogPath:              "", // default to db basename + ".tlog"
		CommitObserver:         nil,
		SyncPolicy:             SyncPolicyStrict,
		CheckpointTxThreshold:  64,
		GroupCommitTxThreshold: 16,
		GroupCommitWindow:      time.Millisecond,
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

func (o *Options) WithCommitObserver(observer CommitObserver) *Options {
	o.CommitObserver = observer
	return o
}

func (o *Options) WithSyncPolicy(policy SyncPolicy) *Options {
	o.SyncPolicy = policy
	return o
}

func (o *Options) WithCheckpointTxThreshold(threshold int) *Options {
	o.CheckpointTxThreshold = threshold
	return o
}

func (o *Options) WithGroupCommitTxThreshold(threshold int) *Options {
	o.GroupCommitTxThreshold = threshold
	return o
}

func (o *Options) WithGroupCommitWindow(window time.Duration) *Options {
	o.GroupCommitWindow = window
	return o
}
