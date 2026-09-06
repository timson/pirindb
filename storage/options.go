package storage

import (
	"fmt"
	"os"
	"time"
)

const (
	MinPageSize = 4096
	MaxPageSize = 32768
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
	MaxTransactionBytes    int64
	NodeCacheBytes         int64
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
		MaxTransactionBytes:    64 * 1024 * 1024,
		NodeCacheBytes:         32 * 1024 * 1024,
	}
}

func cloneOptions(opts *Options) *Options {
	if opts == nil {
		return DefaultOptions()
	}
	clone := *opts
	return &clone
}

func validatePageSize(pageSize uint64) error {
	if pageSize < MinPageSize || pageSize > MaxPageSize {
		return fmt.Errorf("%w: page size must be between %d and %d bytes", ErrInvalidOptions, MinPageSize, MaxPageSize)
	}
	if pageSize&(pageSize-1) != 0 {
		return fmt.Errorf("%w: page size %d is not a power of two", ErrInvalidOptions, pageSize)
	}
	return nil
}

func validateOptions(opts *Options) error {
	if opts == nil {
		return fmt.Errorf("%w: options are nil", ErrInvalidOptions)
	}
	if err := validatePageSize(opts.PageSize); err != nil {
		return err
	}
	if opts.SyncPolicy > SyncPolicyGroup {
		return fmt.Errorf("%w: unknown sync policy %d", ErrInvalidOptions, opts.SyncPolicy)
	}
	if opts.CheckpointTxThreshold < 0 {
		return fmt.Errorf("%w: checkpoint threshold cannot be negative", ErrInvalidOptions)
	}
	if opts.GroupCommitTxThreshold <= 0 {
		return fmt.Errorf("%w: group commit threshold must be positive", ErrInvalidOptions)
	}
	if opts.GroupCommitWindow < 0 {
		return fmt.Errorf("%w: group commit window cannot be negative", ErrInvalidOptions)
	}
	if opts.MaxTransactionBytes < int64(opts.PageSize) || opts.MaxTransactionBytes > 256*1024*1024 {
		return fmt.Errorf("%w: max transaction bytes must be between one page and 256 MiB", ErrInvalidOptions)
	}
	if opts.NodeCacheBytes < 0 || opts.NodeCacheBytes > OneGigabyte {
		return fmt.Errorf("%w: node cache bytes must be between zero and 1 GiB", ErrInvalidOptions)
	}
	return nil
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

func (o *Options) WithMaxTransactionBytes(maxBytes int64) *Options {
	o.MaxTransactionBytes = maxBytes
	return o
}

func (o *Options) WithNodeCacheBytes(cacheBytes int64) *Options {
	o.NodeCacheBytes = cacheBytes
	return o
}
