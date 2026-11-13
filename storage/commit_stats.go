package storage

import (
	"fmt"
	"time"
)

// CommitPhaseStats captures a successful write transaction breakdown.
// It is intended for benchmarks, debugging, and tests; nil observer means no
// additional reporting.
type CommitPhaseStats struct {
	TotalDuration        time.Duration
	TxLogWriteDuration   time.Duration
	TxLogSyncDuration    time.Duration
	DBWriteDuration      time.Duration
	DBSyncDuration       time.Duration
	JournalClearDuration time.Duration
	DirtyNodeCount       int
	DirtyPageCount       int
	DeletedPageCount     int
	JournalPageCount     int
	JournalBytes         int
	UsedFastClear        bool
}

type CommitObserver func(stats CommitPhaseStats)

func (s CommitPhaseStats) String() string {
	return fmt.Sprintf(
		"total=%s txlog_write=%s txlog_sync=%s db_write=%s db_sync=%s journal_clear=%s dirty_nodes=%d dirty_pages=%d deleted_pages=%d journal_pages=%d journal_bytes=%d fast_clear=%t",
		s.TotalDuration,
		s.TxLogWriteDuration,
		s.TxLogSyncDuration,
		s.DBWriteDuration,
		s.DBSyncDuration,
		s.JournalClearDuration,
		s.DirtyNodeCount,
		s.DirtyPageCount,
		s.DeletedPageCount,
		s.JournalPageCount,
		s.JournalBytes,
		s.UsedFastClear,
	)
}
