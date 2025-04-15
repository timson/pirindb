package storage

import (
	"sync"
	"sync/atomic"
)

type DB struct {
	lock sync.RWMutex
	dal  *Dal
	TxN  atomic.Int32
}

type BucketStat struct {
	ItemsN     uint64
	BlobsN     uint64
	BytesInUse uint64
}

type DBStat struct {
	TotalPageNum  int                    // total number pages
	FreePageN     int                    // total number of free pages
	UsedPageN     int                    // total number of used pages
	ReleasedPageN int                    // total number of released pages
	FreeListPageN int                    // total number of pages allocated for freelist
	TotalDBSize   uint64                 // amount of pages * page size
	AvailDBSize   uint64                 // amount of free pages * page size
	UsedDBSize    uint64                 // amount of used pages * page size
	Buckets       map[string]*BucketStat //
	TxN           int                    // total number of started read transactions
}

func Open(path string, opts *Options) (*DB, error) {
	if opts == nil {
		opts = DefaultOptions()
	}
	dal, err := NewDal(path, opts)
	if err != nil {
		return nil, err
	}
	db := &DB{
		lock: sync.RWMutex{},
		dal:  dal,
	}
	return db, nil
}

func (db *DB) Close() error {
	return db.dal.Close()
}

func (db *DB) Begin(write bool) *Tx {
	if write {
		db.lock.Lock()
	} else {
		db.lock.RLock()
		db.TxN.Add(1)
	}
	return newTx(db, write)
}

func (db *DB) View(fn func(tx *Tx) error) error {
	tx := db.Begin(false)
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *DB) Update(fn func(tx *Tx) error) error {
	tx := db.Begin(true)
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *DB) Stat() *DBStat {
	freePages := int(db.dal.freelist.maxPages-db.dal.freelist.currentPage) + len(db.dal.freelist.releasedPages)
	freelistPages := len(db.dal.freelist.freelistPages)
	usedPages := int(db.dal.freelist.currentPage) + freelistPages
	releasedPages := len(db.dal.freelist.releasedPages)
	totalPages := int(db.dal.freelist.maxPages)

	bucketStats := make(map[string]*BucketStat)

	_ = db.View(func(tx *Tx) error {
		buckets := tx.Buckets()
		for _, bucketName := range buckets {
			bucket, err := tx.GetBucket(bucketName)
			if err != nil {
				continue
			}
			bucketStats[string(bucketName)] = &BucketStat{
				ItemsN:     bucket.itemsN,
				BlobsN:     bucket.blobsN,
				BytesInUse: bucket.bytesInUse,
			}
		}
		return nil
	})

	stat := &DBStat{
		TotalPageNum:  totalPages,
		FreePageN:     freePages,
		UsedPageN:     usedPages,
		ReleasedPageN: releasedPages,
		FreeListPageN: freelistPages,
		TotalDBSize:   uint64(totalPages) * db.dal.meta.pageSize,
		AvailDBSize:   uint64(freePages) * db.dal.meta.pageSize,
		UsedDBSize:    uint64(usedPages) * db.dal.meta.pageSize,
		Buckets:       bucketStats,
		TxN:           int(db.TxN.Load()),
	}
	return stat
}

func (db *DB) GetOptions() *Options {
	return db.dal.opts
}
