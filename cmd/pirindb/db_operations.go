package main

import (
	"github.com/timson/pirindb/storage"
)

func Status(db *storage.DB) *storage.DBStat {
	return db.Stat()
}

func Put(db *storage.DB, key string, value string) error {
	tx := db.Begin(true)
	defer tx.Rollback()
	bucket, err := tx.CreateBucketIfNotExists(DBBucket)
	if err != nil {
		return err
	}
	err = bucket.Put([]byte(key), []byte(value))
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

func Delete(db *storage.DB, key string) bool {
	tx := db.Begin(true)
	defer tx.Rollback()
	bucket, _ := tx.GetBucket(DBBucket)
	err := bucket.Remove([]byte(key))
	if err != nil {
		return false
	}
	err = tx.Commit()
	if err != nil {
		return false
	}
	return true
}

func Get(db *storage.DB, key string) (string, bool) {
	tx := db.Begin(false)
	defer tx.Rollback()
	bucket, err := tx.GetBucket(DBBucket)
	if err != nil {
		return "", false
	}
	value, isFound := bucket.Get([]byte(key))
	if !isFound {
		return "", false
	}
	err = tx.Commit()
	if err != nil {
		return "", false
	}
	return string(value), true
}
