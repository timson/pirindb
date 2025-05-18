package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/timson/pirindb/storage"
)

func DBStatus(db *storage.DB) *storage.DBStat {
	return db.Stat()
}

func Put(db *storage.DB, key, value string, clock *HLClock) error {
	var (
		incomingHLC HLC
		rawToStore  []byte
		err         error
	)

	if clock != nil {
		// normal mode: value is not prefixed with HLC
		incomingHLC = clock.Now()
		rawToStore = encodeValue(incomingHLC, []byte(value))
	} else {
		// replication/import mode: value is prefixed with HLC
		rawToStore = []byte(value)
		if incomingHLC, _, err = decodeValue(rawToStore); err != nil {
			return fmt.Errorf("put: invalid prefixed value: %w", err)
		}
	}

	tx := db.Begin(true)
	defer tx.Rollback()

	bucket, err := tx.CreateBucketIfNotExists(DBBucket)
	if err != nil {
		return err
	}

	// compare incomingHLC with storedHLC
	if storedRaw, found := bucket.Get([]byte(key)); found {
		if storedHLC, _, err := decodeValue(storedRaw); err == nil {
			if storedHLC.Compare(incomingHLC) >= 0 {
				// in db with higher or equal HLC, do nothing
				return nil
			}
		}
	}

	if err = bucket.Put([]byte(key), rawToStore); err != nil {
		return err
	}

	return tx.Commit()
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
	raw, found := bucket.Get([]byte(key))
	if !found {
		return "", false
	}

	_, payload, err := decodeValue(raw)
	if err != nil {
		return "", false
	}

	if err = tx.Commit(); err != nil {
		return "", false
	}
	return string(payload), true
}

func encodeValue(hlc HLC, payload []byte) []byte {
	buf := make([]byte, 12+len(payload))
	binary.BigEndian.PutUint64(buf[0:8], uint64(hlc.PhysicalTime))
	binary.BigEndian.PutUint32(buf[8:12], hlc.LogicalCounter)
	copy(buf[12:], payload)
	return buf
}

func decodeValue(raw []byte) (HLC, []byte, error) {
	if len(raw) < 12 {
		return HLC{}, nil, errors.New("value too short to contain HLC prefix")
	}
	hlc := HLC{
		PhysicalTime:   int64(binary.BigEndian.Uint64(raw[0:8])),
		LogicalCounter: binary.BigEndian.Uint32(raw[8:12]),
	}
	return hlc, raw[12:], nil
}
