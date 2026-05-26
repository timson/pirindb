package main

import (
	"bytes"
	"strconv"
	"strings"

	"github.com/timson/pirindb/storage"
)

const (
	redisDatabaseMin   = 0
	redisDatabaseMax   = 9
	redisDatabaseCount = redisDatabaseMax + 1
)

type redisNamespace struct {
	dbIndex           int
	stringBucket      []byte
	listMetaBucket    []byte
	listSysBucket     []byte
	listDataPrefix    []byte
	hashMetaBucket    []byte
	hashSysBucket     []byte
	hashDataPrefix    []byte
	bloomMetaBucket   []byte
	bloomSysBucket    []byte
	bloomDataPrefix   []byte
	topkMetaBucket    []byte
	topkSysBucket     []byte
	topkDataPrefix    []byte
	zsetMetaBucket    []byte
	zsetSysBucket     []byte
	zsetMemberPrefix  []byte
	zsetScorePrefix   []byte
	expireMetaBucket  []byte
	expireIndexBucket []byte
	waiterKeyPrefix   string
}

type redisDBUsage struct {
	keys       uint64
	blobs      uint64
	bytesInUse uint64
}

func redisNamespaceForDB(dbIndex int) redisNamespace {
	suffix := strconv.Itoa(dbIndex)
	return redisNamespace{
		dbIndex:           dbIndex,
		stringBucket:      []byte("__redis_db_" + suffix + "__"),
		listMetaBucket:    []byte("__redis_db_" + suffix + "_list_meta__"),
		listSysBucket:     []byte("__redis_db_" + suffix + "_list_sys__"),
		listDataPrefix:    []byte("__redis_db_" + suffix + "_list_data__:"),
		hashMetaBucket:    []byte("__redis_db_" + suffix + "_hash_meta__"),
		hashSysBucket:     []byte("__redis_db_" + suffix + "_hash_sys__"),
		hashDataPrefix:    []byte("__redis_db_" + suffix + "_hash_data__:"),
		bloomMetaBucket:   []byte("__redis_db_" + suffix + "_bloom_meta__"),
		bloomSysBucket:    []byte("__redis_db_" + suffix + "_bloom_sys__"),
		bloomDataPrefix:   []byte("__redis_db_" + suffix + "_bloom_data__:"),
		topkMetaBucket:    []byte("__redis_db_" + suffix + "_topk_meta__"),
		topkSysBucket:     []byte("__redis_db_" + suffix + "_topk_sys__"),
		topkDataPrefix:    []byte("__redis_db_" + suffix + "_topk_data__:"),
		zsetMetaBucket:    []byte("__redis_db_" + suffix + "_zset_meta__"),
		zsetSysBucket:     []byte("__redis_db_" + suffix + "_zset_sys__"),
		zsetMemberPrefix:  []byte("__redis_db_" + suffix + "_zset_member__:"),
		zsetScorePrefix:   []byte("__redis_db_" + suffix + "_zset_score__:"),
		expireMetaBucket:  []byte("__redis_db_" + suffix + "_expire_meta__"),
		expireIndexBucket: []byte("__redis_db_" + suffix + "_expire_idx__"),
		waiterKeyPrefix:   "db:" + suffix + ":",
	}
}

func redisWaiterKey(dbIndex int, key []byte) string {
	return redisNamespaceForDB(dbIndex).waiterKeyPrefix + string(key)
}

func isRedisReservedBucketName(name []byte) bool {
	return bytes.HasPrefix(name, []byte("__redis_db_"))
}

func summarizeRedisDBUsage(statBuckets map[string]*storage.BucketStat, dbIndex int) redisDBUsage {
	ns := redisNamespaceForDB(dbIndex)
	usage := redisDBUsage{}

	for bucketName, bucketStat := range statBuckets {
		switch {
		case bucketName == string(ns.stringBucket):
			usage.keys += bucketStat.ItemsN
			usage.blobs += bucketStat.BlobsN
			usage.bytesInUse += bucketStat.BytesInUse
		case bucketName == string(ns.listMetaBucket):
			usage.keys += bucketStat.ItemsN
			usage.blobs += bucketStat.BlobsN
			usage.bytesInUse += bucketStat.BytesInUse
		case bucketName == string(ns.hashMetaBucket):
			usage.keys += bucketStat.ItemsN
			usage.blobs += bucketStat.BlobsN
			usage.bytesInUse += bucketStat.BytesInUse
		case bucketName == string(ns.bloomMetaBucket):
			usage.keys += bucketStat.ItemsN
			usage.blobs += bucketStat.BlobsN
			usage.bytesInUse += bucketStat.BytesInUse
		case bucketName == string(ns.topkMetaBucket):
			usage.keys += bucketStat.ItemsN
			usage.blobs += bucketStat.BlobsN
			usage.bytesInUse += bucketStat.BytesInUse
		case bucketName == string(ns.zsetMetaBucket):
			usage.keys += bucketStat.ItemsN
			usage.blobs += bucketStat.BlobsN
			usage.bytesInUse += bucketStat.BytesInUse
		case bucketName == string(ns.listSysBucket), bucketName == string(ns.hashSysBucket), bucketName == string(ns.bloomSysBucket), bucketName == string(ns.topkSysBucket), bucketName == string(ns.zsetSysBucket):
			usage.blobs += bucketStat.BlobsN
			usage.bytesInUse += bucketStat.BytesInUse
		case strings.HasPrefix(bucketName, string(ns.listDataPrefix)), strings.HasPrefix(bucketName, string(ns.hashDataPrefix)),
			strings.HasPrefix(bucketName, string(ns.bloomDataPrefix)), strings.HasPrefix(bucketName, string(ns.topkDataPrefix)),
			strings.HasPrefix(bucketName, string(ns.zsetMemberPrefix)), strings.HasPrefix(bucketName, string(ns.zsetScorePrefix)):
			usage.blobs += bucketStat.BlobsN
			usage.bytesInUse += bucketStat.BytesInUse
		}
	}

	return usage
}
