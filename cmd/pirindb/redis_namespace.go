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
	slotCount         int
	stringBucket      []byte
	listMetaBucket    []byte
	listSysBucket     []byte
	listDataPrefix    []byte
	listDataBucket    []byte
	hashMetaBucket    []byte
	hashSysBucket     []byte
	hashDataPrefix    []byte
	hashDataBucket    []byte
	bloomMetaBucket   []byte
	bloomSysBucket    []byte
	bloomDataPrefix   []byte
	bloomDataBucket   []byte
	topkMetaBucket    []byte
	topkSysBucket     []byte
	topkDataPrefix    []byte
	topkDataBucket    []byte
	zsetMetaBucket    []byte
	zsetSysBucket     []byte
	zsetMemberPrefix  []byte
	zsetScorePrefix   []byte
	zsetMemberBucket  []byte
	zsetScoreBucket   []byte
	slotIndexBucket   []byte
	expireMetaBucket  []byte
	expireIndexBucket []byte
	keyMetaBucket     []byte
	keyMetaSysBucket  []byte
	gcQueueBucket     []byte
	gcSysBucket       []byte
	buildStateBucket  []byte
	waiterKeyPrefix   string
}

type redisDBUsage struct {
	keys       uint64
	blobs      uint64
	bytesInUse uint64
}

func redisNamespaceForDB(dbIndex int, slotCountOverride ...int) redisNamespace {
	suffix := strconv.Itoa(dbIndex)
	slotCount := 16384
	if len(slotCountOverride) > 0 && slotCountOverride[0] > 0 {
		slotCount = slotCountOverride[0]
	}
	return redisNamespace{
		dbIndex:           dbIndex,
		slotCount:         slotCount,
		stringBucket:      []byte("__redis_db_" + suffix + "__"),
		listMetaBucket:    []byte("__redis_db_" + suffix + "_list_meta__"),
		listSysBucket:     []byte("__redis_db_" + suffix + "_list_sys__"),
		listDataPrefix:    []byte("__redis_db_" + suffix + "_list_data__:"),
		listDataBucket:    []byte("__redis_db_" + suffix + "_list_data_v2__"),
		hashMetaBucket:    []byte("__redis_db_" + suffix + "_hash_meta__"),
		hashSysBucket:     []byte("__redis_db_" + suffix + "_hash_sys__"),
		hashDataPrefix:    []byte("__redis_db_" + suffix + "_hash_data__:"),
		hashDataBucket:    []byte("__redis_db_" + suffix + "_hash_data_v2__"),
		bloomMetaBucket:   []byte("__redis_db_" + suffix + "_bloom_meta__"),
		bloomSysBucket:    []byte("__redis_db_" + suffix + "_bloom_sys__"),
		bloomDataPrefix:   []byte("__redis_db_" + suffix + "_bloom_data__:"),
		bloomDataBucket:   []byte("__redis_db_" + suffix + "_bloom_data_v2__"),
		topkMetaBucket:    []byte("__redis_db_" + suffix + "_topk_meta__"),
		topkSysBucket:     []byte("__redis_db_" + suffix + "_topk_sys__"),
		topkDataPrefix:    []byte("__redis_db_" + suffix + "_topk_data__:"),
		topkDataBucket:    []byte("__redis_db_" + suffix + "_topk_data_v2__"),
		zsetMetaBucket:    []byte("__redis_db_" + suffix + "_zset_meta__"),
		zsetSysBucket:     []byte("__redis_db_" + suffix + "_zset_sys__"),
		zsetMemberPrefix:  []byte("__redis_db_" + suffix + "_zset_member__:"),
		zsetScorePrefix:   []byte("__redis_db_" + suffix + "_zset_score__:"),
		zsetMemberBucket:  []byte("__redis_db_" + suffix + "_zset_member_v2__"),
		zsetScoreBucket:   []byte("__redis_db_" + suffix + "_zset_score_v2__"),
		slotIndexBucket:   []byte("__redis_db_" + suffix + "_slot_idx__"),
		expireMetaBucket:  []byte("__redis_db_" + suffix + "_expire_meta__"),
		expireIndexBucket: []byte("__redis_db_" + suffix + "_expire_idx__"),
		keyMetaBucket:     []byte("__redis_db_" + suffix + "_key_meta__"),
		keyMetaSysBucket:  []byte("__redis_db_" + suffix + "_key_meta_sys__"),
		gcQueueBucket:     []byte("__redis_db_" + suffix + "_gc_queue__"),
		gcSysBucket:       []byte("__redis_db_" + suffix + "_gc_sys__"),
		buildStateBucket:  []byte("__redis_db_" + suffix + "_build_state__"),
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
		case bucketName == string(ns.listSysBucket), bucketName == string(ns.hashSysBucket), bucketName == string(ns.bloomSysBucket), bucketName == string(ns.topkSysBucket), bucketName == string(ns.zsetSysBucket), bucketName == string(ns.keyMetaBucket), bucketName == string(ns.keyMetaSysBucket), bucketName == string(ns.gcQueueBucket), bucketName == string(ns.gcSysBucket), bucketName == string(ns.buildStateBucket), bucketName == string(ns.listDataBucket), bucketName == string(ns.hashDataBucket), bucketName == string(ns.bloomDataBucket), bucketName == string(ns.topkDataBucket), bucketName == string(ns.zsetMemberBucket), bucketName == string(ns.zsetScoreBucket):
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
