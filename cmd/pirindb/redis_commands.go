package main

import (
	"sort"
	"strings"
)

const redisServerVersion = "0.1"

type redisCommandSpec struct {
	Name     string
	Arity    int64
	Flags    []string
	FirstKey int64
	LastKey  int64
	KeyStep  int64
}

var redisCommandRegistry = buildRedisCommandRegistry([]redisCommandSpec{
	{Name: "ASKING", Arity: 1},
	{Name: "BF.ADD", Arity: 3}, {Name: "BF.EXISTS", Arity: 3}, {Name: "BF.MADD", Arity: -3}, {Name: "BF.MEXISTS", Arity: -3}, {Name: "BF.RESERVE", Arity: -4},
	{Name: "BLPOP", Arity: -3}, {Name: "BRPOPLPUSH", Arity: 4}, {Name: "BRPOP", Arity: -3},
	{Name: "CLIENT", Arity: -2}, {Name: "CLUSTER", Arity: -2}, {Name: "COMMAND", Arity: -1}, {Name: "CONFIG", Arity: -2},
	{Name: "DECR", Arity: 2}, {Name: "DECRBY", Arity: 3}, {Name: "DEL", Arity: -2}, {Name: "DISCARD", Arity: 1}, {Name: "DBSIZE", Arity: 1},
	{Name: "ECHO", Arity: 2}, {Name: "EXISTS", Arity: -2}, {Name: "EXPIRE", Arity: 3}, {Name: "EXEC", Arity: 1},
	{Name: "FLUSHALL", Arity: -1}, {Name: "FLUSHDB", Arity: -1},
	{Name: "GET", Arity: 2}, {Name: "GETSET", Arity: 3},
	{Name: "HDEL", Arity: -3}, {Name: "HELLO", Arity: -1}, {Name: "HEXISTS", Arity: 3}, {Name: "HGET", Arity: 3}, {Name: "HGETALL", Arity: 2}, {Name: "HKEYS", Arity: 2}, {Name: "HLEN", Arity: 2}, {Name: "HSET", Arity: -4}, {Name: "HVALS", Arity: 2},
	{Name: "INCR", Arity: 2}, {Name: "INCRBY", Arity: 3}, {Name: "INFO", Arity: -1},
	{Name: "KEYS", Arity: 2},
	{Name: "LINDEX", Arity: 3}, {Name: "LLEN", Arity: 2}, {Name: "LPOP", Arity: 2}, {Name: "LPUSH", Arity: -3}, {Name: "LRANGE", Arity: 4}, {Name: "LREM", Arity: 4}, {Name: "LSET", Arity: 4}, {Name: "LTRIM", Arity: 4},
	{Name: "MGET", Arity: -2}, {Name: "MSET", Arity: -3}, {Name: "MULTI", Arity: 1},
	{Name: "PERSIST", Arity: 2}, {Name: "PEXPIRE", Arity: 3}, {Name: "PING", Arity: -1},
	{Name: "PIRIN.BATCH", Arity: 1}, {Name: "PIRIN.DISCARD", Arity: 1}, {Name: "PIRIN.EXEC", Arity: 1},
	{Name: "PIRIN.CHECK", Arity: 1},
	{Name: "PTTL", Arity: 2}, {Name: "QUIT", Arity: 1},
	{Name: "RENAME", Arity: 3}, {Name: "RENAMENX", Arity: 3}, {Name: "RPOP", Arity: 2}, {Name: "RPOPLPUSH", Arity: 3}, {Name: "RPUSH", Arity: -3},
	{Name: "SCAN", Arity: -2}, {Name: "SELECT", Arity: 2}, {Name: "SET", Arity: -3},
	{Name: "TOPK.ADD", Arity: -3}, {Name: "TOPK.COUNT", Arity: -3}, {Name: "TOPK.INFO", Arity: 2}, {Name: "TOPK.INCRBY", Arity: -4}, {Name: "TOPK.LIST", Arity: -2}, {Name: "TOPK.QUERY", Arity: -3}, {Name: "TOPK.RESERVE", Arity: -3},
	{Name: "TTL", Arity: 2}, {Name: "TYPE", Arity: 2}, {Name: "UNLINK", Arity: -2},
	{Name: "ZADD", Arity: -4}, {Name: "ZCARD", Arity: 2}, {Name: "ZREM", Arity: -3}, {Name: "ZREMRANGEBYLEX", Arity: 4}, {Name: "ZREMRANGEBYSCORE", Arity: 4},
	{Name: "ZREVRANGEBYLEX", Arity: -4}, {Name: "ZREVRANGEBYSCORE", Arity: -4}, {Name: "ZRANGEBYLEX", Arity: -4}, {Name: "ZRANGEBYSCORE", Arity: -4}, {Name: "ZSCORE", Arity: 3},
})

var redisCommandByName = func() map[string]redisCommandSpec {
	result := make(map[string]redisCommandSpec, len(redisCommandRegistry))
	for _, spec := range redisCommandRegistry {
		result[spec.Name] = spec
	}
	return result
}()

func buildRedisCommandRegistry(specs []redisCommandSpec) []redisCommandSpec {
	for idx := range specs {
		specs[idx].Name = strings.ToUpper(specs[idx].Name)
		if isRedisWriteCommand(specs[idx].Name) {
			specs[idx].Flags = []string{"write"}
		} else {
			specs[idx].Flags = []string{"readonly"}
		}
		first, last, step := redisCommandKeySpec(specs[idx].Name)
		specs[idx].FirstKey = first
		specs[idx].LastKey = last
		specs[idx].KeyStep = step
	}
	sort.Slice(specs, func(i, j int) bool { return specs[i].Name < specs[j].Name })
	return specs
}

func redisCommandKeySpec(name string) (int64, int64, int64) {
	switch name {
	case "BLPOP", "BRPOP":
		return 1, -2, 1
	case "BRPOPLPUSH", "RENAME", "RENAMENX", "RPOPLPUSH":
		return 1, 2, 1
	case "DEL", "EXISTS", "MGET", "UNLINK":
		return 1, -1, 1
	case "MSET":
		return 1, -1, 2
	case "BF.ADD", "BF.EXISTS", "BF.MADD", "BF.MEXISTS", "BF.RESERVE",
		"TOPK.ADD", "TOPK.COUNT", "TOPK.INFO", "TOPK.INCRBY", "TOPK.LIST", "TOPK.QUERY", "TOPK.RESERVE",
		"DECR", "DECRBY", "EXPIRE", "GET", "GETSET", "HDEL", "HEXISTS", "HGET", "HGETALL", "HKEYS", "HLEN", "HSET", "HVALS",
		"INCR", "INCRBY", "LINDEX", "LLEN", "LPOP", "LPUSH", "LRANGE", "LREM", "LSET", "LTRIM", "RPOP", "RPUSH",
		"PERSIST", "PEXPIRE", "PTTL", "SET", "TTL", "TYPE", "ZADD", "ZCARD", "ZREM", "ZRANGEBYSCORE", "ZREVRANGEBYSCORE",
		"ZRANGEBYLEX", "ZREVRANGEBYLEX", "ZREMRANGEBYSCORE", "ZREMRANGEBYLEX", "ZSCORE":
		return 1, 1, 1
	default:
		return 0, 0, 0
	}
}

func redisCommandNames() []string {
	names := make([]string, 0, len(redisCommandRegistry))
	for _, spec := range redisCommandRegistry {
		names = append(names, spec.Name)
	}
	return names
}

func redisCommandMetadataReply(spec redisCommandSpec) redisReply {
	flags := make([]redisReply, 0, len(spec.Flags))
	for _, flag := range spec.Flags {
		flags = append(flags, redisBulkReply{value: []byte(flag)})
	}
	return redisArrayReply{values: []redisReply{
		redisBulkReply{value: []byte(strings.ToLower(spec.Name))},
		redisIntegerReply{value: spec.Arity},
		redisArrayReply{values: flags},
		redisIntegerReply{value: spec.FirstKey},
		redisIntegerReply{value: spec.LastKey},
		redisIntegerReply{value: spec.KeyStep},
		redisArrayReply{},
		redisArrayReply{},
		redisArrayReply{},
		redisArrayReply{},
	}}
}

func redisAllCommandMetadataReply() redisReply {
	values := make([]redisReply, 0, len(redisCommandRegistry))
	for _, spec := range redisCommandRegistry {
		values = append(values, redisCommandMetadataReply(spec))
	}
	return redisArrayReply{values: values}
}

func redisCommandInfoReply(names [][]byte) redisReply {
	values := make([]redisReply, 0, len(names))
	for _, rawName := range names {
		spec, found := redisCommandByName[strings.ToUpper(string(rawName))]
		if !found {
			values = append(values, redisBulkReply{null: true})
			continue
		}
		values = append(values, redisCommandMetadataReply(spec))
	}
	return redisArrayReply{values: values}
}
