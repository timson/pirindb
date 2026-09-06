package main

import (
	"bufio"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

const redisReferenceAddressEnv = "PIRINDB_REDIS_REFERENCE_ADDR"

func TestRedisSupportedSubsetAgainstReference(t *testing.T) {
	referenceAddress := os.Getenv(redisReferenceAddressEnv)
	if referenceAddress == "" {
		t.Skip("set PIRINDB_REDIS_REFERENCE_ADDR to run Redis differential tests")
	}

	_, pirinConn := setupTestRedisServer(t)
	referenceConn, err := net.Dial("tcp", referenceAddress)
	require.NoError(t, err)
	defer referenceConn.Close()

	pirinReader := bufio.NewReader(pirinConn)
	pirinWriter := bufio.NewWriter(pirinConn)
	referenceReader := bufio.NewReader(referenceConn)
	referenceWriter := bufio.NewWriter(referenceConn)

	type command struct {
		args []string
	}
	commands := []command{
		{[]string{"FLUSHDB"}},
		{[]string{"SET", "string", "value"}},
		{[]string{"GET", "string"}},
		{[]string{"EXISTS", "string", "missing"}},
		{[]string{"INCR", "counter"}},
		{[]string{"INCRBY", "counter", "4"}},
		{[]string{"RPUSH", "list", "a", "b", "c"}},
		{[]string{"LINDEX", "list", "-1"}},
		{[]string{"LRANGE", "list", "0", "-1"}},
		{[]string{"RPOPLPUSH", "list", "list"}},
		{[]string{"LRANGE", "list", "0", "-1"}},
		{[]string{"HSET", "hash", "a", "1", "b", "2"}},
		{[]string{"HGET", "hash", "b"}},
		{[]string{"HDEL", "hash", "a"}},
		{[]string{"ZADD", "zset", "2", "b", "1", "a"}},
		{[]string{"ZRANGEBYSCORE", "zset", "-inf", "+inf"}},
		{[]string{"ZSCORE", "zset", "b"}},
		{[]string{"PEXPIRE", "string", "0"}},
		{[]string{"EXISTS", "string"}},
	}

	for _, testCommand := range commands {
		writeRedisCommand(t, pirinWriter, testCommand.args...)
		writeRedisCommand(t, referenceWriter, testCommand.args...)
		pirinReply := readRedisReply(t, pirinReader)
		referenceReply := readRedisReply(t, referenceReader)
		require.Equal(t, referenceReply, pirinReply, "command %v", testCommand.args)
	}

	transaction := [][]string{
		{"SET", "tx-string", "value"},
		{"MULTI"},
		{"HSET", "tx-string", "field", "value"},
		{"SET", "tx-after-error", "committed"},
		{"EXEC"},
		{"GET", "tx-after-error"},
	}
	for _, args := range transaction {
		writeRedisCommand(t, pirinWriter, args...)
		writeRedisCommand(t, referenceWriter, args...)
		require.Equal(t, readRedisReply(t, referenceReader), readRedisReply(t, pirinReader), "transaction command %v", args)
	}
}
