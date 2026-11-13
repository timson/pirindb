package main

import (
	"errors"
	"strings"
)

var errRedisCommandExceedsTransactionLimit = errors.New("command exceeds the configured storage transaction limit")

func redisCommandDirtyEstimate(args [][]byte) int64 {
	if len(args) == 0 || !isRedisWriteCommand(strings.ToUpper(string(args[0]))) {
		return 0
	}
	// Payload bytes dominate blob writes. The fixed and per-argument allowances
	// cover B-tree paths, metadata, expiry, and slot-index changes without making
	// ordinary large values unusably conservative.
	estimate := int64(16 * 1024)
	for _, arg := range args {
		estimate += int64(len(arg) + 256)
	}
	return estimate
}

func redisCommandPayloadEstimate(args [][]byte) int64 {
	if len(args) == 0 || !isRedisWriteCommand(strings.ToUpper(string(args[0]))) {
		return 0
	}
	var estimate int64
	for _, arg := range args {
		estimate += int64(len(arg) + 256)
	}
	return estimate
}

func (srv *RedisServer) preflightRedisCommand(args [][]byte) error {
	if srv == nil || srv.DB == nil {
		return nil
	}
	opts := srv.DB.GetOptions()
	if opts == nil || opts.MaxTransactionBytes <= 0 {
		return nil
	}
	if redisCommandDirtyEstimate(args) > opts.MaxTransactionBytes {
		return errRedisCommandExceedsTransactionLimit
	}
	return nil
}

func (srv *RedisServer) preflightRedisQueuedCommands(commands [][][]byte) error {
	if srv == nil || srv.DB == nil {
		return nil
	}
	opts := srv.DB.GetOptions()
	if opts == nil || opts.MaxTransactionBytes <= 0 {
		return nil
	}
	estimate := int64(64 * 1024)
	hasWrite := false
	for _, args := range commands {
		commandEstimate := redisCommandPayloadEstimate(args)
		if commandEstimate == 0 {
			continue
		}
		hasWrite = true
		if estimate > opts.MaxTransactionBytes-commandEstimate {
			return errRedisCommandExceedsTransactionLimit
		}
		estimate += commandEstimate
	}
	if !hasWrite {
		return nil
	}
	if estimate > opts.MaxTransactionBytes {
		return errRedisCommandExceedsTransactionLimit
	}
	return nil
}
