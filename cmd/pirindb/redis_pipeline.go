package main

import (
	"strings"

	"github.com/timson/pirindb/storage"
)

const (
	defaultRedisPipelineMaxCommands = 256
	defaultRedisPipelineMaxBytes    = int64(8 * 1024 * 1024)
)

type redisBufferedResult struct {
	reply     redisReply
	future    *storage.CommitFuture
	args      [][]byte
	dbIndex   int
	closeConn bool
}

func (srv *RedisServer) groupCommitPipelineEnabled() bool {
	if srv == nil || srv.DB == nil {
		return false
	}
	opts := srv.DB.GetOptions()
	return opts != nil && opts.SyncPolicy == storage.SyncPolicyGroup
}

func (srv *RedisServer) redisPipelineLimits() (int, int64) {
	commands := defaultRedisPipelineMaxCommands
	bytes := defaultRedisPipelineMaxBytes
	if srv != nil && srv.Config != nil && srv.Config.Redis != nil {
		if srv.Config.Redis.PipelineMaxCommands > 0 {
			commands = srv.Config.Redis.PipelineMaxCommands
		}
		if srv.Config.Redis.PipelineMaxBytes > 0 {
			bytes = srv.Config.Redis.PipelineMaxBytes
		}
	}
	return commands, bytes
}

func redisCommandRequestBytes(args [][]byte) int64 {
	total := int64(16)
	for _, arg := range args {
		total += int64(len(arg) + 16)
	}
	return total
}

func (srv *RedisServer) readBufferedRedisCommands(resp *redisConn, first [][]byte) ([][][]byte, error) {
	maxCommands, maxBytes := srv.redisPipelineLimits()
	commands := make([][][]byte, 0, maxCommands)
	commands = append(commands, first)
	totalBytes := redisCommandRequestBytes(first)
	for len(commands) < maxCommands && totalBytes < maxBytes && resp.reader.Buffered() > 0 {
		args, err := resp.readCommand()
		if err != nil {
			return nil, err
		}
		commands = append(commands, args)
		totalBytes += redisCommandRequestBytes(args)
	}
	return commands, nil
}

func redisAsyncWriteEligible(resp *redisConn, args [][]byte) bool {
	if resp == nil || resp.pipelineActive || len(args) == 0 {
		return false
	}
	command := strings.ToUpper(string(args[0]))
	if !isRedisWriteCommand(command) {
		return false
	}
	switch command {
	case "BLPOP", "BRPOP", "BRPOPLPUSH", "FLUSHALL", "FLUSHDB", "LREM", "ZREMRANGEBYSCORE", "ZREMRANGEBYLEX":
		return false
	default:
		return true
	}
}

func (srv *RedisServer) executeAsyncRedisWrite(resp *redisConn, args [][]byte) redisBufferedResult {
	result := redisBufferedResult{args: args, dbIndex: resp.selectedDB}
	if err := validateRedisCommand(args); err != nil {
		result.reply = redisErrorReply{message: formatRedisError(err)}
		return result
	}
	unlock, err := srv.lockRedisCommandMutations(resp.selectedDB, args)
	if err != nil {
		result.reply = redisErrorReply{message: formatRedisError(err)}
		return result
	}

	allowAsking := resp.asking
	resp.asking = false
	move, slotCount, err := srv.clusterCopyingDeltaMoveForArgs(args)
	if err != nil {
		unlock()
		result.reply = redisErrorReply{message: formatRedisError(err)}
		return result
	}
	tx, err := srv.DB.BeginE(true)
	if err != nil {
		unlock()
		result.reply = redisErrorReply{message: formatRedisError(err)}
		return result
	}
	nowMs := srv.nowUnixMilli()
	reply, err := srv.executeCommand(resp.selectedDB, args, tx, nowMs, allowAsking)
	if err == nil && move != nil {
		err = captureClusterDeltaMutationsForCommandTx(tx, resp.selectedDB, args, move, slotCount, nowMs)
	}
	if err != nil {
		tx.Rollback()
		unlock()
		result.reply = redisErrorReply{message: formatRedisError(err)}
		return result
	}
	result.reply = reply
	result.future = tx.CommitAsync()
	unlock()
	return result
}

func (srv *RedisServer) executeBufferedRedisCommands(resp *redisConn, commands [][][]byte) (bool, error) {
	results := make([]redisBufferedResult, 0, len(commands))
	closeConn := false
	for _, args := range commands {
		if redisAsyncWriteEligible(resp, args) {
			results = append(results, srv.executeAsyncRedisWrite(resp, args))
			continue
		}
		reply, closeAfterReply, err := srv.dispatch(resp, args)
		if err != nil {
			reply = redisErrorReply{message: formatRedisError(err)}
		}
		results = append(results, redisBufferedResult{reply: reply, closeConn: closeAfterReply})
		if closeAfterReply {
			closeConn = true
			break
		}
	}

	for idx := range results {
		result := &results[idx]
		if result.future != nil {
			if err := result.future.Wait(); err != nil {
				result.reply = redisErrorReply{message: formatRedisError(err)}
			} else if len(result.args) > 0 {
				srv.notifyStandaloneRedisWrite(result.dbIndex, result.args)
				command := strings.ToUpper(string(result.args[0]))
				if command == "EXPIRE" || command == "PEXPIRE" || command == "SET" {
					srv.wakeExpiryWorker()
				}
				if command == "UNLINK" || command == "LTRIM" {
					srv.wakeGCWorker()
				}
			}
		}
		if result.reply != nil {
			if err := result.reply.writeRESP(resp); err != nil {
				return false, err
			}
		}
	}
	if err := resp.flush(); err != nil {
		return false, err
	}
	return closeConn, nil
}
