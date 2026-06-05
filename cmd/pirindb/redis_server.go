package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/timson/pirindb/storage"
	"io"
	"log/slog"
	"math"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type RedisServer struct {
	DB         *storage.DB
	Logger     *slog.Logger
	Config     *Config
	Cluster    *ClusterManager
	clusterErr error
	listener   net.Listener
	wg         sync.WaitGroup
	conns      sync.Map
	waitMu     sync.Mutex
	waiters    map[string]map[chan struct{}]struct{}
	stopOnce   sync.Once
	stopCh     chan struct{}
	nowFn      func() time.Time
	randFn     func() uint64
}

type redisConn struct {
	reader         *bufio.Reader
	writer         *bufio.Writer
	selectedDB     int
	asking         bool
	pipelineActive bool
	pipelineAsking bool
	queuedCommands [][][]byte
}

type redisScanOptions struct {
	cursor  string
	pattern string
	count   int
}

type redisReply interface {
	writeRESP(conn *redisConn) error
}

type redisSimpleStringReply struct {
	value string
}

type redisErrorReply struct {
	message string
}

type redisIntegerReply struct {
	value int64
}

type redisBulkReply struct {
	value []byte
	null  bool
}

type redisArrayReply struct {
	values []redisReply
}

var supportedRedisCommandNames = []string{
	"ASKING",
	"BF.ADD",
	"BF.EXISTS",
	"BF.MADD",
	"BF.MEXISTS",
	"BF.RESERVE",
	"BLPOP",
	"BRPOPLPUSH",
	"BRPOP",
	"CLUSTER",
	"CONFIG",
	"COMMAND",
	"DECR",
	"DECRBY",
	"DEL",
	"DBSIZE",
	"DISCARD",
	"EXISTS",
	"EXPIRE",
	"EXEC",
	"FLUSHALL",
	"FLUSHDB",
	"GET",
	"GETSET",
	"HDEL",
	"HEXISTS",
	"HGET",
	"HGETALL",
	"HKEYS",
	"HLEN",
	"HSET",
	"HVALS",
	"INCR",
	"INCRBY",
	"INFO",
	"KEYS",
	"LINDEX",
	"LLEN",
	"LPOP",
	"LPUSH",
	"LRANGE",
	"LREM",
	"LSET",
	"LTRIM",
	"MGET",
	"MSET",
	"MULTI",
	"PERSIST",
	"PEXPIRE",
	"PING",
	"PIPELINE",
	"PTTL",
	"QUIT",
	"RENAME",
	"RENAMENX",
	"RPOP",
	"RPOPLPUSH",
	"RPUSH",
	"SCAN",
	"SELECT",
	"SET",
	"SSCAN",
	"TTL",
	"TOPK.ADD",
	"TOPK.COUNT",
	"TOPK.INFO",
	"TOPK.INCRBY",
	"TOPK.LIST",
	"TOPK.QUERY",
	"TOPK.RESERVE",
	"TYPE",
	"UNLINK",
	"ZADD",
	"ZCARD",
	"ZREM",
	"ZREMRANGEBYLEX",
	"ZREMRANGEBYSCORE",
	"ZREVRANGEBYLEX",
	"ZREVRANGEBYSCORE",
	"ZRANGEBYLEX",
	"ZRANGEBYSCORE",
	"ZSCORE",
}

func NewRedisServer(cfg *Config, db *storage.DB, logger *slog.Logger) *RedisServer {
	cluster, clusterErr := NewClusterManager(cfg, db, logger)
	return &RedisServer{
		DB:         db,
		Logger:     logger,
		Config:     cfg,
		Cluster:    cluster,
		clusterErr: clusterErr,
		waiters:    make(map[string]map[chan struct{}]struct{}),
		stopCh:     make(chan struct{}),
		nowFn:      time.Now,
		randFn: func() uint64 {
			return uint64(rand.Int63())
		},
	}
}

func (srv *RedisServer) Start() error {
	if srv.clusterErr != nil {
		return srv.clusterErr
	}
	if srv.Config == nil || srv.Config.Redis == nil || !srv.Config.Redis.Enabled {
		return nil
	}

	addr := fmt.Sprintf("%s:%d", srv.Config.Redis.Host, srv.Config.Redis.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	srv.listener = listener
	srv.Logger.Info("started listening", "protocol", "redis", "port", srv.Config.Redis.Port, "host", srv.Config.Redis.Host)

	srv.wg.Add(1)
	go srv.acceptLoop()
	return nil
}

func (srv *RedisServer) Stop() error {
	if srv.listener == nil {
		return nil
	}
	srv.stopOnce.Do(func() {
		close(srv.stopCh)
	})

	err := srv.listener.Close()
	srv.conns.Range(func(key, _ any) bool {
		conn := key.(net.Conn)
		_ = conn.Close()
		return true
	})
	srv.wg.Wait()

	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

func (srv *RedisServer) acceptLoop() {
	defer srv.wg.Done()

	for {
		conn, err := srv.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			srv.Logger.Error("Redis accept error", "error", err)
			return
		}

		srv.conns.Store(conn, struct{}{})
		srv.wg.Add(1)
		go func() {
			defer srv.wg.Done()
			defer srv.conns.Delete(conn)
			defer func() {
				_ = conn.Close()
			}()
			srv.handleConn(conn)
		}()
	}
}

func (srv *RedisServer) handleConn(conn net.Conn) {
	resp := &redisConn{
		reader: bufio.NewReaderSize(conn, 32*1024),
		writer: bufio.NewWriterSize(conn, 32*1024),
	}

	for {
		args, err := resp.readCommand()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			_ = resp.writeError("ERR " + err.Error())
			_ = resp.flush()
			return
		}

		reply, closeConn, cmdErr := srv.dispatch(resp, args)
		if cmdErr != nil {
			reply = redisErrorReply{message: formatRedisError(cmdErr)}
		}
		if reply != nil {
			if err = reply.writeRESP(resp); err != nil {
				return
			}
		}
		if err = resp.flush(); err != nil {
			return
		}
		if closeConn {
			return
		}
	}
}

func (srv *RedisServer) dispatch(resp *redisConn, args [][]byte) (redisReply, bool, error) {
	if len(args) == 0 {
		return nil, false, errors.New("empty command")
	}

	command := strings.ToUpper(string(args[0]))
	if resp.pipelineActive {
		switch command {
		case "EXEC":
			if len(args) != 1 {
				return nil, false, errors.New("wrong number of arguments for 'exec' command")
			}
			replies, err := srv.executePipeline(resp.selectedDB, resp.queuedCommands, resp.pipelineAsking)
			resp.resetPipeline()
			if err != nil {
				return nil, false, err
			}
			return redisArrayReply{values: replies}, false, nil
		case "DISCARD":
			if len(args) != 1 {
				return nil, false, errors.New("wrong number of arguments for 'discard' command")
			}
			resp.resetPipeline()
			return redisSimpleStringReply{value: "OK"}, false, nil
		case "PIPELINE", "MULTI":
			return nil, false, errors.New("pipeline already started")
		case "QUIT":
			if len(args) != 1 {
				return nil, false, errors.New("wrong number of arguments for 'quit' command")
			}
			resp.resetPipeline()
			return redisSimpleStringReply{value: "OK"}, true, nil
		default:
			if err := validateQueuedCommand(args); err != nil {
				return nil, false, err
			}
			resp.queueCommand(args)
			return redisSimpleStringReply{value: "QUEUED"}, false, nil
		}
	}

	switch command {
	case "ASKING":
		if len(args) != 1 {
			return nil, false, errors.New("wrong number of arguments for 'asking' command")
		}
		resp.asking = true
		return redisSimpleStringReply{value: "OK"}, false, nil
	case "PIPELINE", "MULTI":
		if len(args) != 1 {
			return nil, false, fmt.Errorf("wrong number of arguments for '%s' command", strings.ToLower(command))
		}
		if resp.asking {
			resp.pipelineAsking = true
			resp.asking = false
		}
		resp.pipelineActive = true
		resp.queuedCommands = resp.queuedCommands[:0]
		return redisSimpleStringReply{value: "OK"}, false, nil
	case "EXEC":
		return nil, false, errors.New("exec without pipeline")
	case "DISCARD":
		return nil, false, errors.New("discard without pipeline")
	case "QUIT":
		if len(args) != 1 {
			return nil, false, errors.New("wrong number of arguments for 'quit' command")
		}
		return redisSimpleStringReply{value: "OK"}, true, nil
	case "SELECT":
		selectedDB, err := parseRedisSelectDB(args)
		if err != nil {
			return nil, false, err
		}
		resp.selectedDB = selectedDB
		return redisSimpleStringReply{value: "OK"}, false, nil
	default:
		allowAsking := resp.asking
		resp.asking = false
		reply, err := srv.executeCommand(resp.selectedDB, args, nil, srv.nowUnixMilli(), allowAsking)
		return reply, false, err
	}
}

func isRedisDeltaCaptureWriteCommand(command string) bool {
	switch command {
	case "BF.ADD", "BF.MADD", "BF.RESERVE",
		"TOPK.ADD", "TOPK.INCRBY", "TOPK.RESERVE",
		"DEL", "UNLINK", "EXPIRE", "PEXPIRE", "PERSIST",
		"SET", "MSET", "GETSET", "INCR", "INCRBY", "DECR", "DECRBY",
		"LPUSH", "RPUSH", "LPOP", "RPOP", "LSET", "LREM", "LTRIM", "RPOPLPUSH",
		"HSET", "HDEL",
		"RENAME", "RENAMENX",
		"ZADD", "ZREM", "ZREMRANGEBYSCORE", "ZREMRANGEBYLEX":
		return true
	default:
		return false
	}
}

func (srv *RedisServer) clusterCopyingDeltaMoveForArgs(args [][]byte) (*clusterPendingMove, int, error) {
	if srv.Cluster == nil || len(args) == 0 {
		return nil, 0, nil
	}
	command := strings.ToUpper(string(args[0]))
	if !isRedisDeltaCaptureWriteCommand(command) {
		return nil, 0, nil
	}
	state := srv.Cluster.Snapshot()
	if state == nil || !state.Rebalancing || state.PendingMove == nil {
		return nil, 0, nil
	}
	if state.PendingMove.Stage != clusterMoveStageCopying || state.PendingMove.SourceNodeID != srv.Cluster.LocalNodeID {
		return nil, 0, nil
	}
	keys, err := redisCommandKeys(args)
	if err != nil {
		return nil, 0, err
	}
	if len(keys) == 0 {
		return nil, 0, nil
	}
	route, err := srv.Cluster.RouteKeys(keys)
	if err != nil {
		return nil, 0, err
	}
	if route == nil || !route.hasKey {
		return nil, 0, nil
	}
	move := state.PendingMoveForSlot(route.slot)
	if move == nil || move.ID == "" {
		return nil, 0, nil
	}
	cloned := *move
	return &cloned, state.SlotCount, nil
}

func (srv *RedisServer) clusterCopyingDeltaMoveForPipeline(queuedCommands [][][]byte) (*clusterPendingMove, int, error) {
	if srv.Cluster == nil {
		return nil, 0, nil
	}
	state := srv.Cluster.Snapshot()
	if state == nil || !state.Rebalancing || state.PendingMove == nil {
		return nil, 0, nil
	}
	if state.PendingMove.Stage != clusterMoveStageCopying || state.PendingMove.SourceNodeID != srv.Cluster.LocalNodeID {
		return nil, 0, nil
	}
	combinedKeys := make([][]byte, 0)
	for _, args := range queuedCommands {
		keys, err := redisCommandKeys(args)
		if err != nil {
			return nil, 0, err
		}
		combinedKeys = append(combinedKeys, keys...)
	}
	if len(combinedKeys) == 0 {
		return nil, 0, nil
	}
	route, err := srv.Cluster.RouteKeys(combinedKeys)
	if err != nil {
		return nil, 0, err
	}
	if route == nil || !route.hasKey {
		return nil, 0, nil
	}
	move := state.PendingMoveForSlot(route.slot)
	if move == nil || move.ID == "" {
		return nil, 0, nil
	}
	cloned := *move
	return &cloned, state.SlotCount, nil
}

func (srv *RedisServer) notifyStandaloneRedisWrite(selectedDB int, args [][]byte) {
	if len(args) == 0 {
		return
	}
	switch strings.ToUpper(string(args[0])) {
	case "LPUSH", "RPUSH":
		srv.notifyListWaiters(selectedDB, args[1])
	case "RPOPLPUSH", "RENAME", "RENAMENX":
		srv.notifyListWaiters(selectedDB, args[2])
	}
}

func (srv *RedisServer) executeStandaloneWriteWithClusterDelta(selectedDB int, args [][]byte, nowMs int64, allowAsking bool, move *clusterPendingMove, slotCount int) (redisReply, error) {
	tx := srv.DB.Begin(true)
	defer tx.Rollback()

	reply, err := srv.executeCommand(selectedDB, args, tx, nowMs, allowAsking)
	if err != nil {
		return nil, err
	}
	if err = captureClusterDeltaMutationsForCommandTx(tx, selectedDB, args, move, slotCount, nowMs); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	srv.notifyStandaloneRedisWrite(selectedDB, args)
	return reply, nil
}

func (srv *RedisServer) executePipeline(selectedDB int, queuedCommands [][][]byte, allowAsking bool) ([]redisReply, error) {
	if err := srv.ensurePipelineClusterRoute(selectedDB, queuedCommands, allowAsking); err != nil {
		return nil, err
	}

	writeTx := pipelineNeedsWriteTx(queuedCommands)
	var (
		deltaMove      *clusterPendingMove
		deltaSlotCount int
		err            error
	)
	if writeTx {
		deltaMove, deltaSlotCount, err = srv.clusterCopyingDeltaMoveForPipeline(queuedCommands)
		if err != nil {
			return nil, err
		}
	}
	tx := srv.DB.Begin(writeTx)
	defer tx.Rollback()

	nowMs := srv.nowUnixMilli()
	replies := make([]redisReply, 0, len(queuedCommands))
	for idx, args := range queuedCommands {
		reply, err := srv.executeCommand(selectedDB, args, tx, nowMs, allowAsking)
		if err != nil {
			return nil, fmt.Errorf("pipeline aborted at command %d (%s): %w", idx+1, strings.ToLower(string(args[0])), err)
		}
		if deltaMove != nil && isRedisDeltaCaptureWriteCommand(strings.ToUpper(string(args[0]))) {
			if err = captureClusterDeltaMutationsForCommandTx(tx, selectedDB, args, deltaMove, deltaSlotCount, nowMs); err != nil {
				return nil, err
			}
		}
		replies = append(replies, reply)
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	srv.notifyPipelineListWrites(selectedDB, queuedCommands)
	return replies, nil
}

func pipelineNeedsWriteTx(queuedCommands [][][]byte) bool {
	for _, args := range queuedCommands {
		if len(args) == 0 {
			continue
		}
		switch strings.ToUpper(string(args[0])) {
		case "SET", "GETSET", "DEL", "UNLINK", "MSET", "INCR", "INCRBY", "DECR", "DECRBY", "LPOP", "LPUSH", "LREM", "LSET", "LTRIM", "RPOP", "RPUSH", "RPOPLPUSH", "HSET", "HDEL", "FLUSHALL", "FLUSHDB", "EXPIRE", "PEXPIRE", "PERSIST", "RENAME", "RENAMENX", "ZADD", "ZREM", "ZREMRANGEBYSCORE", "ZREMRANGEBYLEX", "BF.ADD", "BF.MADD", "BF.RESERVE", "TOPK.ADD", "TOPK.INCRBY", "TOPK.RESERVE":
			return true
		}
	}
	return false
}

func validateQueuedCommand(args [][]byte) error {
	if len(args) == 0 {
		return errors.New("empty command")
	}

	command := strings.ToUpper(string(args[0]))
	switch command {
	case "INFO", "BLPOP", "BRPOP", "BRPOPLPUSH":
		return fmt.Errorf("%s is not supported inside pipeline", strings.ToLower(command))
	case "ASKING", "PIPELINE", "MULTI", "EXEC", "DISCARD", "QUIT", "SELECT":
		return fmt.Errorf("command '%s' is not queueable", strings.ToLower(command))
	default:
		return validateRedisCommand(args)
	}
}

func validateRedisCommand(args [][]byte) error {
	if len(args) == 0 {
		return errors.New("empty command")
	}

	command := strings.ToUpper(string(args[0]))
	switch command {
	case "ASKING":
		if len(args) != 1 {
			return errors.New("wrong number of arguments for 'asking' command")
		}
	case "BF.ADD", "BF.EXISTS":
		if len(args) != 3 {
			return fmt.Errorf("wrong number of arguments for '%s' command", strings.ToLower(command))
		}
	case "BF.MADD", "BF.MEXISTS":
		if len(args) < 3 {
			return fmt.Errorf("wrong number of arguments for '%s' command", strings.ToLower(command))
		}
	case "BF.RESERVE":
		_, _, err := parseRedisBloomReserveArgs(args)
		return err
	case "TOPK.ADD", "TOPK.COUNT", "TOPK.QUERY":
		if len(args) < 3 {
			return fmt.Errorf("wrong number of arguments for '%s' command", strings.ToLower(command))
		}
	case "TOPK.INFO":
		if len(args) != 2 {
			return errors.New("wrong number of arguments for 'topk.info' command")
		}
	case "TOPK.INCRBY":
		_, _, _, err := parseRedisTopKIncrByArgs(args)
		return err
	case "TOPK.LIST":
		_, _, err := parseRedisTopKListArgs(args)
		return err
	case "TOPK.RESERVE":
		_, _, _, _, _, err := parseRedisTopKReserveArgs(args)
		return err
	case "BLPOP", "BRPOP":
		_, _, _, err := parseRedisBlockingPopArgs(args)
		return err
	case "BRPOPLPUSH":
		_, _, _, _, err := parseRedisBlockingMoveArgs(args)
		return err
	case "PING":
		if len(args) > 2 {
			return errors.New("wrong number of arguments for 'ping' command")
		}
	case "CLUSTER":
		if len(args) < 2 {
			return errors.New("wrong number of arguments for 'cluster' command")
		}
		switch strings.ToUpper(string(args[1])) {
		case "KEYSLOT":
			if len(args) != 3 {
				return errors.New("wrong number of arguments for 'cluster|keyslot' command")
			}
		case "SLOTS", "SHARDS":
			if len(args) != 2 {
				return fmt.Errorf("wrong number of arguments for 'cluster|%s' command", strings.ToLower(string(args[1])))
			}
		default:
			return fmt.Errorf("unsupported cluster subcommand '%s'", strings.ToLower(string(args[1])))
		}
	case "COMMAND":
		if len(args) > 2 {
			return errors.New("wrong number of arguments for 'command' command")
		}
		if len(args) == 2 {
			switch strings.ToUpper(string(args[1])) {
			case "LIST", "COUNT":
			default:
				return fmt.Errorf("unsupported command subcommand '%s'", strings.ToLower(string(args[1])))
			}
		}
	case "CONFIG":
		if len(args) < 2 {
			return errors.New("wrong number of arguments for 'config' command")
		}
		switch strings.ToUpper(string(args[1])) {
		case "GET":
			if len(args) != 3 {
				return errors.New("wrong number of arguments for 'config|get' command")
			}
		case "HELP", "RESETSTAT", "REWRITE":
			if len(args) != 2 {
				return fmt.Errorf("wrong number of arguments for 'config|%s' command", strings.ToLower(string(args[1])))
			}
		case "SET":
			if len(args) != 4 {
				return errors.New("wrong number of arguments for 'config|set' command")
			}
		default:
			return fmt.Errorf("unsupported config subcommand '%s'", strings.ToLower(string(args[1])))
		}
	case "INFO":
		if len(args) > 2 {
			return errors.New("wrong number of arguments for 'info' command")
		}
	case "DBSIZE":
		if len(args) != 1 {
			return errors.New("wrong number of arguments for 'dbsize' command")
		}
	case "EXPIRE":
		_, _, err := parseRedisExpireArgs(args, 1000)
		return err
	case "EXISTS":
		if len(args) < 2 {
			return errors.New("wrong number of arguments for 'exists' command")
		}
	case "PEXPIRE":
		_, _, err := parseRedisExpireArgs(args, 1)
		return err
	case "FLUSHALL":
		if len(args) != 1 {
			return errors.New("wrong number of arguments for 'flushall' command")
		}
	case "FLUSHDB":
		if len(args) > 2 {
			return errors.New("wrong number of arguments for 'flushdb' command")
		}
		if len(args) == 2 {
			switch strings.ToUpper(string(args[1])) {
			case "ASYNC", "SYNC":
			default:
				return fmt.Errorf("unsupported flush mode '%s'", strings.ToLower(string(args[1])))
			}
		}
	case "DECR", "INCR":
		if len(args) != 2 {
			return fmt.Errorf("wrong number of arguments for '%s' command", strings.ToLower(command))
		}
	case "DECRBY", "INCRBY":
		if len(args) != 3 {
			return fmt.Errorf("wrong number of arguments for '%s' command", strings.ToLower(command))
		}
		_, err := parseRedisInt64(args[2])
		return err
	case "GET":
		if len(args) != 2 {
			return errors.New("wrong number of arguments for 'get' command")
		}
	case "GETSET":
		if len(args) != 3 {
			return errors.New("wrong number of arguments for 'getset' command")
		}
	case "TYPE":
		if len(args) != 2 {
			return errors.New("wrong number of arguments for 'type' command")
		}
	case "HDEL":
		if len(args) < 3 {
			return errors.New("wrong number of arguments for 'hdel' command")
		}
	case "HEXISTS", "HGET":
		if len(args) != 3 {
			return fmt.Errorf("wrong number of arguments for '%s' command", strings.ToLower(command))
		}
	case "HGETALL", "HKEYS", "HLEN", "HVALS":
		if len(args) != 2 {
			return fmt.Errorf("wrong number of arguments for '%s' command", strings.ToLower(command))
		}
	case "LPOP", "LLEN", "RPOP":
		if len(args) != 2 {
			return fmt.Errorf("wrong number of arguments for '%s' command", strings.ToLower(command))
		}
	case "LINDEX":
		if len(args) != 3 {
			return errors.New("wrong number of arguments for 'lindex' command")
		}
		_, err := parseRedisInt64(args[2])
		return err
	case "LRANGE", "LTRIM":
		if len(args) != 4 {
			return fmt.Errorf("wrong number of arguments for '%s' command", strings.ToLower(command))
		}
		if _, err := parseRedisInt64(args[2]); err != nil {
			return err
		}
		_, err := parseRedisInt64(args[3])
		return err
	case "LREM":
		if len(args) != 4 {
			return errors.New("wrong number of arguments for 'lrem' command")
		}
		_, err := parseRedisInt64(args[2])
		return err
	case "LSET":
		if len(args) != 4 {
			return errors.New("wrong number of arguments for 'lset' command")
		}
		_, err := parseRedisInt64(args[2])
		return err
	case "RPOPLPUSH":
		if len(args) != 3 {
			return errors.New("wrong number of arguments for 'rpoplpush' command")
		}
	case "HSET":
		if len(args) < 4 || len(args)%2 != 0 {
			return errors.New("wrong number of arguments for 'hset' command")
		}
	case "LPUSH", "RPUSH":
		if len(args) < 3 {
			return fmt.Errorf("wrong number of arguments for '%s' command", strings.ToLower(command))
		}
	case "MGET":
		if len(args) < 2 {
			return errors.New("wrong number of arguments for 'mget' command")
		}
	case "PERSIST":
		if len(args) != 2 {
			return errors.New("wrong number of arguments for 'persist' command")
		}
	case "PTTL":
		if len(args) != 2 {
			return errors.New("wrong number of arguments for 'pttl' command")
		}
	case "RENAME", "RENAMENX":
		if len(args) != 3 {
			return fmt.Errorf("wrong number of arguments for '%s' command", strings.ToLower(command))
		}
	case "SET":
		_, _, _, err := parseRedisSetArgs(args)
		return err
	case "MSET":
		if len(args) < 3 || len(args)%2 == 0 {
			return errors.New("wrong number of arguments for 'mset' command")
		}
	case "DEL", "UNLINK":
		if len(args) < 2 {
			return fmt.Errorf("wrong number of arguments for '%s' command", strings.ToLower(command))
		}
	case "KEYS":
		if len(args) != 2 {
			return errors.New("wrong number of arguments for 'keys' command")
		}
	case "SCAN":
		_, err := parseRedisScanOptions(args, 1)
		return err
	case "SELECT":
		_, err := parseRedisSelectDB(args)
		return err
	case "SSCAN":
		if len(args) < 3 {
			return errors.New("wrong number of arguments for 'sscan' command")
		}
		_, err := parseRedisScanOptions(args, 2)
		return err
	case "TTL":
		if len(args) != 2 {
			return errors.New("wrong number of arguments for 'ttl' command")
		}
	case "ZADD":
		if len(args) < 4 || len(args)%2 != 0 {
			return errors.New("wrong number of arguments for 'zadd' command")
		}
		for i := 2; i < len(args); i += 2 {
			if _, err := parseRedisZSetScore(args[i], errRedisInvalidFloat); err != nil {
				return err
			}
		}
	case "ZCARD":
		if len(args) != 2 {
			return errors.New("wrong number of arguments for 'zcard' command")
		}
	case "ZREM":
		if len(args) < 3 {
			return errors.New("wrong number of arguments for 'zrem' command")
		}
	case "ZRANGEBYSCORE":
		if len(args) < 4 {
			return errors.New("wrong number of arguments for 'zrangebyscore' command")
		}
		if _, err := parseRedisZSetScoreBound(args[2]); err != nil {
			return err
		}
		if _, err := parseRedisZSetScoreBound(args[3]); err != nil {
			return err
		}
		_, err := parseRedisZSetLimitArgs(args, 4)
		return err
	case "ZREVRANGEBYSCORE":
		if len(args) < 4 {
			return errors.New("wrong number of arguments for 'zrevrangebyscore' command")
		}
		if _, err := parseRedisZSetScoreBound(args[2]); err != nil {
			return err
		}
		if _, err := parseRedisZSetScoreBound(args[3]); err != nil {
			return err
		}
		_, err := parseRedisZSetLimitArgs(args, 4)
		return err
	case "ZRANGEBYLEX":
		if len(args) < 4 {
			return errors.New("wrong number of arguments for 'zrangebylex' command")
		}
		if _, err := parseRedisZSetLexBound(args[2]); err != nil {
			return err
		}
		if _, err := parseRedisZSetLexBound(args[3]); err != nil {
			return err
		}
		_, err := parseRedisZSetLimitArgs(args, 4)
		return err
	case "ZREVRANGEBYLEX":
		if len(args) < 4 {
			return errors.New("wrong number of arguments for 'zrevrangebylex' command")
		}
		if _, err := parseRedisZSetLexBound(args[2]); err != nil {
			return err
		}
		if _, err := parseRedisZSetLexBound(args[3]); err != nil {
			return err
		}
		_, err := parseRedisZSetLimitArgs(args, 4)
		return err
	case "ZREMRANGEBYSCORE":
		if len(args) != 4 {
			return errors.New("wrong number of arguments for 'zremrangebyscore' command")
		}
		if _, err := parseRedisZSetScoreBound(args[2]); err != nil {
			return err
		}
		if _, err := parseRedisZSetScoreBound(args[3]); err != nil {
			return err
		}
	case "ZREMRANGEBYLEX":
		if len(args) != 4 {
			return errors.New("wrong number of arguments for 'zremrangebylex' command")
		}
		if _, err := parseRedisZSetLexBound(args[2]); err != nil {
			return err
		}
		if _, err := parseRedisZSetLexBound(args[3]); err != nil {
			return err
		}
	case "ZSCORE":
		if len(args) != 3 {
			return errors.New("wrong number of arguments for 'zscore' command")
		}
	default:
		return fmt.Errorf("unsupported command '%s'", strings.ToLower(command))
	}

	return nil
}

func parseRedisSelectDB(args [][]byte) (int, error) {
	if len(args) != 2 {
		return 0, errors.New("wrong number of arguments for 'select' command")
	}

	dbIndex, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return 0, errors.New("db index must be an integer")
	}
	if dbIndex < redisDatabaseMin || dbIndex > redisDatabaseMax {
		return 0, fmt.Errorf("db index is out of range, only %d..%d supported", redisDatabaseMin, redisDatabaseMax)
	}
	return dbIndex, nil
}

func parseRedisExpireArgs(args [][]byte, multiplierMs int64) ([]byte, int64, error) {
	if len(args) != 3 {
		return nil, 0, fmt.Errorf("wrong number of arguments for '%s' command", strings.ToLower(string(args[0])))
	}

	duration, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return nil, 0, errors.New("expire time must be an integer")
	}
	return args[1], duration * multiplierMs, nil
}

func parseRedisSetArgs(args [][]byte) ([]byte, []byte, redisSetOptions, error) {
	if len(args) < 3 {
		return nil, nil, redisSetOptions{}, errors.New("wrong number of arguments for 'set' command")
	}

	opts := redisSetOptions{}
	for i := 3; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "NX":
			if opts.onlyIfMissing || opts.onlyIfExists {
				return nil, nil, redisSetOptions{}, errors.New("set condition option was already specified")
			}
			opts.onlyIfMissing = true
		case "XX":
			if opts.onlyIfMissing || opts.onlyIfExists {
				return nil, nil, redisSetOptions{}, errors.New("set condition option was already specified")
			}
			opts.onlyIfExists = true
		case "GET":
			if opts.returnOld {
				return nil, nil, redisSetOptions{}, errors.New("GET option was already specified")
			}
			opts.returnOld = true
		case "EX":
			if opts.hasExpire {
				return nil, nil, redisSetOptions{}, errors.New("expire option was already specified")
			}
			if i+1 >= len(args) {
				return nil, nil, redisSetOptions{}, errors.New("syntax error")
			}
			duration, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || duration <= 0 || duration > math.MaxInt64/1000 {
				return nil, nil, redisSetOptions{}, errors.New("expire time must be a positive integer")
			}
			opts.hasExpire = true
			opts.expireAtMs = duration * 1000
			i++
		default:
			return nil, nil, redisSetOptions{}, fmt.Errorf("unsupported set option '%s'", strings.ToLower(string(args[i])))
		}
	}

	return args[1], args[2], opts, nil
}

func (srv *RedisServer) nowUnixMilli() int64 {
	return redisNowUnixMilli(srv.nowFn())
}

func (srv *RedisServer) clusterSlotCount() int {
	if srv != nil && srv.Cluster != nil {
		if state := srv.Cluster.Snapshot(); state != nil && state.SlotCount > 0 {
			return state.SlotCount
		}
	}
	return 16384
}

func (srv *RedisServer) redisNamespace(dbIndex int) redisNamespace {
	return redisNamespaceForDB(dbIndex, srv.clusterSlotCount())
}

func (srv *RedisServer) sweepSelectedRedisDB(selectedDB int, nowMs int64, limit int) error {
	return SweepExpiredRedisDB(srv.DB, srv.redisNamespace(selectedDB), nowMs, limit)
}

func (srv *RedisServer) sweepAllRedisDBs(nowMs int64, limitPerDB int) error {
	return srv.DB.Update(func(tx *storage.Tx) error {
		for dbIndex := redisDatabaseMin; dbIndex <= redisDatabaseMax; dbIndex++ {
			if _, err := sweepExpiredRedisKeysTx(tx, srv.redisNamespace(dbIndex), nowMs, limitPerDB); err != nil {
				return err
			}
		}
		return nil
	})
}

func (srv *RedisServer) executeCommand(selectedDB int, args [][]byte, tx *storage.Tx, nowMs int64, allowAsking bool) (redisReply, error) {
	if err := validateRedisCommand(args); err != nil {
		return nil, err
	}
	if err := srv.ensureClusterRoute(selectedDB, args, tx, nowMs, allowAsking); err != nil {
		return nil, err
	}

	command := strings.ToUpper(string(args[0]))
	if tx == nil {
		move, slotCount, err := srv.clusterCopyingDeltaMoveForArgs(args)
		if err != nil {
			return nil, err
		}
		if move != nil {
			return srv.executeStandaloneWriteWithClusterDelta(selectedDB, args, nowMs, allowAsking, move, slotCount)
		}
	}
	ns := srv.redisNamespace(selectedDB)
	switch command {
	case "ASKING":
		return redisSimpleStringReply{value: "OK"}, nil
	case "BF.ADD":
		var (
			added int64
			err   error
		)
		if tx == nil {
			err = srv.DB.Update(func(writeTx *storage.Tx) error {
				added, err = redisBloomAddTx(writeTx, ns, args[1], args[2], nowMs)
				return err
			})
		} else {
			added, err = redisBloomAddTx(tx, ns, args[1], args[2], nowMs)
		}
		if err != nil {
			return nil, err
		}
		return redisIntegerReply{value: added}, nil
	case "BF.EXISTS":
		var (
			exists bool
			err    error
		)
		if tx == nil {
			err = srv.DB.View(func(readTx *storage.Tx) error {
				exists, err = redisBloomExistsTx(readTx, ns, args[1], args[2], nowMs)
				return err
			})
		} else {
			exists, err = redisBloomExistsTx(tx, ns, args[1], args[2], nowMs)
		}
		if err != nil {
			return nil, err
		}
		if exists {
			return redisIntegerReply{value: 1}, nil
		}
		return redisIntegerReply{value: 0}, nil
	case "BF.MADD":
		var (
			results []int64
			err     error
		)
		if tx == nil {
			err = srv.DB.Update(func(writeTx *storage.Tx) error {
				results, err = redisBloomMAddTx(writeTx, ns, args[1], args[2:], nowMs)
				return err
			})
		} else {
			results, err = redisBloomMAddTx(tx, ns, args[1], args[2:], nowMs)
		}
		if err != nil {
			return nil, err
		}
		return newRedisIntegerArrayReply(results), nil
	case "BF.MEXISTS":
		var (
			results []int64
			err     error
		)
		if tx == nil {
			err = srv.DB.View(func(readTx *storage.Tx) error {
				results, err = redisBloomMExistsTx(readTx, ns, args[1], args[2:], nowMs)
				return err
			})
		} else {
			results, err = redisBloomMExistsTx(tx, ns, args[1], args[2:], nowMs)
		}
		if err != nil {
			return nil, err
		}
		return newRedisIntegerArrayReply(results), nil
	case "BF.RESERVE":
		key, opts, err := parseRedisBloomReserveArgs(args)
		if err != nil {
			return nil, err
		}
		if tx == nil {
			err = srv.DB.Update(func(writeTx *storage.Tx) error {
				return redisBloomReserveTx(writeTx, ns, key, opts, nowMs)
			})
		} else {
			err = redisBloomReserveTx(tx, ns, key, opts, nowMs)
		}
		if err != nil {
			return nil, err
		}
		return redisSimpleStringReply{value: "OK"}, nil
	case "TOPK.ADD":
		increments := make([]uint64, len(args)-2)
		for i := range increments {
			increments[i] = 1
		}
		var (
			dropped [][]byte
			err     error
		)
		if tx == nil {
			err = srv.DB.Update(func(writeTx *storage.Tx) error {
				dropped, err = redisTopKIncrByTx(writeTx, ns, args[1], args[2:], increments, nowMs, srv.randFn)
				return err
			})
		} else {
			dropped, err = redisTopKIncrByTx(tx, ns, args[1], args[2:], increments, nowMs, srv.randFn)
		}
		if err != nil {
			return nil, err
		}
		return newRedisNullableBulkArrayReply(dropped), nil
	case "TOPK.COUNT":
		var (
			counts []int64
			err    error
		)
		if tx == nil {
			err = srv.DB.View(func(readTx *storage.Tx) error {
				counts, err = redisTopKCountTx(readTx, ns, args[1], args[2:], nowMs)
				return err
			})
		} else {
			counts, err = redisTopKCountTx(tx, ns, args[1], args[2:], nowMs)
		}
		if err != nil {
			return nil, err
		}
		return newRedisIntegerArrayReply(counts), nil
	case "TOPK.INFO":
		var (
			meta *redisTopKMeta
			err  error
		)
		if tx == nil {
			err = srv.DB.View(func(readTx *storage.Tx) error {
				meta, err = redisTopKInfoTx(readTx, ns, args[1], nowMs)
				return err
			})
		} else {
			meta, err = redisTopKInfoTx(tx, ns, args[1], nowMs)
		}
		if err != nil {
			return nil, err
		}
		return redisArrayReply{
			values: []redisReply{
				redisSimpleStringReply{value: "k"},
				redisIntegerReply{value: uint64ToRedisInt(meta.K)},
				redisSimpleStringReply{value: "width"},
				redisIntegerReply{value: uint64ToRedisInt(meta.Width)},
				redisSimpleStringReply{value: "depth"},
				redisIntegerReply{value: uint64ToRedisInt(meta.Depth)},
				redisSimpleStringReply{value: "decay"},
				redisSimpleStringReply{value: strconv.FormatFloat(meta.Decay(), 'g', -1, 64)},
			},
		}, nil
	case "TOPK.INCRBY":
		key, items, increments, err := parseRedisTopKIncrByArgs(args)
		if err != nil {
			return nil, err
		}
		var dropped [][]byte
		if tx == nil {
			err = srv.DB.Update(func(writeTx *storage.Tx) error {
				dropped, err = redisTopKIncrByTx(writeTx, ns, key, items, increments, nowMs, srv.randFn)
				return err
			})
		} else {
			dropped, err = redisTopKIncrByTx(tx, ns, key, items, increments, nowMs, srv.randFn)
		}
		if err != nil {
			return nil, err
		}
		return newRedisNullableBulkArrayReply(dropped), nil
	case "TOPK.LIST":
		key, withCount, err := parseRedisTopKListArgs(args)
		if err != nil {
			return nil, err
		}
		var entries []redisTopKHeapEntry
		if tx == nil {
			err = srv.DB.View(func(readTx *storage.Tx) error {
				entries, err = redisTopKListTx(readTx, ns, key, nowMs)
				return err
			})
		} else {
			entries, err = redisTopKListTx(tx, ns, key, nowMs)
		}
		if err != nil {
			return nil, err
		}
		if !withCount {
			values := make([][]byte, 0, len(entries))
			for _, entry := range entries {
				values = append(values, entry.Item)
			}
			return newRedisBulkArrayReply(values), nil
		}
		replies := make([]redisReply, 0, len(entries)*2)
		for _, entry := range entries {
			replies = append(replies, redisBulkReply{value: cloneBytes(entry.Item)})
			replies = append(replies, redisIntegerReply{value: uint64ToRedisInt(entry.Count)})
		}
		return redisArrayReply{values: replies}, nil
	case "TOPK.QUERY":
		var (
			results []int64
			err     error
		)
		if tx == nil {
			err = srv.DB.View(func(readTx *storage.Tx) error {
				results, err = redisTopKQueryTx(readTx, ns, args[1], args[2:], nowMs)
				return err
			})
		} else {
			results, err = redisTopKQueryTx(tx, ns, args[1], args[2:], nowMs)
		}
		if err != nil {
			return nil, err
		}
		return newRedisIntegerArrayReply(results), nil
	case "TOPK.RESERVE":
		key, k, width, depth, decay, err := parseRedisTopKReserveArgs(args)
		if err != nil {
			return nil, err
		}
		if tx == nil {
			err = srv.DB.Update(func(writeTx *storage.Tx) error {
				return redisTopKReserveTx(writeTx, ns, key, k, width, depth, decay, nowMs)
			})
		} else {
			err = redisTopKReserveTx(tx, ns, key, k, width, depth, decay, nowMs)
		}
		if err != nil {
			return nil, err
		}
		return redisSimpleStringReply{value: "OK"}, nil
	case "BLPOP":
		return srv.executeBlockingPop(selectedDB, args, true)
	case "BRPOPLPUSH":
		return srv.executeBlockingMove(selectedDB, args)
	case "BRPOP":
		return srv.executeBlockingPop(selectedDB, args, false)
	case "PING":
		if len(args) == 2 {
			return redisBulkReply{value: cloneBytes(args[1])}, nil
		}
		return redisSimpleStringReply{value: "PONG"}, nil
	case "CLUSTER":
		return srv.executeClusterCommand(args)
	case "COMMAND":
		if len(args) == 2 && strings.ToUpper(string(args[1])) == "COUNT" {
			return redisIntegerReply{value: int64(len(supportedRedisCommandNames))}, nil
		}
		return newRedisStringArrayReply(supportedRedisCommandNames), nil
	case "CONFIG":
		switch strings.ToUpper(string(args[1])) {
		case "GET":
			return newRedisBulkArrayReply(buildRedisConfigReply(srv.Config, args[2])), nil
		case "HELP":
			return newRedisStringArrayReply([]string{
				"CONFIG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
				"GET <pattern> -- Return configuration parameters matching the glob-style pattern.",
				"RESETSTAT -- Reset server statistics counters (no-op in PirinDB).",
				"REWRITE -- Unsupported in PirinDB.",
				"SET <parameter> <value> -- Unsupported in PirinDB.",
				"HELP -- Print this help.",
			}), nil
		case "RESETSTAT":
			return redisSimpleStringReply{value: "OK"}, nil
		case "REWRITE":
			return nil, errors.New("CONFIG REWRITE is not supported")
		case "SET":
			return nil, errors.New("CONFIG SET is not supported")
		default:
			return nil, fmt.Errorf("unsupported config subcommand '%s'", strings.ToLower(string(args[1])))
		}
	case "INFO":
		if tx != nil {
			return nil, errors.New("info is not supported inside pipeline")
		}
		if err := srv.sweepAllRedisDBs(nowMs, 0); err != nil {
			return nil, err
		}
		return redisBulkReply{value: []byte(buildRedisInfo(srv.Config, srv.DB))}, nil
	case "DBSIZE":
		var (
			count int64
			err   error
		)
		if tx == nil {
			count, err = CountRedisDBKeys(srv.DB, ns, nowMs)
		} else {
			count, err = countRedisDBKeysTx(tx, ns, nowMs)
		}
		if err != nil {
			return nil, err
		}
		return redisIntegerReply{value: count}, nil
	case "EXPIRE":
		key, durationMs, err := parseRedisExpireArgs(args, 1000)
		if err != nil {
			return nil, err
		}
		var applied int64
		if tx == nil {
			err = srv.DB.Update(func(writeTx *storage.Tx) error {
				applied, err = redisExpireKeyAtTx(writeTx, ns, key, nowMs+durationMs, nowMs)
				return err
			})
		} else {
			applied, err = redisExpireKeyAtTx(tx, ns, key, nowMs+durationMs, nowMs)
		}
		if err != nil {
			return nil, err
		}
		return redisIntegerReply{value: applied}, nil
	case "EXISTS":
		var (
			existing int64
			err      error
		)
		if tx == nil {
			existing, err = CountExistingRedisKeys(srv.DB, ns, args[1:], nowMs)
		} else {
			existing, err = countExistingRedisKeysTx(tx, ns, args[1:], nowMs)
		}
		if err != nil {
			return nil, err
		}
		return redisIntegerReply{value: existing}, nil
	case "FLUSHALL":
		if tx == nil {
			if err := FlushAllRedis(srv.DB); err != nil {
				return nil, err
			}
		} else {
			if err := flushAllRedisTx(tx); err != nil {
				return nil, err
			}
		}
		return redisSimpleStringReply{value: "OK"}, nil
	case "FLUSHDB":
		if tx == nil {
			if err := FlushRedisDB(srv.DB, ns); err != nil {
				return nil, err
			}
		} else {
			if err := flushRedisDBTx(tx, ns); err != nil {
				return nil, err
			}
		}
		return redisSimpleStringReply{value: "OK"}, nil
	case "DECR":
		var (
			value int64
			err   error
		)
		if tx == nil {
			value, err = ApplyRedisIntDelta(srv.DB, ns, args[1], 1, true, nowMs)
		} else {
			value, err = applyRedisIntDeltaTx(tx, ns, args[1], 1, true, nowMs)
		}
		if err != nil {
			return nil, err
		}
		return redisIntegerReply{value: value}, nil
	case "DECRBY":
		delta, err := parseRedisInt64(args[2])
		if err != nil {
			return nil, err
		}
		var value int64
		if tx == nil {
			value, err = ApplyRedisIntDelta(srv.DB, ns, args[1], delta, true, nowMs)
		} else {
			value, err = applyRedisIntDeltaTx(tx, ns, args[1], delta, true, nowMs)
		}
		if err != nil {
			return nil, err
		}
		return redisIntegerReply{value: value}, nil
	case "GET":
		var (
			value []byte
			found bool
			err   error
		)
		if tx == nil {
			value, found, err = GetRedisBytes(srv.DB, ns, args[1], nowMs)
		} else {
			value, found, err = getRedisBytesTx(tx, ns, args[1], nowMs)
		}
		if err != nil {
			return nil, err
		}
		if !found {
			return redisBulkReply{null: true}, nil
		}
		return redisBulkReply{value: value}, nil
	case "GETSET":
		var (
			value []byte
			found bool
			err   error
		)
		if tx == nil {
			value, found, err = GetSetRedisBytes(srv.DB, ns, args[1], args[2], nowMs)
		} else {
			value, found, err = getSetRedisBytesTx(tx, ns, args[1], args[2], nowMs)
		}
		if err != nil {
			return nil, err
		}
		if !found {
			return redisBulkReply{null: true}, nil
		}
		return redisBulkReply{value: value}, nil
	case "TYPE":
		var (
			keyType string
			err     error
		)
		if tx == nil {
			err = srv.DB.View(func(readTx *storage.Tx) error {
				keyType, err = redisKeyTypeTx(readTx, ns, args[1], nowMs)
				return err
			})
		} else {
			keyType, err = redisKeyTypeTx(tx, ns, args[1], nowMs)
		}
		if err != nil {
			return nil, err
		}
		if keyType == redisKeyTypeNone {
			keyType = "none"
		}
		return redisSimpleStringReply{value: keyType}, nil
	case "PERSIST":
		var (
			removed int64
			err     error
		)
		if tx == nil {
			err = srv.DB.Update(func(writeTx *storage.Tx) error {
				removed, err = redisPersistTTLTx(writeTx, ns, args[1], nowMs)
				return err
			})
		} else {
			removed, err = redisPersistTTLTx(tx, ns, args[1], nowMs)
		}
		if err != nil {
			return nil, err
		}
		return redisIntegerReply{value: removed}, nil
	case "PEXPIRE":
		key, durationMs, err := parseRedisExpireArgs(args, 1)
		if err != nil {
			return nil, err
		}
		var applied int64
		if tx == nil {
			err = srv.DB.Update(func(writeTx *storage.Tx) error {
				applied, err = redisExpireKeyAtTx(writeTx, ns, key, nowMs+durationMs, nowMs)
				return err
			})
		} else {
			applied, err = redisExpireKeyAtTx(tx, ns, key, nowMs+durationMs, nowMs)
		}
		if err != nil {
			return nil, err
		}
		return redisIntegerReply{value: applied}, nil
	case "PTTL":
		var (
			ttlMs int64
			err   error
		)
		if tx == nil {
			err = srv.DB.View(func(readTx *storage.Tx) error {
				ttlMs, err = redisPTTLTx(readTx, ns, args[1], nowMs)
				return err
			})
		} else {
			ttlMs, err = redisPTTLTx(tx, ns, args[1], nowMs)
		}
		if err != nil {
			return nil, err
		}
		return redisIntegerReply{value: ttlMs}, nil
	case "RENAME", "RENAMENX":
		onlyIfDestinationMissing := command == "RENAMENX"
		var (
			renamed    bool
			sourceType string
			err        error
		)
		if tx == nil {
			renamed, sourceType, err = RenameRedisKey(srv.DB, ns, args[1], args[2], onlyIfDestinationMissing, nowMs)
		} else {
			renamed, sourceType, err = renameRedisKeyTx(tx, ns, args[1], args[2], onlyIfDestinationMissing, nowMs)
		}
		if err != nil {
			return nil, err
		}
		if tx == nil && renamed && sourceType == redisKeyTypeList {
			srv.notifyListWaiters(selectedDB, args[2])
		}
		if onlyIfDestinationMissing {
			if renamed {
				return redisIntegerReply{value: 1}, nil
			}
			return redisIntegerReply{value: 0}, nil
		}
		return redisSimpleStringReply{value: "OK"}, nil
	case "HDEL":
		var (
			deleted int64
			err     error
		)
		if tx == nil {
			err = srv.DB.Update(func(writeTx *storage.Tx) error {
				deleted, err = redisHashDelTx(writeTx, ns, args[1], args[2:], nowMs)
				return err
			})
		} else {
			deleted, err = redisHashDelTx(tx, ns, args[1], args[2:], nowMs)
		}
		if err != nil {
			return nil, err
		}
		return redisIntegerReply{value: deleted}, nil
	case "HEXISTS":
		var (
			exists bool
			err    error
		)
		if tx == nil {
			err = srv.DB.View(func(readTx *storage.Tx) error {
				exists, err = redisHashExistsTx(readTx, ns, args[1], args[2], nowMs)
				return err
			})
		} else {
			exists, err = redisHashExistsTx(tx, ns, args[1], args[2], nowMs)
		}
		if err != nil {
			return nil, err
		}
		if exists {
			return redisIntegerReply{value: 1}, nil
		}
		return redisIntegerReply{value: 0}, nil
	case "HGET":
		var (
			value []byte
			found bool
			err   error
		)
		if tx == nil {
			err = srv.DB.View(func(readTx *storage.Tx) error {
				value, found, err = redisHashGetTx(readTx, ns, args[1], args[2], nowMs)
				return err
			})
		} else {
			value, found, err = redisHashGetTx(tx, ns, args[1], args[2], nowMs)
		}
		if err != nil {
			return nil, err
		}
		if !found {
			return redisBulkReply{null: true}, nil
		}
		return redisBulkReply{value: value}, nil
	case "HGETALL":
		var (
			values [][]byte
			err    error
		)
		if tx == nil {
			err = srv.DB.View(func(readTx *storage.Tx) error {
				values, err = redisHashGetAllTx(readTx, ns, args[1], nowMs)
				return err
			})
		} else {
			values, err = redisHashGetAllTx(tx, ns, args[1], nowMs)
		}
		if err != nil {
			return nil, err
		}
		return newRedisBulkArrayReply(values), nil
	case "HKEYS":
		var (
			keys [][]byte
			err  error
		)
		if tx == nil {
			err = srv.DB.View(func(readTx *storage.Tx) error {
				keys, err = redisHashKeysTx(readTx, ns, args[1], nowMs)
				return err
			})
		} else {
			keys, err = redisHashKeysTx(tx, ns, args[1], nowMs)
		}
		if err != nil {
			return nil, err
		}
		return newRedisBulkArrayReply(keys), nil
	case "HLEN":
		var (
			length int64
			err    error
		)
		if tx == nil {
			err = srv.DB.View(func(readTx *storage.Tx) error {
				length, err = redisHashLenTx(readTx, ns, args[1], nowMs)
				return err
			})
		} else {
			length, err = redisHashLenTx(tx, ns, args[1], nowMs)
		}
		if err != nil {
			return nil, err
		}
		return redisIntegerReply{value: length}, nil
	case "HSET":
		var (
			added int64
			err   error
		)
		pairs := make([]hashFieldValuePair, 0, (len(args)-2)/2)
		for i := 2; i < len(args); i += 2 {
			pairs = append(pairs, hashFieldValuePair{
				field: args[i],
				value: args[i+1],
			})
		}
		if tx == nil {
			err = srv.DB.Update(func(writeTx *storage.Tx) error {
				added, err = redisHashSetTx(writeTx, ns, args[1], pairs, nowMs)
				return err
			})
		} else {
			added, err = redisHashSetTx(tx, ns, args[1], pairs, nowMs)
		}
		if err != nil {
			return nil, err
		}
		return redisIntegerReply{value: added}, nil
	case "HVALS":
		var (
			values [][]byte
			err    error
		)
		if tx == nil {
			err = srv.DB.View(func(readTx *storage.Tx) error {
				values, err = redisHashValuesTx(readTx, ns, args[1], nowMs)
				return err
			})
		} else {
			values, err = redisHashValuesTx(tx, ns, args[1], nowMs)
		}
		if err != nil {
			return nil, err
		}
		return newRedisBulkArrayReply(values), nil
	case "ZADD":
		pairs := make([]redisZSetScoreMemberPair, 0, (len(args)-2)/2)
		for i := 2; i < len(args); i += 2 {
			score, err := parseRedisZSetScore(args[i], errRedisInvalidFloat)
			if err != nil {
				return nil, err
			}
			pairs = append(pairs, redisZSetScoreMemberPair{
				score:  score,
				member: args[i+1],
			})
		}
		var (
			added int64
			err   error
		)
		if tx == nil {
			err = srv.DB.Update(func(writeTx *storage.Tx) error {
				added, err = redisZSetAddTx(writeTx, ns, args[1], pairs, nowMs)
				return err
			})
		} else {
			added, err = redisZSetAddTx(tx, ns, args[1], pairs, nowMs)
		}
		if err != nil {
			return nil, err
		}
		return redisIntegerReply{value: added}, nil
	case "ZCARD":
		var (
			card int64
			err  error
		)
		if tx == nil {
			err = srv.DB.View(func(readTx *storage.Tx) error {
				card, err = redisZSetCardTx(readTx, ns, args[1], nowMs)
				return err
			})
		} else {
			card, err = redisZSetCardTx(tx, ns, args[1], nowMs)
		}
		if err != nil {
			return nil, err
		}
		return redisIntegerReply{value: card}, nil
	case "ZREM":
		var (
			removed int64
			err     error
		)
		if tx == nil {
			err = srv.DB.Update(func(writeTx *storage.Tx) error {
				removed, err = redisZSetRemTx(writeTx, ns, args[1], args[2:], nowMs)
				return err
			})
		} else {
			removed, err = redisZSetRemTx(tx, ns, args[1], args[2:], nowMs)
		}
		if err != nil {
			return nil, err
		}
		return redisIntegerReply{value: removed}, nil
	case "ZRANGEBYSCORE":
		min, err := parseRedisZSetScoreBound(args[2])
		if err != nil {
			return nil, err
		}
		max, err := parseRedisZSetScoreBound(args[3])
		if err != nil {
			return nil, err
		}
		opts, err := parseRedisZSetLimitArgs(args, 4)
		if err != nil {
			return nil, err
		}
		var values [][]byte
		if tx == nil {
			err = srv.DB.View(func(readTx *storage.Tx) error {
				values, err = redisZSetRangeByScoreTx(readTx, ns, args[1], min, max, false, opts, nowMs)
				return err
			})
		} else {
			values, err = redisZSetRangeByScoreTx(tx, ns, args[1], min, max, false, opts, nowMs)
		}
		if err != nil {
			return nil, err
		}
		return newRedisBulkArrayReply(values), nil
	case "ZREVRANGEBYSCORE":
		max, err := parseRedisZSetScoreBound(args[2])
		if err != nil {
			return nil, err
		}
		min, err := parseRedisZSetScoreBound(args[3])
		if err != nil {
			return nil, err
		}
		opts, err := parseRedisZSetLimitArgs(args, 4)
		if err != nil {
			return nil, err
		}
		var values [][]byte
		if tx == nil {
			err = srv.DB.View(func(readTx *storage.Tx) error {
				values, err = redisZSetRangeByScoreTx(readTx, ns, args[1], min, max, true, opts, nowMs)
				return err
			})
		} else {
			values, err = redisZSetRangeByScoreTx(tx, ns, args[1], min, max, true, opts, nowMs)
		}
		if err != nil {
			return nil, err
		}
		return newRedisBulkArrayReply(values), nil
	case "ZRANGEBYLEX":
		min, err := parseRedisZSetLexBound(args[2])
		if err != nil {
			return nil, err
		}
		max, err := parseRedisZSetLexBound(args[3])
		if err != nil {
			return nil, err
		}
		opts, err := parseRedisZSetLimitArgs(args, 4)
		if err != nil {
			return nil, err
		}
		var values [][]byte
		if tx == nil {
			err = srv.DB.View(func(readTx *storage.Tx) error {
				values, err = redisZSetRangeByLexTx(readTx, ns, args[1], min, max, false, opts, nowMs)
				return err
			})
		} else {
			values, err = redisZSetRangeByLexTx(tx, ns, args[1], min, max, false, opts, nowMs)
		}
		if err != nil {
			return nil, err
		}
		return newRedisBulkArrayReply(values), nil
	case "ZREVRANGEBYLEX":
		max, err := parseRedisZSetLexBound(args[2])
		if err != nil {
			return nil, err
		}
		min, err := parseRedisZSetLexBound(args[3])
		if err != nil {
			return nil, err
		}
		opts, err := parseRedisZSetLimitArgs(args, 4)
		if err != nil {
			return nil, err
		}
		var values [][]byte
		if tx == nil {
			err = srv.DB.View(func(readTx *storage.Tx) error {
				values, err = redisZSetRangeByLexTx(readTx, ns, args[1], min, max, true, opts, nowMs)
				return err
			})
		} else {
			values, err = redisZSetRangeByLexTx(tx, ns, args[1], min, max, true, opts, nowMs)
		}
		if err != nil {
			return nil, err
		}
		return newRedisBulkArrayReply(values), nil
	case "ZREMRANGEBYSCORE":
		min, err := parseRedisZSetScoreBound(args[2])
		if err != nil {
			return nil, err
		}
		max, err := parseRedisZSetScoreBound(args[3])
		if err != nil {
			return nil, err
		}
		var removed int64
		if tx == nil {
			err = srv.DB.Update(func(writeTx *storage.Tx) error {
				removed, err = redisZSetRemRangeByScoreTx(writeTx, ns, args[1], min, max, nowMs)
				return err
			})
		} else {
			removed, err = redisZSetRemRangeByScoreTx(tx, ns, args[1], min, max, nowMs)
		}
		if err != nil {
			return nil, err
		}
		return redisIntegerReply{value: removed}, nil
	case "ZREMRANGEBYLEX":
		min, err := parseRedisZSetLexBound(args[2])
		if err != nil {
			return nil, err
		}
		max, err := parseRedisZSetLexBound(args[3])
		if err != nil {
			return nil, err
		}
		var removed int64
		if tx == nil {
			err = srv.DB.Update(func(writeTx *storage.Tx) error {
				removed, err = redisZSetRemRangeByLexTx(writeTx, ns, args[1], min, max, nowMs)
				return err
			})
		} else {
			removed, err = redisZSetRemRangeByLexTx(tx, ns, args[1], min, max, nowMs)
		}
		if err != nil {
			return nil, err
		}
		return redisIntegerReply{value: removed}, nil
	case "ZSCORE":
		var (
			score float64
			found bool
			err   error
		)
		if tx == nil {
			err = srv.DB.View(func(readTx *storage.Tx) error {
				score, found, err = redisZSetScoreTx(readTx, ns, args[1], args[2], nowMs)
				return err
			})
		} else {
			score, found, err = redisZSetScoreTx(tx, ns, args[1], args[2], nowMs)
		}
		if err != nil {
			return nil, err
		}
		if !found {
			return redisBulkReply{null: true}, nil
		}
		return redisBulkReply{value: []byte(formatRedisZSetScore(score))}, nil
	case "INCR":
		var (
			value int64
			err   error
		)
		if tx == nil {
			value, err = ApplyRedisIntDelta(srv.DB, ns, args[1], 1, false, nowMs)
		} else {
			value, err = applyRedisIntDeltaTx(tx, ns, args[1], 1, false, nowMs)
		}
		if err != nil {
			return nil, err
		}
		return redisIntegerReply{value: value}, nil
	case "INCRBY":
		delta, err := parseRedisInt64(args[2])
		if err != nil {
			return nil, err
		}
		var value int64
		if tx == nil {
			value, err = ApplyRedisIntDelta(srv.DB, ns, args[1], delta, false, nowMs)
		} else {
			value, err = applyRedisIntDeltaTx(tx, ns, args[1], delta, false, nowMs)
		}
		if err != nil {
			return nil, err
		}
		return redisIntegerReply{value: value}, nil
	case "LINDEX":
		index, err := parseRedisInt64(args[2])
		if err != nil {
			return nil, err
		}
		var (
			value []byte
			found bool
		)
		if tx == nil {
			err = srv.DB.View(func(readTx *storage.Tx) error {
				value, found, err = redisListIndexTx(readTx, ns, args[1], index, nowMs)
				return err
			})
		} else {
			value, found, err = redisListIndexTx(tx, ns, args[1], index, nowMs)
		}
		if err != nil {
			return nil, err
		}
		if !found {
			return redisBulkReply{null: true}, nil
		}
		return redisBulkReply{value: value}, nil
	case "LLEN":
		var (
			length int64
			err    error
		)
		if tx == nil {
			err = srv.DB.View(func(readTx *storage.Tx) error {
				length, err = redisListLenTx(readTx, ns, args[1], nowMs)
				return err
			})
		} else {
			length, err = redisListLenTx(tx, ns, args[1], nowMs)
		}
		if err != nil {
			return nil, err
		}
		return redisIntegerReply{value: length}, nil
	case "LPOP":
		var (
			value []byte
			found bool
			err   error
		)
		if tx == nil {
			err = srv.DB.Update(func(writeTx *storage.Tx) error {
				value, found, err = redisPopTx(writeTx, ns, args[1], true, nowMs)
				return err
			})
		} else {
			value, found, err = redisPopTx(tx, ns, args[1], true, nowMs)
		}
		if err != nil {
			return nil, err
		}
		if !found {
			return redisBulkReply{null: true}, nil
		}
		return redisBulkReply{value: value}, nil
	case "LPUSH":
		var (
			length int64
			err    error
		)
		values := make([][]byte, 0, len(args)-2)
		for _, value := range args[2:] {
			values = append(values, value)
		}
		if tx == nil {
			err = srv.DB.Update(func(writeTx *storage.Tx) error {
				length, err = redisPushTx(writeTx, ns, args[1], values, true, nowMs)
				return err
			})
		} else {
			length, err = redisPushTx(tx, ns, args[1], values, true, nowMs)
		}
		if err != nil {
			return nil, err
		}
		if tx == nil {
			srv.notifyListWaiters(selectedDB, args[1])
		}
		return redisIntegerReply{value: length}, nil
	case "LRANGE":
		start, err := parseRedisInt64(args[2])
		if err != nil {
			return nil, err
		}
		stop, err := parseRedisInt64(args[3])
		if err != nil {
			return nil, err
		}
		var values [][]byte
		if tx == nil {
			err = srv.DB.View(func(readTx *storage.Tx) error {
				values, err = redisListRangeTx(readTx, ns, args[1], start, stop, nowMs)
				return err
			})
		} else {
			values, err = redisListRangeTx(tx, ns, args[1], start, stop, nowMs)
		}
		if err != nil {
			return nil, err
		}
		return newRedisBulkArrayReply(values), nil
	case "LREM":
		count, err := parseRedisInt64(args[2])
		if err != nil {
			return nil, err
		}
		var removed int64
		if tx == nil {
			err = srv.DB.Update(func(writeTx *storage.Tx) error {
				removed, err = redisListRemTx(writeTx, ns, args[1], count, args[3], nowMs)
				return err
			})
		} else {
			removed, err = redisListRemTx(tx, ns, args[1], count, args[3], nowMs)
		}
		if err != nil {
			return nil, err
		}
		return redisIntegerReply{value: removed}, nil
	case "LSET":
		index, err := parseRedisInt64(args[2])
		if err != nil {
			return nil, err
		}
		if tx == nil {
			err = srv.DB.Update(func(writeTx *storage.Tx) error {
				return redisListSetTx(writeTx, ns, args[1], index, args[3], nowMs)
			})
		} else {
			err = redisListSetTx(tx, ns, args[1], index, args[3], nowMs)
		}
		if err != nil {
			return nil, err
		}
		return redisSimpleStringReply{value: "OK"}, nil
	case "LTRIM":
		start, err := parseRedisInt64(args[2])
		if err != nil {
			return nil, err
		}
		stop, err := parseRedisInt64(args[3])
		if err != nil {
			return nil, err
		}
		if tx == nil {
			err = srv.DB.Update(func(writeTx *storage.Tx) error {
				return redisListTrimTx(writeTx, ns, args[1], start, stop, nowMs)
			})
		} else {
			err = redisListTrimTx(tx, ns, args[1], start, stop, nowMs)
		}
		if err != nil {
			return nil, err
		}
		return redisSimpleStringReply{value: "OK"}, nil
	case "MGET":
		var (
			values [][]byte
			found  []bool
			err    error
		)
		if tx == nil {
			values, found, err = GetManyRedisBytes(srv.DB, ns, args[1:], nowMs)
		} else {
			values, found, err = getManyRedisBytesTx(tx, ns, args[1:], nowMs)
		}
		if err != nil {
			return nil, err
		}
		replies := make([]redisReply, 0, len(values))
		for i := range values {
			if !found[i] {
				replies = append(replies, redisBulkReply{null: true})
				continue
			}
			replies = append(replies, redisBulkReply{value: values[i]})
		}
		return redisArrayReply{values: replies}, nil
	case "SET":
		key, value, opts, err := parseRedisSetArgs(args)
		if err != nil {
			return nil, err
		}
		var (
			currentValue []byte
			found        bool
			applied      bool
		)
		if tx == nil {
			currentValue, found, applied, err = SetRedisBytes(srv.DB, ns, key, value, opts, nowMs)
		} else {
			currentValue, found, applied, err = setRedisBytesTx(tx, ns, key, value, opts, nowMs)
		}
		if err != nil {
			return nil, err
		}
		if opts.returnOld {
			if !found {
				return redisBulkReply{null: true}, nil
			}
			return redisBulkReply{value: currentValue}, nil
		}
		if !applied {
			return redisBulkReply{null: true}, nil
		}
		return redisSimpleStringReply{value: "OK"}, nil
	case "MSET":
		pairs := make([]keyValuePair, 0, (len(args)-1)/2)
		for i := 1; i < len(args); i += 2 {
			pairs = append(pairs, keyValuePair{
				key:   args[i],
				value: args[i+1],
			})
		}
		var err error
		if tx == nil {
			err = PutManyRedisBytes(srv.DB, ns, pairs, nowMs)
		} else {
			err = putManyRedisBytesTx(tx, ns, pairs, nowMs)
		}
		if err != nil {
			return nil, err
		}
		return redisSimpleStringReply{value: "OK"}, nil
	case "RPOP":
		var (
			value []byte
			found bool
			err   error
		)
		if tx == nil {
			err = srv.DB.Update(func(writeTx *storage.Tx) error {
				value, found, err = redisPopTx(writeTx, ns, args[1], false, nowMs)
				return err
			})
		} else {
			value, found, err = redisPopTx(tx, ns, args[1], false, nowMs)
		}
		if err != nil {
			return nil, err
		}
		if !found {
			return redisBulkReply{null: true}, nil
		}
		return redisBulkReply{value: value}, nil
	case "RPOPLPUSH":
		var (
			value []byte
			found bool
			err   error
		)
		if tx == nil {
			err = srv.DB.Update(func(writeTx *storage.Tx) error {
				value, found, err = redisMoveTx(writeTx, ns, args[1], args[2], nowMs)
				return err
			})
		} else {
			value, found, err = redisMoveTx(tx, ns, args[1], args[2], nowMs)
		}
		if err != nil {
			return nil, err
		}
		if !found {
			return redisBulkReply{null: true}, nil
		}
		if tx == nil {
			srv.notifyListWaiters(selectedDB, args[2])
		}
		return redisBulkReply{value: value}, nil
	case "RPUSH":
		var (
			length int64
			err    error
		)
		values := make([][]byte, 0, len(args)-2)
		for _, value := range args[2:] {
			values = append(values, value)
		}
		if tx == nil {
			err = srv.DB.Update(func(writeTx *storage.Tx) error {
				length, err = redisPushTx(writeTx, ns, args[1], values, false, nowMs)
				return err
			})
		} else {
			length, err = redisPushTx(tx, ns, args[1], values, false, nowMs)
		}
		if err != nil {
			return nil, err
		}
		if tx == nil {
			srv.notifyListWaiters(selectedDB, args[1])
		}
		return redisIntegerReply{value: length}, nil
	case "DEL", "UNLINK":
		var (
			deleted int64
			err     error
		)
		if tx == nil {
			deleted, err = DeleteManyRedisBytes(srv.DB, ns, args[1:], nowMs)
		} else {
			deleted, err = deleteManyRedisBytesTx(tx, ns, args[1:], nowMs)
		}
		if err != nil {
			return nil, err
		}
		return redisIntegerReply{value: deleted}, nil
	case "KEYS":
		if literalKey, ok := parseRedisLiteralPattern(args[1]); ok {
			var (
				keyType string
				err     error
			)
			if tx == nil {
				err = srv.DB.View(func(readTx *storage.Tx) error {
					keyType, err = redisKeyTypeTx(readTx, ns, literalKey, nowMs)
					return err
				})
			} else {
				keyType, err = redisKeyTypeTx(tx, ns, literalKey, nowMs)
			}
			if err != nil {
				return nil, err
			}
			if keyType == redisKeyTypeNone {
				return newRedisBulkArrayReply(nil), nil
			}
			return newRedisBulkArrayReply([][]byte{literalKey}), nil
		}
		if prefix, ok := parseRedisPrefixPattern(args[1]); ok {
			var (
				keys [][]byte
				err  error
			)
			if tx == nil {
				if err = srv.sweepSelectedRedisDB(selectedDB, nowMs, redisLazySweepLimit); err != nil {
					return nil, err
				}
				keys, err = ListRedisKeysByPrefix(srv.DB, ns, prefix, nowMs)
			} else {
				keys, err = listRedisKeysByPrefixTx(tx, ns, prefix, nowMs)
			}
			if err != nil {
				return nil, err
			}
			return newRedisBulkArrayReply(keys), nil
		}

		var (
			keys [][]byte
			err  error
		)
		matcher := newRedisPatternMatcher(args[1])
		if tx == nil {
			if err = srv.sweepSelectedRedisDB(selectedDB, nowMs, redisLazySweepLimit); err != nil {
				return nil, err
			}
			keys, err = ListRedisKeys(srv.DB, ns, matcher, nowMs)
		} else {
			keys, err = listRedisKeysTx(tx, ns, matcher, nowMs)
		}
		if err != nil {
			return nil, err
		}
		return newRedisBulkArrayReply(keys), nil
	case "SCAN":
		opts, err := parseRedisScanOptions(args, 1)
		if err != nil {
			return nil, err
		}
		return srv.executeScan(selectedDB, tx, opts, nowMs)
	case "SSCAN":
		opts, err := parseRedisScanOptions(args, 2)
		if err != nil {
			return nil, err
		}
		return srv.executeScan(selectedDB, tx, opts, nowMs)
	case "TTL":
		var (
			ttl int64
			err error
		)
		if tx == nil {
			err = srv.DB.View(func(readTx *storage.Tx) error {
				ttl, err = redisTTLTx(readTx, ns, args[1], nowMs)
				return err
			})
		} else {
			ttl, err = redisTTLTx(tx, ns, args[1], nowMs)
		}
		if err != nil {
			return nil, err
		}
		return redisIntegerReply{value: ttl}, nil
	default:
		return nil, fmt.Errorf("unsupported command '%s'", strings.ToLower(command))
	}
}

func (srv *RedisServer) executeScan(selectedDB int, tx *storage.Tx, opts redisScanOptions, nowMs int64) (redisReply, error) {
	var (
		nextCursor string
		keys       [][]byte
		err        error
	)

	ns := srv.redisNamespace(selectedDB)
	if prefix, ok := parseRedisPrefixPattern([]byte(opts.pattern)); ok {
		if tx == nil {
			if err = srv.sweepSelectedRedisDB(selectedDB, nowMs, redisLazySweepLimit); err != nil {
				return nil, err
			}
			nextCursor, keys, err = ScanRedisKeysByPrefix(srv.DB, ns, opts.cursor, prefix, opts.count, nowMs)
		} else {
			nextCursor, keys, err = scanRedisKeysByPrefixTx(tx, ns, opts.cursor, prefix, opts.count, nowMs)
		}
		if err != nil {
			return nil, err
		}
		return redisArrayReply{
			values: []redisReply{
				redisBulkReply{value: []byte(nextCursor)},
				newRedisBulkArrayReply(keys),
			},
		}, nil
	}

	matcher := newRedisPatternMatcher([]byte(opts.pattern))
	if tx == nil {
		if err = srv.sweepSelectedRedisDB(selectedDB, nowMs, redisLazySweepLimit); err != nil {
			return nil, err
		}
		nextCursor, keys, err = ScanRedisKeys(srv.DB, ns, opts.cursor, matcher, opts.count, nowMs)
	} else {
		nextCursor, keys, err = scanRedisKeysTx(tx, ns, opts.cursor, matcher, opts.count, nowMs)
	}
	if err != nil {
		return nil, err
	}
	return newRedisScanReply(nextCursor, keys), nil
}

func buildRedisInfo(cfg *Config, db *storage.DB) string {
	stat := db.Stat()

	redisPort := 0
	if cfg != nil && cfg.Redis != nil {
		redisPort = cfg.Redis.Port
	}

	bucketViews := make(map[string]*storage.BucketStat, len(stat.Buckets))
	for bucketName, bucketStat := range stat.Buckets {
		bucketViews[bucketName] = bucketStat
	}

	var keyspaceBuilder strings.Builder
	for dbIndex := redisDatabaseMin; dbIndex <= redisDatabaseMax; dbIndex++ {
		usage := summarizeRedisDBUsage(bucketViews, dbIndex)
		if usage.keys == 0 {
			continue
		}
		fmt.Fprintf(&keyspaceBuilder, "db%d:keys=%d,blobs=%d,bytes_in_use=%d\r\n", dbIndex, usage.keys, usage.blobs, usage.bytesInUse)
	}

	return fmt.Sprintf(
		"# Server\r\npirindb_version:%s\r\nprocess_id:%d\r\ntcp_port:%d\r\nrole:master\r\n# Stats\r\ntotal_pages:%d\r\nused_pages:%d\r\nfree_pages:%d\r\nread_transactions:%d\r\n# Keyspace\r\n%s",
		version,
		os.Getpid(),
		redisPort,
		stat.TotalPageNum,
		stat.UsedPageN,
		stat.FreePageN,
		stat.TxN,
		keyspaceBuilder.String(),
	)
}

func parseRedisScanOptions(args [][]byte, cursorIndex int) (redisScanOptions, error) {
	if len(args) <= cursorIndex {
		return redisScanOptions{}, errors.New("missing scan cursor")
	}

	opts := redisScanOptions{
		cursor:  string(args[cursorIndex]),
		pattern: "*",
		count:   defaultRedisScanCount,
	}

	for i := cursorIndex + 1; i < len(args); i += 2 {
		if i+1 >= len(args) {
			return redisScanOptions{}, errors.New("scan options must be provided as option/value pairs")
		}

		switch strings.ToUpper(string(args[i])) {
		case "MATCH":
			opts.pattern = string(args[i+1])
		case "COUNT":
			count, err := strconv.Atoi(string(args[i+1]))
			if err != nil || count <= 0 {
				return redisScanOptions{}, errors.New("count must be a positive integer")
			}
			opts.count = count
		default:
			return redisScanOptions{}, fmt.Errorf("unsupported scan option '%s'", strings.ToLower(string(args[i])))
		}
	}

	return opts, nil
}

func newRedisPatternMatcher(pattern []byte) keyMatcher {
	if len(pattern) == 0 || bytes.Equal(pattern, []byte("*")) {
		return func([]byte) bool {
			return true
		}
	}

	p := cloneBytes(pattern)
	return func(key []byte) bool {
		return redisPatternMatch(p, key)
	}
}

func parseRedisLiteralPattern(pattern []byte) ([]byte, bool) {
	if len(pattern) == 0 {
		return cloneBytes(pattern), true
	}

	literal := make([]byte, 0, len(pattern))
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '*', '?':
			return nil, false
		case '\\':
			if i+1 >= len(pattern) {
				return nil, false
			}
			i++
			literal = append(literal, pattern[i])
		default:
			literal = append(literal, pattern[i])
		}
	}
	return literal, true
}

func parseRedisPrefixPattern(pattern []byte) ([]byte, bool) {
	if len(pattern) == 0 {
		return nil, false
	}

	prefix := make([]byte, 0, len(pattern))
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '?':
			return nil, false
		case '*':
			if i != len(pattern)-1 || len(prefix) == 0 {
				return nil, false
			}
			return prefix, true
		case '\\':
			if i+1 >= len(pattern) {
				return nil, false
			}
			i++
			prefix = append(prefix, pattern[i])
		default:
			prefix = append(prefix, pattern[i])
		}
	}
	return nil, false
}

func redisPatternMatch(pattern []byte, key []byte) bool {
	patternIndex := 0
	keyIndex := 0
	starPatternIndex := -1
	starKeyIndex := 0

	for keyIndex < len(key) {
		if patternIndex < len(pattern) {
			switch pattern[patternIndex] {
			case '\\':
				if patternIndex+1 < len(pattern) && pattern[patternIndex+1] == key[keyIndex] {
					patternIndex += 2
					keyIndex++
					continue
				}
			case '?':
				patternIndex++
				keyIndex++
				continue
			case '*':
				starPatternIndex = patternIndex
				patternIndex++
				starKeyIndex = keyIndex
				continue
			default:
				if pattern[patternIndex] == key[keyIndex] {
					patternIndex++
					keyIndex++
					continue
				}
			}
		}

		if starPatternIndex == -1 {
			return false
		}

		patternIndex = starPatternIndex + 1
		starKeyIndex++
		keyIndex = starKeyIndex
	}

	for patternIndex < len(pattern) {
		if pattern[patternIndex] == '*' {
			patternIndex++
			continue
		}
		if pattern[patternIndex] == '\\' && patternIndex+1 < len(pattern) {
			return false
		}
		break
	}

	return patternIndex == len(pattern)
}

func buildRedisConfigReply(cfg *Config, pattern []byte) [][]byte {
	cfgPattern := bytes.ToLower(cloneBytes(pattern))
	if len(cfgPattern) == 0 {
		cfgPattern = []byte("*")
	}

	match := func(name string) bool {
		return redisPatternMatch(cfgPattern, []byte(name))
	}

	entries := make([][]byte, 0, 24)
	add := func(name, value string) {
		if match(name) {
			entries = append(entries, []byte(name), []byte(value))
		}
	}

	redisHost := "127.0.0.1"
	redisPort := 6379
	dbFilename := "pirin.db"
	syncPolicy := "strict"
	checkpointThreshold := 64
	groupThreshold := 16
	groupWindowMs := 1

	if cfg != nil {
		if cfg.Redis != nil {
			if cfg.Redis.Host != "" {
				redisHost = cfg.Redis.Host
			}
			if cfg.Redis.Port > 0 {
				redisPort = cfg.Redis.Port
			}
		}
		if cfg.DB != nil {
			if cfg.DB.Filename != "" {
				dbFilename = cfg.DB.Filename
			}
			if cfg.DB.SyncPolicy != "" {
				syncPolicy = cfg.DB.SyncPolicy
			}
			if cfg.DB.CheckpointTxThreshold > 0 {
				checkpointThreshold = cfg.DB.CheckpointTxThreshold
			}
			if cfg.DB.GroupCommitTxThreshold > 0 {
				groupThreshold = cfg.DB.GroupCommitTxThreshold
			}
			if cfg.DB.GroupCommitWindowMs > 0 {
				groupWindowMs = cfg.DB.GroupCommitWindowMs
			}
		}
	}

	dbDir := filepath.Dir(dbFilename)
	dbBase := filepath.Base(dbFilename)

	add("appendonly", "no")
	add("bind", redisHost)
	add("databases", strconv.Itoa(redisDatabaseMax-redisDatabaseMin+1))
	add("dbfilename", dbBase)
	add("dir", dbDir)
	add("maxmemory", "0")
	add("maxmemory-policy", "noeviction")
	add("port", strconv.Itoa(redisPort))
	add("requirepass", "")
	add("save", "")
	add("tcp-port", strconv.Itoa(redisPort))

	add("pirindb-sync-policy", syncPolicy)
	add("pirindb-checkpoint-tx-threshold", strconv.Itoa(checkpointThreshold))
	add("pirindb-group-commit-tx-threshold", strconv.Itoa(groupThreshold))
	add("pirindb-group-commit-window-ms", strconv.Itoa(groupWindowMs))

	return entries
}

func (resp *redisConn) queueCommand(args [][]byte) {
	resp.queuedCommands = append(resp.queuedCommands, cloneCommandArgs(args))
}

func cloneCommandArgs(args [][]byte) [][]byte {
	cloned := make([][]byte, 0, len(args))
	for _, arg := range args {
		cloned = append(cloned, cloneBytes(arg))
	}
	return cloned
}

func (resp *redisConn) resetPipeline() {
	resp.pipelineActive = false
	resp.pipelineAsking = false
	resp.queuedCommands = resp.queuedCommands[:0]
}

func (resp *redisConn) readCommand() ([][]byte, error) {
	prefix, err := resp.reader.ReadByte()
	if err != nil {
		return nil, err
	}

	switch prefix {
	case '*':
		return resp.readArrayCommand()
	default:
		line, err := resp.readLineWithPrefix(prefix)
		if err != nil {
			return nil, err
		}
		fields := bytes.Fields(line)
		args := make([][]byte, 0, len(fields))
		for _, field := range fields {
			args = append(args, cloneBytes(field))
		}
		return args, nil
	}
}

func (resp *redisConn) readArrayCommand() ([][]byte, error) {
	arrayLenLine, err := resp.readLine()
	if err != nil {
		return nil, err
	}

	arrayLen, err := strconv.Atoi(string(arrayLenLine))
	if err != nil || arrayLen < 0 {
		return nil, errors.New("invalid array length")
	}

	args := make([][]byte, 0, arrayLen)
	for i := 0; i < arrayLen; i++ {
		bulkType, err := resp.reader.ReadByte()
		if err != nil {
			return nil, err
		}
		if bulkType != '$' {
			return nil, errors.New("expected bulk string argument")
		}

		bulkLenLine, err := resp.readLine()
		if err != nil {
			return nil, err
		}
		bulkLen, err := strconv.Atoi(string(bulkLenLine))
		if err != nil || bulkLen < 0 {
			return nil, errors.New("invalid bulk string length")
		}

		buf := make([]byte, bulkLen+2)
		if _, err = io.ReadFull(resp.reader, buf); err != nil {
			return nil, err
		}
		if buf[bulkLen] != '\r' || buf[bulkLen+1] != '\n' {
			return nil, errors.New("invalid bulk string terminator")
		}

		args = append(args, cloneBytes(buf[:bulkLen]))
	}

	return args, nil
}

func (resp *redisConn) readLine() ([]byte, error) {
	line, err := resp.reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, errors.New("protocol error")
	}
	return line[:len(line)-2], nil
}

func (resp *redisConn) readLineWithPrefix(prefix byte) ([]byte, error) {
	line, err := resp.readLine()
	if err != nil {
		return nil, err
	}
	return append([]byte{prefix}, line...), nil
}

func newRedisBulkArrayReply(values [][]byte) redisReply {
	replies := make([]redisReply, 0, len(values))
	for _, value := range values {
		replies = append(replies, redisBulkReply{value: cloneBytes(value)})
	}
	return redisArrayReply{values: replies}
}

func newRedisNullableBulkArrayReply(values [][]byte) redisReply {
	replies := make([]redisReply, 0, len(values))
	for _, value := range values {
		if value == nil {
			replies = append(replies, redisBulkReply{null: true})
			continue
		}
		replies = append(replies, redisBulkReply{value: cloneBytes(value)})
	}
	return redisArrayReply{values: replies}
}

func newRedisIntegerArrayReply(values []int64) redisReply {
	replies := make([]redisReply, 0, len(values))
	for _, value := range values {
		replies = append(replies, redisIntegerReply{value: value})
	}
	return redisArrayReply{values: replies}
}

func newRedisStringArrayReply(values []string) redisReply {
	replies := make([]redisReply, 0, len(values))
	for _, value := range values {
		replies = append(replies, redisBulkReply{value: []byte(value)})
	}
	return redisArrayReply{values: replies}
}

func newRedisScanReply(cursor string, values [][]byte) redisReply {
	if cursor == "" {
		cursor = "0"
	}
	return redisArrayReply{
		values: []redisReply{
			redisBulkReply{value: []byte(cursor)},
			newRedisBulkArrayReply(values),
		},
	}
}

func (reply redisSimpleStringReply) writeRESP(conn *redisConn) error {
	return conn.writeSimpleString(reply.value)
}

func (reply redisErrorReply) writeRESP(conn *redisConn) error {
	return conn.writeError(reply.message)
}

func (reply redisIntegerReply) writeRESP(conn *redisConn) error {
	return conn.writeInteger(reply.value)
}

func (reply redisBulkReply) writeRESP(conn *redisConn) error {
	if reply.null {
		return conn.writeNullBulk()
	}
	return conn.writeBulk(reply.value)
}

func (reply redisArrayReply) writeRESP(conn *redisConn) error {
	if _, err := conn.writer.WriteString("*" + strconv.Itoa(len(reply.values)) + "\r\n"); err != nil {
		return err
	}
	for _, value := range reply.values {
		if value == nil {
			if err := conn.writeNullBulk(); err != nil {
				return err
			}
			continue
		}
		if err := value.writeRESP(conn); err != nil {
			return err
		}
	}
	return nil
}

func (resp *redisConn) writeSimpleString(value string) error {
	_, err := resp.writer.WriteString("+" + value + "\r\n")
	return err
}

func (resp *redisConn) writeError(message string) error {
	_, err := resp.writer.WriteString("-" + message + "\r\n")
	return err
}

func formatRedisError(err error) string {
	if err == nil {
		return "ERR unknown error"
	}
	message := err.Error()
	if strings.HasPrefix(message, "WRONGTYPE") || strings.HasPrefix(message, "MOVED") || strings.HasPrefix(message, "ASK") || strings.HasPrefix(message, "CROSSSLOT") || strings.HasPrefix(message, "TRYAGAIN") {
		return message
	}
	return "ERR " + message
}

func (resp *redisConn) writeInteger(value int64) error {
	_, err := resp.writer.WriteString(":" + strconv.FormatInt(value, 10) + "\r\n")
	return err
}

func (resp *redisConn) writeNullBulk() error {
	_, err := resp.writer.WriteString("$-1\r\n")
	return err
}

func (resp *redisConn) writeBulk(value []byte) error {
	if _, err := resp.writer.WriteString("$" + strconv.Itoa(len(value)) + "\r\n"); err != nil {
		return err
	}
	if _, err := resp.writer.Write(value); err != nil {
		return err
	}
	_, err := resp.writer.WriteString("\r\n")
	return err
}

func (resp *redisConn) flush() error {
	return resp.writer.Flush()
}
