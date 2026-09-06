package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/timson/pirindb/storage"
)

type redisNullArrayReply struct{}

type redisBlockingPopResult struct {
	key   []byte
	value []byte
	found bool
}

type redisBlockingMoveResult struct {
	value []byte
	found bool
}

func parseRedisBlockingPopArgs(args [][]byte) ([][]byte, time.Duration, bool, error) {
	if len(args) < 3 {
		return nil, 0, false, fmt.Errorf("wrong number of arguments for '%s' command", strings.ToLower(string(args[0])))
	}

	timeoutSeconds, err := strconv.ParseFloat(string(args[len(args)-1]), 64)
	if err != nil || timeoutSeconds < 0 {
		return nil, 0, false, errors.New("timeout must be a non-negative number")
	}

	keys := make([][]byte, 0, len(args)-2)
	for _, key := range args[1 : len(args)-1] {
		keys = append(keys, cloneBytes(key))
	}

	if timeoutSeconds == 0 {
		return keys, 0, true, nil
	}
	return keys, time.Duration(timeoutSeconds * float64(time.Second)), false, nil
}

func parseRedisBlockingMoveArgs(args [][]byte) ([]byte, []byte, time.Duration, bool, error) {
	if len(args) != 4 {
		return nil, nil, 0, false, errors.New("wrong number of arguments for 'brpoplpush' command")
	}

	timeoutSeconds, err := strconv.ParseFloat(string(args[3]), 64)
	if err != nil || timeoutSeconds < 0 {
		return nil, nil, 0, false, errors.New("timeout must be a non-negative number")
	}

	source := cloneBytes(args[1])
	destination := cloneBytes(args[2])
	if timeoutSeconds == 0 {
		return source, destination, 0, true, nil
	}
	return source, destination, time.Duration(timeoutSeconds * float64(time.Second)), false, nil
}

func (srv *RedisServer) executeBlockingPop(selectedDB int, args [][]byte, left bool) (redisReply, error) {
	keys, timeout, blockForever, err := parseRedisBlockingPopArgs(args)
	if err != nil {
		return nil, err
	}

	var deadline time.Time
	if !blockForever {
		deadline = time.Now().Add(timeout)
	}

	for {
		result, err := srv.tryPopAny(selectedDB, keys, left)
		if err != nil {
			return nil, err
		}
		if result.found {
			return redisArrayReply{
				values: []redisReply{
					redisBulkReply{value: result.key},
					redisBulkReply{value: result.value},
				},
			}, nil
		}

		if !blockForever && time.Now().After(deadline) {
			return redisNullArrayReply{}, nil
		}

		waiter := make(chan struct{}, 1)
		srv.registerListWaiter(selectedDB, keys, waiter)

		result, err = srv.tryPopAny(selectedDB, keys, left)
		if err != nil {
			srv.unregisterListWaiter(selectedDB, keys, waiter)
			return nil, err
		}
		if result.found {
			srv.unregisterListWaiter(selectedDB, keys, waiter)
			return redisArrayReply{
				values: []redisReply{
					redisBulkReply{value: result.key},
					redisBulkReply{value: result.value},
				},
			}, nil
		}

		if !blockForever {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				srv.unregisterListWaiter(selectedDB, keys, waiter)
				return redisNullArrayReply{}, nil
			}
			timer := time.NewTimer(remaining)
			select {
			case <-waiter:
			case <-timer.C:
				srv.unregisterListWaiter(selectedDB, keys, waiter)
				return redisNullArrayReply{}, nil
			case <-srv.stopCh:
				srv.unregisterListWaiter(selectedDB, keys, waiter)
				return nil, errors.New("redis server stopped")
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		} else {
			select {
			case <-waiter:
			case <-srv.stopCh:
				srv.unregisterListWaiter(selectedDB, keys, waiter)
				return nil, errors.New("redis server stopped")
			}
		}

		srv.unregisterListWaiter(selectedDB, keys, waiter)
	}
}

func (srv *RedisServer) executeBlockingMove(selectedDB int, args [][]byte) (redisReply, error) {
	source, destination, timeout, blockForever, err := parseRedisBlockingMoveArgs(args)
	if err != nil {
		return nil, err
	}

	var deadline time.Time
	if !blockForever {
		deadline = time.Now().Add(timeout)
	}

	keys := [][]byte{source}
	for {
		result, err := srv.tryMove(selectedDB, source, destination)
		if err != nil {
			return nil, err
		}
		if result.found {
			srv.notifyListWaiters(selectedDB, destination)
			return redisBulkReply{value: result.value}, nil
		}

		if !blockForever && time.Now().After(deadline) {
			return redisBulkReply{null: true}, nil
		}

		waiter := make(chan struct{}, 1)
		srv.registerListWaiter(selectedDB, keys, waiter)

		result, err = srv.tryMove(selectedDB, source, destination)
		if err != nil {
			srv.unregisterListWaiter(selectedDB, keys, waiter)
			return nil, err
		}
		if result.found {
			srv.unregisterListWaiter(selectedDB, keys, waiter)
			srv.notifyListWaiters(selectedDB, destination)
			return redisBulkReply{value: result.value}, nil
		}

		if !blockForever {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				srv.unregisterListWaiter(selectedDB, keys, waiter)
				return redisBulkReply{null: true}, nil
			}
			timer := time.NewTimer(remaining)
			select {
			case <-waiter:
			case <-timer.C:
				srv.unregisterListWaiter(selectedDB, keys, waiter)
				return redisBulkReply{null: true}, nil
			case <-srv.stopCh:
				srv.unregisterListWaiter(selectedDB, keys, waiter)
				return nil, errors.New("redis server stopped")
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		} else {
			select {
			case <-waiter:
			case <-srv.stopCh:
				srv.unregisterListWaiter(selectedDB, keys, waiter)
				return nil, errors.New("redis server stopped")
			}
		}

		srv.unregisterListWaiter(selectedDB, keys, waiter)
	}
}

func (srv *RedisServer) tryPopAny(selectedDB int, keys [][]byte, left bool) (redisBlockingPopResult, error) {
	unlock := srv.mutationLocks.lockKeys(selectedDB, keys, false)
	defer unlock()
	ns := srv.redisNamespace(selectedDB)
	nowMs := srv.nowUnixMilli()
	var result redisBlockingPopResult
	err := srv.DB.Update(func(tx *storage.Tx) error {
		for _, key := range keys {
			value, found, err := redisPopTx(tx, ns, key, left, nowMs)
			if err != nil {
				return err
			}
			if found {
				result = redisBlockingPopResult{
					key:   cloneBytes(key),
					value: value,
					found: true,
				}
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return redisBlockingPopResult{}, err
	}
	return result, nil
}

func (srv *RedisServer) tryMove(selectedDB int, source, destination []byte) (redisBlockingMoveResult, error) {
	unlock := srv.mutationLocks.lockKeys(selectedDB, [][]byte{source, destination}, false)
	defer unlock()
	ns := srv.redisNamespace(selectedDB)
	nowMs := srv.nowUnixMilli()
	var result redisBlockingMoveResult
	err := srv.DB.Update(func(tx *storage.Tx) error {
		value, found, err := redisMoveTx(tx, ns, source, destination, nowMs)
		if err != nil {
			return err
		}
		if found {
			result = redisBlockingMoveResult{
				value: value,
				found: true,
			}
		}
		return nil
	})
	if err != nil {
		return redisBlockingMoveResult{}, err
	}
	return result, nil
}

func (srv *RedisServer) registerListWaiter(selectedDB int, keys [][]byte, waiter chan struct{}) {
	srv.waitMu.Lock()
	defer srv.waitMu.Unlock()

	for _, key := range keys {
		waiterKey := redisWaiterKey(selectedDB, key)
		waiters := srv.waiters[waiterKey]
		if waiters == nil {
			waiters = make(map[chan struct{}]struct{})
			srv.waiters[waiterKey] = waiters
		}
		waiters[waiter] = struct{}{}
	}
}

func (srv *RedisServer) unregisterListWaiter(selectedDB int, keys [][]byte, waiter chan struct{}) {
	srv.waitMu.Lock()
	defer srv.waitMu.Unlock()

	for _, key := range keys {
		waiterKey := redisWaiterKey(selectedDB, key)
		waiters := srv.waiters[waiterKey]
		if waiters == nil {
			continue
		}
		delete(waiters, waiter)
		if len(waiters) == 0 {
			delete(srv.waiters, waiterKey)
		}
	}
}

func (srv *RedisServer) notifyListWaiters(selectedDB int, keys ...[]byte) {
	srv.waitMu.Lock()
	targets := make(map[chan struct{}]struct{})
	for _, key := range keys {
		for waiter := range srv.waiters[redisWaiterKey(selectedDB, key)] {
			targets[waiter] = struct{}{}
		}
	}
	srv.waitMu.Unlock()

	for waiter := range targets {
		select {
		case waiter <- struct{}{}:
		default:
		}
	}
}

func (srv *RedisServer) notifyPipelineListWrites(selectedDB int, queuedCommands [][][]byte) {
	keySet := make(map[string][]byte)
	renameCandidates := make(map[string][]byte)
	for _, args := range queuedCommands {
		if len(args) < 2 {
			continue
		}
		switch strings.ToUpper(string(args[0])) {
		case "LPUSH", "RPUSH":
			keyString := string(args[1])
			if _, exists := keySet[keyString]; !exists {
				keySet[keyString] = cloneBytes(args[1])
			}
		case "RPOPLPUSH":
			if len(args) < 3 {
				continue
			}
			keyString := string(args[2])
			if _, exists := keySet[keyString]; !exists {
				keySet[keyString] = cloneBytes(args[2])
			}
		case "RENAME", "RENAMENX":
			if len(args) < 3 {
				continue
			}
			renameCandidates[string(args[2])] = cloneBytes(args[2])
		}
	}

	if len(renameCandidates) > 0 {
		nowMs := srv.nowUnixMilli()
		ns := srv.redisNamespace(selectedDB)
		_ = srv.DB.View(func(tx *storage.Tx) error {
			for keyString, key := range renameCandidates {
				keyType, err := redisKeyTypeTx(tx, ns, key, nowMs)
				if err != nil {
					continue
				}
				if keyType == redisKeyTypeList {
					if _, exists := keySet[keyString]; !exists {
						keySet[keyString] = key
					}
				}
			}
			return nil
		})
	}

	keys := make([][]byte, 0, len(keySet))
	for _, key := range keySet {
		keys = append(keys, key)
	}
	srv.notifyListWaiters(selectedDB, keys...)
}

func (reply redisNullArrayReply) writeRESP(conn *redisConn) error {
	_, err := conn.writer.WriteString("*-1\r\n")
	return err
}
