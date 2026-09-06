package main

import (
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"

	"github.com/timson/pirindb/storage"
)

func cloneArgs(args [][]byte) [][]byte {
	cloned := make([][]byte, 0, len(args))
	for _, arg := range args {
		cloned = append(cloned, cloneBytes(arg))
	}
	return cloned
}

func isRedisWriteCommand(command string) bool {
	switch command {
	case "BF.ADD", "BF.MADD", "BF.RESERVE",
		"TOPK.ADD", "TOPK.INCRBY", "TOPK.RESERVE",
		"BLPOP", "BRPOP", "BRPOPLPUSH",
		"DEL", "UNLINK", "EXPIRE", "PEXPIRE", "PERSIST",
		"FLUSHALL", "FLUSHDB",
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

func isRedisGlobalClusterWriteCommand(command string) bool {
	switch command {
	case "FLUSHALL", "FLUSHDB":
		return true
	default:
		return false
	}
}

func isRedisOnlineRebalanceUnsupportedWrite(command string) bool {
	switch command {
	case "BLPOP", "BRPOP", "BRPOPLPUSH", "FLUSHALL", "FLUSHDB":
		return true
	default:
		return false
	}
}

func redisCommandKeys(args [][]byte) ([][]byte, error) {
	if len(args) == 0 {
		return nil, nil
	}

	command := strings.ToUpper(string(args[0]))
	switch command {
	case "BF.ADD", "BF.EXISTS", "BF.MADD", "BF.MEXISTS", "BF.RESERVE",
		"TOPK.ADD", "TOPK.COUNT", "TOPK.INFO", "TOPK.INCRBY", "TOPK.LIST", "TOPK.QUERY", "TOPK.RESERVE",
		"DECR", "DECRBY", "EXPIRE", "GET", "GETSET", "HDEL", "HEXISTS", "HGET", "HGETALL", "HKEYS", "HLEN", "HSET", "HVALS",
		"INCR", "INCRBY", "LINDEX", "LLEN", "LPOP", "LPUSH", "LRANGE", "LREM", "LSET", "LTRIM", "RPOP", "RPUSH",
		"PERSIST", "PEXPIRE", "PTTL", "SET", "TTL", "TYPE", "ZADD", "ZCARD", "ZREM", "ZRANGEBYSCORE", "ZREVRANGEBYSCORE",
		"ZRANGEBYLEX", "ZREVRANGEBYLEX", "ZREMRANGEBYSCORE", "ZREMRANGEBYLEX", "ZSCORE":
		return [][]byte{args[1]}, nil
	case "BLPOP", "BRPOP":
		return cloneArgs(args[1 : len(args)-1]), nil
	case "BRPOPLPUSH", "RENAME", "RENAMENX", "RPOPLPUSH":
		return [][]byte{args[1], args[2]}, nil
	case "DEL", "UNLINK", "EXISTS", "MGET":
		return cloneArgs(args[1:]), nil
	case "MSET":
		keys := make([][]byte, 0, (len(args)-1)/2)
		for i := 1; i < len(args); i += 2 {
			keys = append(keys, cloneBytes(args[i]))
		}
		return keys, nil
	default:
		return nil, nil
	}
}

func isMultiKeyClusterCommand(command string, keys [][]byte) bool {
	if len(keys) <= 1 {
		return false
	}
	switch command {
	case "BLPOP", "BRPOP", "DEL", "EXISTS", "MGET", "MSET", "UNLINK":
		return true
	default:
		return false
	}
}

func (srv *RedisServer) ensureClusterRoute(selectedDB int, args [][]byte, tx *storage.Tx, nowMs int64, allowAsking bool) error {
	_ = selectedDB
	_ = tx
	_ = nowMs

	if srv.Cluster == nil {
		return nil
	}

	command := strings.ToUpper(string(args[0]))
	state := srv.Cluster.Snapshot()
	if state == nil {
		return errors.New("cluster state is unavailable")
	}
	if state.Rebalancing && isRedisGlobalClusterWriteCommand(command) {
		return errors.New("TRYAGAIN cluster rebalancing is in progress")
	}

	keys, err := redisCommandKeys(args)
	if err != nil {
		return err
	}
	route, err := srv.Cluster.RouteKeys(keys)
	if err != nil {
		return err
	}
	if route == nil || !route.hasKey {
		return nil
	}

	if move := state.PendingMoveForSlot(route.slot); move != nil {
		return srv.ensureMovingSlotRoute(state, route, move, command, keys, allowAsking)
	}

	if route.owner.ID != srv.Cluster.LocalNodeID {
		return fmt.Errorf("MOVED %d %s", route.slot, route.owner.RedisAddress)
	}
	return nil
}

func (srv *RedisServer) ensureMovingSlotRoute(state *clusterState, route *clusterRoute, move *clusterPendingMove, command string, keys [][]byte, allowAsking bool) error {
	sourceNode, _ := state.NodeByID(move.SourceNodeID)
	destinationNode, _ := state.NodeByID(move.DestinationNodeID)
	isSource := srv.Cluster.LocalNodeID == move.SourceNodeID
	isDestination := srv.Cluster.LocalNodeID == move.DestinationNodeID

	switch move.Stage {
	case clusterMoveStageCopying:
		if isSource {
			if isRedisOnlineRebalanceUnsupportedWrite(command) {
				return fmt.Errorf("TRYAGAIN slot %d is being migrated", route.slot)
			}
			return nil
		}
		return fmt.Errorf("MOVED %d %s", route.slot, sourceNode.RedisAddress)
	case clusterMoveStageCatchup:
		if isSource {
			if isRedisWriteCommand(command) {
				return fmt.Errorf("TRYAGAIN slot %d is being migrated", route.slot)
			}
			return nil
		}
		return fmt.Errorf("MOVED %d %s", route.slot, sourceNode.RedisAddress)
	case clusterMoveStageAsking:
		if isMultiKeyClusterCommand(command, keys) {
			return errors.New("TRYAGAIN Multiple keys request during rehashing of slot")
		}
		if isSource {
			return fmt.Errorf("ASK %d %s", route.slot, destinationNode.RedisAddress)
		}
		if isDestination {
			if allowAsking {
				return nil
			}
			return fmt.Errorf("MOVED %d %s", route.slot, sourceNode.RedisAddress)
		}
		return fmt.Errorf("MOVED %d %s", route.slot, sourceNode.RedisAddress)
	default:
		if route.owner.ID != srv.Cluster.LocalNodeID {
			return fmt.Errorf("MOVED %d %s", route.slot, route.owner.RedisAddress)
		}
		return nil
	}
}

func (srv *RedisServer) ensurePipelineClusterRoute(selectedDB int, queuedCommands [][][]byte, allowAsking bool) error {
	_ = selectedDB

	if srv.Cluster == nil {
		return nil
	}
	var combinedKeys [][]byte
	for _, args := range queuedCommands {
		if err := srv.ensureClusterRoute(selectedDB, args, nil, 0, allowAsking); err != nil {
			return err
		}
		keys, err := redisCommandKeys(args)
		if err != nil {
			return err
		}
		combinedKeys = append(combinedKeys, keys...)
	}
	_, err := srv.Cluster.RouteKeys(combinedKeys)
	return err
}

func (srv *RedisServer) executeClusterCommand(args [][]byte) (redisReply, error) {
	if srv.Cluster == nil {
		return nil, errors.New("cluster mode is disabled")
	}
	state := srv.Cluster.Snapshot()
	if state == nil {
		return nil, errors.New("cluster state is unavailable")
	}

	switch strings.ToUpper(string(args[1])) {
	case "KEYSLOT":
		return redisIntegerReply{value: int64(ClusterKeySlot(args[2], state.SlotCount))}, nil
	case "INFO":
		return buildRedisClusterInfoReply(state)
	case "NODES":
		return buildRedisClusterNodesReply(state, srv.Cluster.LocalNodeID)
	case "SLOTS":
		return buildRedisClusterSlotsReply(clusterStateSlotRanges(state))
	case "SHARDS":
		return buildRedisClusterShardsReply(state.Nodes, clusterStateSlotRanges(state))
	default:
		return nil, fmt.Errorf("unsupported cluster subcommand '%s'", strings.ToLower(string(args[1])))
	}
}

func buildRedisClusterInfoReply(state *clusterState) (redisReply, error) {
	if state == nil {
		return nil, errors.New("cluster state is unavailable")
	}
	if state.SlotCount <= 0 {
		return nil, errors.New("cluster slot count must be greater than zero")
	}

	knownNodes := make(map[string]struct{}, len(state.Nodes))
	for _, node := range state.Nodes {
		knownNodes[node.ID] = struct{}{}
	}

	assignedSlots := 0
	okSlots := 0
	failedSlots := 0
	slotServingNodes := make(map[string]struct{}, len(state.Nodes))
	for slot := 0; slot < state.SlotCount; slot++ {
		if slot >= len(state.SlotOwners) || state.SlotOwners[slot] == "" {
			continue
		}
		assignedSlots++
		ownerID := state.SlotOwners[slot]
		if _, known := knownNodes[ownerID]; !known {
			failedSlots++
			continue
		}
		okSlots++
		slotServingNodes[ownerID] = struct{}{}
	}

	clusterStateName := "ok"
	if assignedSlots != state.SlotCount || failedSlots != 0 {
		clusterStateName = "fail"
	}

	var builder strings.Builder
	fmt.Fprintf(&builder, "cluster_state:%s\r\n", clusterStateName)
	fmt.Fprintf(&builder, "cluster_slots_assigned:%d\r\n", assignedSlots)
	fmt.Fprintf(&builder, "cluster_slots_ok:%d\r\n", okSlots)
	builder.WriteString("cluster_slots_pfail:0\r\n")
	fmt.Fprintf(&builder, "cluster_slots_fail:%d\r\n", failedSlots)
	fmt.Fprintf(&builder, "cluster_known_nodes:%d\r\n", len(state.Nodes))
	fmt.Fprintf(&builder, "cluster_size:%d\r\n", len(slotServingNodes))
	fmt.Fprintf(&builder, "cluster_current_epoch:%d\r\n", state.Epoch)
	fmt.Fprintf(&builder, "cluster_my_epoch:%d\r\n", state.Epoch)
	builder.WriteString("cluster_stats_messages_sent:0\r\n")
	builder.WriteString("cluster_stats_messages_received:0\r\n")
	builder.WriteString("total_cluster_links_buffer_limit_exceeded:0\r\n")

	return redisBulkReply{value: []byte(builder.String())}, nil
}

func buildRedisClusterNodesReply(state *clusterState, localNodeID string) (redisReply, error) {
	if state == nil {
		return nil, errors.New("cluster state is unavailable")
	}

	nodes := append([]clusterNodeState(nil), state.Nodes...)
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})

	rangesByNode := make(map[string][]clusterSlotRange, len(nodes))
	for _, slotRange := range clusterStateSlotRanges(state) {
		rangesByNode[slotRange.Node.ID] = append(rangesByNode[slotRange.Node.ID], slotRange)
	}

	localNodeFound := false
	var builder strings.Builder
	for _, node := range nodes {
		host, port, err := splitRedisAddress(node.RedisAddress)
		if err != nil {
			return nil, err
		}
		flags := "master"
		if node.ID == localNodeID {
			localNodeFound = true
			flags = "myself,master"
		}

		// PirinDB has no separate Redis cluster bus, so its bus port is
		// explicitly advertised as zero while retaining the standard field.
		address := net.JoinHostPort(host, strconv.Itoa(port)) + "@0"
		fmt.Fprintf(&builder, "%s %s %s - 0 0 %d connected", node.ID, address, flags, state.Epoch)
		for _, slotRange := range rangesByNode[node.ID] {
			builder.WriteByte(' ')
			writeRedisClusterSlotRange(&builder, slotRange.StartSlot, slotRange.EndSlot)
		}
		writeRedisClusterMigrationSlots(&builder, state, node.ID, localNodeID)
		builder.WriteByte('\n')
	}
	if !localNodeFound {
		return nil, fmt.Errorf("local cluster node %q is not in cluster state", localNodeID)
	}

	return redisBulkReply{value: []byte(builder.String())}, nil
}

func writeRedisClusterSlotRange(builder *strings.Builder, startSlot int, endSlot int) {
	if startSlot == endSlot {
		fmt.Fprintf(builder, "%d", startSlot)
		return
	}
	fmt.Fprintf(builder, "%d-%d", startSlot, endSlot)
}

func writeRedisClusterMigrationSlots(builder *strings.Builder, state *clusterState, nodeID string, localNodeID string) {
	move := state.PendingMove
	if move == nil || move.Stage != clusterMoveStageAsking || nodeID != localNodeID {
		return
	}

	for slot := move.StartSlot; slot <= move.EndSlot; slot++ {
		switch nodeID {
		case move.SourceNodeID:
			fmt.Fprintf(builder, " [%d->-%s]", slot, move.DestinationNodeID)
		case move.DestinationNodeID:
			fmt.Fprintf(builder, " [%d-<-%s]", slot, move.SourceNodeID)
		}
	}
}

func buildRedisClusterSlotsReply(ranges []clusterSlotRange) (redisReply, error) {
	replies := make([]redisReply, 0, len(ranges))
	for _, slotRange := range ranges {
		host, port, err := splitRedisAddress(slotRange.Node.RedisAddress)
		if err != nil {
			return nil, err
		}
		replies = append(replies, redisArrayReply{
			values: []redisReply{
				redisIntegerReply{value: int64(slotRange.StartSlot)},
				redisIntegerReply{value: int64(slotRange.EndSlot)},
				redisArrayReply{
					values: []redisReply{
						redisBulkReply{value: []byte(host)},
						redisIntegerReply{value: int64(port)},
						redisBulkReply{value: []byte(slotRange.Node.ID)},
					},
				},
			},
		})
	}
	return redisArrayReply{values: replies}, nil
}

func buildRedisClusterShardsReply(nodes []clusterNodeState, ranges []clusterSlotRange) (redisReply, error) {
	rangesByNode := make(map[string][]clusterSlotRange, len(nodes))
	for _, slotRange := range ranges {
		rangesByNode[slotRange.Node.ID] = append(rangesByNode[slotRange.Node.ID], slotRange)
	}

	shards := make([]redisReply, 0, len(nodes))
	for _, node := range nodes {
		nodeRanges := rangesByNode[node.ID]
		host, port, err := splitRedisAddress(node.RedisAddress)
		if err != nil {
			return nil, err
		}
		slotReplies := make([]redisReply, 0, len(nodeRanges))
		for _, slotRange := range nodeRanges {
			slotReplies = append(slotReplies, redisArrayReply{
				values: []redisReply{
					redisIntegerReply{value: int64(slotRange.StartSlot)},
					redisIntegerReply{value: int64(slotRange.EndSlot)},
				},
			})
		}
		nodeReply := redisArrayReply{
			values: []redisReply{
				redisBulkReply{value: []byte("id")},
				redisBulkReply{value: []byte(node.ID)},
				redisBulkReply{value: []byte("endpoint")},
				redisBulkReply{value: []byte(host)},
				redisBulkReply{value: []byte("ip")},
				redisBulkReply{value: []byte(host)},
				redisBulkReply{value: []byte("port")},
				redisIntegerReply{value: int64(port)},
				redisBulkReply{value: []byte("role")},
				redisBulkReply{value: []byte("master")},
				redisBulkReply{value: []byte("health")},
				redisBulkReply{value: []byte("online")},
			},
		}
		shards = append(shards, redisArrayReply{
			values: []redisReply{
				redisBulkReply{value: []byte("slots")},
				redisArrayReply{values: slotReplies},
				redisBulkReply{value: []byte("nodes")},
				redisArrayReply{values: []redisReply{nodeReply}},
			},
		})
	}
	return redisArrayReply{values: shards}, nil
}

func splitRedisAddress(address string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(address)
	if err != nil {
		return "", 0, fmt.Errorf("invalid redis address %q: %w", address, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("invalid redis port in address %q: %w", address, err)
	}
	return host, port, nil
}
