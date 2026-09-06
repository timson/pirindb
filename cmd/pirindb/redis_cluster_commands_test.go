package main

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRedisClusterInfoAndNodesCommands(t *testing.T) {
	cfg := newTestClusterConfig(t, "a", filepath.Join(t.TempDir(), "cluster-info-nodes.db"))
	clusterServer := openTestClusterServer(t, cfg)
	server := NewRedisServer(clusterServer.Config, clusterServer.DB, clusterServer.Logger)
	server.Cluster = clusterServer.Cluster
	state := buildClusterStateForTests(cfg.Cluster.SlotCount, "http://127.0.0.1:17000", "http://127.0.0.1:17001")
	state.Epoch = 7
	require.NoError(t, server.Cluster.ReplaceState(state))

	nowMs := redisNowUnixMilli(time.Now())
	regularInfo, err := server.buildRedisInfo(nowMs)
	require.NoError(t, err)
	require.Contains(t, regularInfo, "redis_mode:cluster\r\n")
	require.Contains(t, regularInfo, "cluster_enabled:1\r\n")

	reply, err := server.executeCommand(0, [][]byte{[]byte("CLUSTER"), []byte("INFO")}, nil, nowMs, false)
	require.NoError(t, err)
	require.Equal(t, strings.Join([]string{
		"cluster_state:ok",
		"cluster_slots_assigned:128",
		"cluster_slots_ok:128",
		"cluster_slots_pfail:0",
		"cluster_slots_fail:0",
		"cluster_known_nodes:2",
		"cluster_size:2",
		"cluster_current_epoch:7",
		"cluster_my_epoch:7",
		"cluster_stats_messages_sent:0",
		"cluster_stats_messages_received:0",
		"total_cluster_links_buffer_limit_exceeded:0",
		"",
	}, "\r\n"), string(reply.(redisBulkReply).value))

	reply, err = server.executeCommand(0, [][]byte{[]byte("CLUSTER"), []byte("NODES")}, nil, nowMs, false)
	require.NoError(t, err)
	require.Equal(t, strings.Join([]string{
		"a 127.0.0.1:7000@0 myself,master - 0 0 7 connected 0-63",
		"b 127.0.0.1:7001@0 master - 0 0 7 connected 64-127",
		"",
	}, "\n"), string(reply.(redisBulkReply).value))
}

func TestRedisClusterInfoDistinguishesKnownNodesFromSlotServingNodes(t *testing.T) {
	state := buildClusterStateForTestsABC(128, "http://127.0.0.1:17000", "http://127.0.0.1:17001", "http://127.0.0.1:17002")
	state.Epoch = 11

	reply, err := buildRedisClusterInfoReply(state)
	require.NoError(t, err)
	info := string(reply.(redisBulkReply).value)
	require.Contains(t, info, "cluster_state:ok\r\n")
	require.Contains(t, info, "cluster_known_nodes:3\r\n")
	require.Contains(t, info, "cluster_size:2\r\n")

	nodesReply, err := buildRedisClusterNodesReply(state, "c")
	require.NoError(t, err)
	nodes := string(nodesReply.(redisBulkReply).value)
	require.Contains(t, nodes, "c 127.0.0.1:7002@0 myself,master - 0 0 11 connected\n")
}

func TestRedisClusterInfoReportsIncompleteOrInvalidSlotCoverage(t *testing.T) {
	t.Run("unassigned slot", func(t *testing.T) {
		state := buildClusterStateForTests(128, "http://127.0.0.1:17000", "http://127.0.0.1:17001")
		state.SlotOwners[0] = ""

		reply, err := buildRedisClusterInfoReply(state)
		require.NoError(t, err)
		info := string(reply.(redisBulkReply).value)
		require.Contains(t, info, "cluster_state:fail\r\n")
		require.Contains(t, info, "cluster_slots_assigned:127\r\n")
		require.Contains(t, info, "cluster_slots_ok:127\r\n")
		require.Contains(t, info, "cluster_slots_fail:0\r\n")
	})

	t.Run("unknown owner", func(t *testing.T) {
		state := buildClusterStateForTests(128, "http://127.0.0.1:17000", "http://127.0.0.1:17001")
		state.SlotOwners[0] = "unknown"

		reply, err := buildRedisClusterInfoReply(state)
		require.NoError(t, err)
		info := string(reply.(redisBulkReply).value)
		require.Contains(t, info, "cluster_state:fail\r\n")
		require.Contains(t, info, "cluster_slots_assigned:128\r\n")
		require.Contains(t, info, "cluster_slots_ok:127\r\n")
		require.Contains(t, info, "cluster_slots_fail:1\r\n")
	})
}

func TestRedisClusterNodesReportsAskingMigrationOnLocalNode(t *testing.T) {
	state := buildClusterStateForTests(128, "http://127.0.0.1:17000", "http://127.0.0.1:17001")
	state.Epoch = 9
	state.Rebalancing = true
	state.PendingMove = &clusterPendingMove{
		SourceNodeID:      "a",
		DestinationNodeID: "b",
		StartSlot:         10,
		EndSlot:           11,
		Stage:             clusterMoveStageAsking,
	}

	sourceReply, err := buildRedisClusterNodesReply(state, "a")
	require.NoError(t, err)
	sourceNodes := string(sourceReply.(redisBulkReply).value)
	require.Contains(t, sourceNodes, "a 127.0.0.1:7000@0 myself,master - 0 0 9 connected 0-63 [10->-b] [11->-b]\n")
	require.NotContains(t, sourceNodes, "[10-<-a]")

	destinationReply, err := buildRedisClusterNodesReply(state, "b")
	require.NoError(t, err)
	destinationNodes := string(destinationReply.(redisBulkReply).value)
	require.Contains(t, destinationNodes, "b 127.0.0.1:7001@0 myself,master - 0 0 9 connected 64-127 [10-<-a] [11-<-a]\n")
	require.NotContains(t, destinationNodes, "[10->-b]")

	state.PendingMove.Stage = clusterMoveStageCopying
	copyingReply, err := buildRedisClusterNodesReply(state, "a")
	require.NoError(t, err)
	copyingNodes := string(copyingReply.(redisBulkReply).value)
	require.NotContains(t, copyingNodes, "->-")
	require.NotContains(t, copyingNodes, "-<-")
}

func TestRedisClusterInfoAndNodesValidation(t *testing.T) {
	for _, subcommand := range []string{"INFO", "NODES"} {
		t.Run(strings.ToLower(subcommand), func(t *testing.T) {
			require.NoError(t, validateRedisCommand([][]byte{[]byte("CLUSTER"), []byte(subcommand)}))
			err := validateRedisCommand([][]byte{[]byte("CLUSTER"), []byte(subcommand), []byte("extra")})
			require.EqualError(t, err, "wrong number of arguments for 'cluster|"+strings.ToLower(subcommand)+"' command")
		})
	}

	state := buildClusterStateForTests(128, "http://127.0.0.1:17000", "http://127.0.0.1:17001")
	_, err := buildRedisClusterNodesReply(state, "missing")
	require.EqualError(t, err, `local cluster node "missing" is not in cluster state`)
}
