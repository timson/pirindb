package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/timson/pirindb/storage"
)

func newTestClusterNodesAB() []*ClusterNodeConfig {
	return []*ClusterNodeConfig{
		{
			ID:           "a",
			RedisAddress: "127.0.0.1:7000",
			HTTPAddress:  "http://127.0.0.1:17000",
			Slots:        []string{"0-63"},
		},
		{
			ID:           "b",
			RedisAddress: "127.0.0.1:7001",
			HTTPAddress:  "http://127.0.0.1:17001",
			Slots:        []string{"64-127"},
		},
	}
}

func newTestClusterNodesABC() []*ClusterNodeConfig {
	nodes := newTestClusterNodesAB()
	nodes = append(nodes, &ClusterNodeConfig{
		ID:           "c",
		RedisAddress: "127.0.0.1:7002",
		HTTPAddress:  "http://127.0.0.1:17002",
		Slots:        nil,
	})
	return nodes
}

func newTestClusterTopologyAB() *ClusterTopologyConfig {
	return &ClusterTopologyConfig{
		Name:      "test-cluster",
		SlotCount: 128,
		Nodes:     newTestClusterNodesAB(),
		BootstrapSlots: []*ClusterBootstrapSlotsConfig{
			{NodeID: "a", Slots: []string{"0-63"}},
			{NodeID: "b", Slots: []string{"64-127"}},
		},
	}
}

func newTestClusterTopologyABC() *ClusterTopologyConfig {
	return &ClusterTopologyConfig{
		Name:      "test-cluster",
		SlotCount: 128,
		Nodes:     newTestClusterNodesABC(),
		BootstrapSlots: []*ClusterBootstrapSlotsConfig{
			{NodeID: "a", Slots: []string{"0-63"}},
			{NodeID: "b", Slots: []string{"64-127"}},
		},
	}
}

func newTestClusterConfigWithNodes(t *testing.T, nodeID string, dbPath string, nodes []*ClusterNodeConfig) *Config {
	t.Helper()
	return &Config{
		Server: &ServerConfig{
			Host:     "127.0.0.1",
			Port:     0,
			LogLevel: "ERROR",
		},
		Redis: &RedisConfig{
			Enabled: true,
			Host:    "127.0.0.1",
			Port:    0,
		},
		Cluster: &ClusterConfig{
			Enabled:   true,
			NodeID:    nodeID,
			SlotCount: 128,
			Nodes:     nodes,
		},
		DB: &DatabaseConfig{
			Filename:               dbPath,
			SyncPolicy:             "strict",
			CheckpointTxThreshold:  64,
			GroupCommitTxThreshold: 16,
			GroupCommitWindowMs:    1,
		},
	}
}

func newTestClusterConfig(t *testing.T, nodeID string, dbPath string) *Config {
	t.Helper()
	return newTestClusterConfigWithNodes(t, nodeID, dbPath, newTestClusterNodesAB())
}

func newTestClusterConfigWithTopology(t *testing.T, nodeID string, dbPath string, topology *ClusterTopologyConfig) *Config {
	t.Helper()
	return &Config{
		Server: &ServerConfig{
			Host:     "127.0.0.1",
			Port:     0,
			LogLevel: "ERROR",
		},
		Redis: &RedisConfig{
			Enabled: true,
			Host:    "127.0.0.1",
			Port:    0,
		},
		Cluster: &ClusterConfig{
			Enabled:   true,
			NodeID:    nodeID,
			SlotCount: topology.SlotCount,
			Topology:  cloneClusterTopology(topology),
		},
		DB: &DatabaseConfig{
			Filename:               dbPath,
			SyncPolicy:             "strict",
			CheckpointTxThreshold:  64,
			GroupCommitTxThreshold: 16,
			GroupCommitWindowMs:    1,
		},
	}
}

func openTestClusterServer(t *testing.T, cfg *Config) *Server {
	t.Helper()
	logger := createLogger("ERROR")
	storage.SetLogger(logger)
	db, err := storage.Open(cfg.DB.Filename, cfg.DB.StorageOptions())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
		_ = os.Remove(cfg.DB.Filename)
		_ = os.Remove(db.GetOptions().TxLogPath)
	})
	return NewServer(cfg, db, logger)
}

func buildClusterStateForTests(slotCount int, sourceHTTP string, destHTTP string) *clusterState {
	owners := make([]string, slotCount)
	for slot := 0; slot < slotCount; slot++ {
		if slot <= 63 {
			owners[slot] = "a"
		} else {
			owners[slot] = "b"
		}
	}
	return &clusterState{
		ClusterID: "test-cluster",
		Version:   1,
		SlotCount: slotCount,
		Nodes: []clusterNodeState{
			{ID: "a", RedisAddress: "127.0.0.1:7000", HTTPAddress: sourceHTTP},
			{ID: "b", RedisAddress: "127.0.0.1:7001", HTTPAddress: destHTTP},
		},
		SlotOwners: owners,
	}
}

func findKeyForOwner(state *clusterState, ownerID string) []byte {
	for i := 0; i < 100000; i++ {
		key := []byte("cluster:key:" + strconv.Itoa(i))
		slot := ClusterKeySlot(key, state.SlotCount)
		if state.SlotOwners[slot] == ownerID {
			return key
		}
	}
	return nil
}

func findHashTagForOwner(state *clusterState, ownerID string) string {
	for i := 0; i < 100000; i++ {
		tag := "cluster-tag-" + strconv.Itoa(i)
		key := []byte("{" + tag + "}")
		slot := ClusterKeySlot(key, state.SlotCount)
		if state.SlotOwners[slot] == ownerID {
			return tag
		}
	}
	return ""
}

func TestRedisClusterRedirectAndCrossSlot(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "cluster-redis.db")
	cfg := newTestClusterConfig(t, "a", dbPath)
	logger := createLogger("ERROR")
	storage.SetLogger(logger)
	db, err := storage.Open(dbPath, cfg.DB.StorageOptions())
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	server := NewRedisServer(cfg, db, logger)
	require.NoError(t, server.clusterErr)
	state := buildClusterStateForTests(cfg.Cluster.SlotCount, "http://127.0.0.1:17000", "http://127.0.0.1:17001")
	require.NoError(t, server.Cluster.ReplaceState(state))

	localKey := findKeyForOwner(state, "a")
	remoteKey := findKeyForOwner(state, "b")
	require.NotNil(t, localKey)
	require.NotNil(t, remoteKey)

	require.NoError(t, PutRedisBytes(db, redisNamespaceForDB(0, state.SlotCount), localKey, []byte("local"), redisNowUnixMilli(time.Now())))

	reply, err := server.executeCommand(0, [][]byte{[]byte("GET"), localKey}, nil, redisNowUnixMilli(time.Now()), false)
	require.NoError(t, err)
	require.Equal(t, "local", string(reply.(redisBulkReply).value))

	_, err = server.executeCommand(0, [][]byte{[]byte("GET"), remoteKey}, nil, redisNowUnixMilli(time.Now()), false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "MOVED")
	require.Contains(t, err.Error(), "127.0.0.1:7001")

	_, err = server.executeCommand(0, [][]byte{[]byte("MGET"), localKey, remoteKey}, nil, redisNowUnixMilli(time.Now()), false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "CROSSSLOT")
}

func TestRedisClusterCommands(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "cluster-commands.db")
	cfg := newTestClusterConfig(t, "a", dbPath)
	logger := createLogger("ERROR")
	storage.SetLogger(logger)
	db, err := storage.Open(dbPath, cfg.DB.StorageOptions())
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	server := NewRedisServer(cfg, db, logger)
	require.NoError(t, server.clusterErr)
	state := buildClusterStateForTests(cfg.Cluster.SlotCount, "http://127.0.0.1:17000", "http://127.0.0.1:17001")
	require.NoError(t, server.Cluster.ReplaceState(state))

	key := []byte("cluster:{42}:key")
	nowMs := redisNowUnixMilli(time.Now())

	reply, err := server.executeCommand(0, [][]byte{[]byte("CLUSTER"), []byte("KEYSLOT"), key}, nil, nowMs, false)
	require.NoError(t, err)
	require.Equal(t, int64(ClusterKeySlot(key, state.SlotCount)), reply.(redisIntegerReply).value)

	reply, err = server.executeCommand(0, [][]byte{[]byte("CLUSTER"), []byte("SLOTS")}, nil, nowMs, false)
	require.NoError(t, err)
	slotRanges := reply.(redisArrayReply)
	require.Len(t, slotRanges.values, 2)
	firstRange := slotRanges.values[0].(redisArrayReply)
	require.Equal(t, int64(0), firstRange.values[0].(redisIntegerReply).value)
	require.Equal(t, int64(63), firstRange.values[1].(redisIntegerReply).value)
	firstNode := firstRange.values[2].(redisArrayReply)
	require.Equal(t, "127.0.0.1", string(firstNode.values[0].(redisBulkReply).value))
	require.Equal(t, int64(7000), firstNode.values[1].(redisIntegerReply).value)
	require.Equal(t, "a", string(firstNode.values[2].(redisBulkReply).value))

	reply, err = server.executeCommand(0, [][]byte{[]byte("CLUSTER"), []byte("SHARDS")}, nil, nowMs, false)
	require.NoError(t, err)
	shards := reply.(redisArrayReply)
	require.Len(t, shards.values, 2)
	firstShard := shards.values[0].(redisArrayReply)
	require.Equal(t, "slots", string(firstShard.values[0].(redisBulkReply).value))
	require.Equal(t, "nodes", string(firstShard.values[2].(redisBulkReply).value))
	nodeList := firstShard.values[3].(redisArrayReply)
	require.Len(t, nodeList.values, 1)
	nodeEntry := nodeList.values[0].(redisArrayReply)
	require.Equal(t, "id", string(nodeEntry.values[0].(redisBulkReply).value))
	require.Equal(t, "a", string(nodeEntry.values[1].(redisBulkReply).value))
}

func TestClusterSlotStreamRoundTripWithBlobBackedString(t *testing.T) {
	tmpDir := t.TempDir()
	sourceDBPath := filepath.Join(tmpDir, "stream-source.db")
	destDBPath := filepath.Join(tmpDir, "stream-dest.db")
	sourceSrv := openTestClusterServer(t, newTestClusterConfig(t, "a", sourceDBPath))
	destSrv := openTestClusterServer(t, newTestClusterConfig(t, "b", destDBPath))

	state := buildClusterStateForTests(sourceSrv.Cluster.Snapshot().SlotCount, "http://127.0.0.1:17000", "http://127.0.0.1:17001")
	require.NoError(t, sourceSrv.Cluster.ReplaceState(state))
	require.NoError(t, destSrv.Cluster.ReplaceState(state))

	key := findKeyForOwner(state, "a")
	require.NotNil(t, key)
	slot := ClusterKeySlot(key, state.SlotCount)
	nowMs := redisNowUnixMilli(time.Now())
	value := bytes.Repeat([]byte("cluster-stream-blob-"), 4096)

	require.NoError(t, PutRedisBytes(sourceSrv.DB, redisNamespaceForDB(0, state.SlotCount), key, value, nowMs))

	var stream bytes.Buffer
	require.NoError(t, streamClusterSlotRange(sourceSrv.DB, &stream, state.SlotCount, slot, slot, nowMs))
	_, err := importClusterSlotStream(destSrv.DB, bytes.NewReader(stream.Bytes()), nowMs)
	require.NoError(t, err)

	got, found, err := GetRedisBytes(destSrv.DB, redisNamespaceForDB(0, state.SlotCount), key, nowMs)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, value, got)
}

func TestClusterRebalanceJobPersistsAndResumesAfterRestart(t *testing.T) {
	tmpDir := t.TempDir()
	sourceDBPath := filepath.Join(tmpDir, "resume-source.db")
	destDBPath := filepath.Join(tmpDir, "resume-dest.db")
	sourceCfg := newTestClusterConfig(t, "a", sourceDBPath)
	destCfg := newTestClusterConfig(t, "b", destDBPath)

	sourceSrv := openTestClusterServer(t, sourceCfg)
	destSrv := openTestClusterServer(t, destCfg)

	destHTTP := httptest.NewServer(destSrv.buildRouter())
	defer destHTTP.Close()

	state := buildClusterStateForTests(sourceSrv.Cluster.Snapshot().SlotCount, "http://127.0.0.1:17000", destHTTP.URL)
	require.NoError(t, sourceSrv.Cluster.ReplaceState(state))
	require.NoError(t, destSrv.Cluster.ReplaceState(state))

	slot := -1
	keys := make([][]byte, 0, 5)
	for i := 0; len(keys) < 5 && i < 100000; i++ {
		key := []byte("resume:key:" + strconv.Itoa(i))
		currentSlot := ClusterKeySlot(key, state.SlotCount)
		if state.SlotOwners[currentSlot] != "a" {
			continue
		}
		if slot == -1 {
			slot = currentSlot
		}
		if currentSlot == slot {
			keys = append(keys, key)
		}
	}
	require.Len(t, keys, 5)
	nowMs := redisNowUnixMilli(time.Now())
	ns := redisNamespaceForDB(0, state.SlotCount)
	for i, key := range keys {
		require.NoError(t, PutRedisBytes(sourceSrv.DB, ns, key, []byte("value-"+strconv.Itoa(i)), nowMs))
	}

	seedStats, err := sourceSrv.postClusterSlotImportChunk(clusterNodeState{
		ID:           "b",
		RedisAddress: "127.0.0.1:7001",
		HTTPAddress:  destHTTP.URL,
	}, "resume-job", 1, state.SlotCount, slot, slot, nowMs, redisDatabaseMin, nil, 2)
	require.NoError(t, err)
	require.Equal(t, uint64(2), seedStats.EntriesTransferred)

	resumeState := state.Clone()
	resumeState.Rebalancing = true
	resumeState.PendingMove = &clusterPendingMove{
		SourceNodeID:      "a",
		DestinationNodeID: "b",
		StartSlot:         slot,
		EndSlot:           slot,
		Stage:             clusterMoveStageCopying,
	}
	require.NoError(t, sourceSrv.Cluster.ReplaceState(resumeState))

	job := &clusterRebalanceJob{
		ID:                 "resume-job",
		Status:             clusterRebalanceStatusRunning,
		Phase:              clusterRebalancePhaseCopying,
		Move:               *resumeState.PendingMove,
		ChunkEntryLimit:    2,
		SnapshotWatermark:  2,
		LastSentChunk:      1,
		LastCommittedChunk: 1,
		KeysTransferred:    2,
		BytesTransferred:   seedStats.BytesTransferred,
		CopyCursorDB:       seedStats.NextCursorDB,
		CopyCursorKey:      cloneBytes(seedStats.NextCursorKey),
		CleanupCursorDB:    redisDatabaseMin,
	}
	require.NoError(t, sourceSrv.persistRebalanceJob(job))
	require.NoError(t, sourceSrv.DB.Close())

	restartedSource := openTestClusterServer(t, sourceCfg)
	require.NoError(t, restartedSource.Cluster.ReplaceState(resumeState))
	restartedSource.resumePersistedRebalanceJobs()

	require.Eventually(t, func() bool {
		restartedSource.rebalanceMu.Lock()
		resumeJob := restartedSource.rebalanceJobs["resume-job"]
		restartedSource.rebalanceMu.Unlock()
		if resumeJob == nil {
			return false
		}
		snapshot := resumeJob.snapshot()
		return snapshot.Status == clusterRebalanceStatusDone
	}, 10*time.Second, 100*time.Millisecond)

	for i, key := range keys {
		value, found, err := GetRedisBytes(destSrv.DB, ns, key, nowMs)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "value-"+strconv.Itoa(i), string(value))
	}
}

func TestClusterOnlineRebalanceAppliesWriteDeltaAfterBaseCopy(t *testing.T) {
	tmpDir := t.TempDir()
	sourceSrv := openTestClusterServer(t, newTestClusterConfig(t, "a", filepath.Join(tmpDir, "online-source.db")))
	destSrv := openTestClusterServer(t, newTestClusterConfig(t, "b", filepath.Join(tmpDir, "online-dest.db")))

	sourceHTTP := httptest.NewServer(sourceSrv.buildRouter())
	defer sourceHTTP.Close()
	destHTTP := httptest.NewServer(destSrv.buildRouter())
	defer destHTTP.Close()

	state := buildClusterStateForTests(sourceSrv.Cluster.Snapshot().SlotCount, sourceHTTP.URL, destHTTP.URL)
	require.NoError(t, sourceSrv.Cluster.ReplaceState(state))
	require.NoError(t, destSrv.Cluster.ReplaceState(state))
	sourceSrv.RedisServer = NewRedisServer(sourceSrv.Config, sourceSrv.DB, sourceSrv.Logger)
	sourceSrv.RedisServer.Cluster = sourceSrv.Cluster

	tag := findHashTagForOwner(state, "a")
	require.NotEmpty(t, tag)
	slotToMove := ClusterKeySlot([]byte("{"+tag+"}"), state.SlotCount)
	ns := redisNamespaceForDB(0, state.SlotCount)
	nowMs := redisNowUnixMilli(time.Now())

	keys := make([][]byte, 0, clusterImportBatchEntryLimit+32)
	for i := 0; i < clusterImportBatchEntryLimit+32; i++ {
		key := []byte("online:{" + tag + "}:" + strconv.Itoa(i))
		keys = append(keys, key)
		require.NoError(t, PutRedisBytes(sourceSrv.DB, ns, key, []byte("base-"+strconv.Itoa(i)), nowMs))
	}
	targetKey := keys[0]

	job := &clusterRebalanceJob{
		ID:     "online-rebalance",
		Status: clusterRebalanceStatusQueued,
		Phase:  clusterRebalancePhaseCopying,
		Move: clusterPendingMove{
			ID:                "online-rebalance",
			SourceNodeID:      "a",
			DestinationNodeID: "b",
			StartSlot:         slotToMove,
			EndSlot:           slotToMove,
		},
		ChunkEntryLimit: 1,
		CopyCursorDB:    redisDatabaseMin,
		CleanupCursorDB: redisDatabaseMin,
	}

	go sourceSrv.runClusterRebalanceJob(job)

	require.Eventually(t, func() bool {
		snapshot := job.snapshot()
		return snapshot.Phase == clusterRebalancePhaseCopying && snapshot.LastCommittedChunk >= 1
	}, 10*time.Second, 25*time.Millisecond)

	reply, err := sourceSrv.RedisServer.executeCommand(0, [][]byte{[]byte("SET"), targetKey, []byte("delta-updated")}, nil, redisNowUnixMilli(time.Now()), false)
	require.NoError(t, err)
	require.Equal(t, "OK", reply.(redisSimpleStringReply).value)

	require.Eventually(t, func() bool {
		snapshot := job.snapshot()
		return snapshot.Status == clusterRebalanceStatusDone
	}, 15*time.Second, 50*time.Millisecond)

	finalSnapshot := job.snapshot()
	require.Greater(t, finalSnapshot.DeltaMutationsApplied, uint64(0))
	require.GreaterOrEqual(t, finalSnapshot.LastDeltaAppliedSeq, uint64(1))

	value, found, err := GetRedisBytes(destSrv.DB, ns, targetKey, redisNowUnixMilli(time.Now()))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "delta-updated", string(value))

	value, found, err = GetRedisBytes(sourceSrv.DB, ns, targetKey, redisNowUnixMilli(time.Now()))
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, value)
}

func TestClusterRebalanceLargeSlotRangeUsesChunkedCopyAndCleanup(t *testing.T) {
	tmpDir := t.TempDir()
	sourceSrv := openTestClusterServer(t, newTestClusterConfig(t, "a", filepath.Join(tmpDir, "large-source.db")))
	destSrv := openTestClusterServer(t, newTestClusterConfig(t, "b", filepath.Join(tmpDir, "large-dest.db")))

	sourceHTTP := httptest.NewServer(sourceSrv.buildRouter())
	defer sourceHTTP.Close()
	destHTTP := httptest.NewServer(destSrv.buildRouter())
	defer destHTTP.Close()

	state := buildClusterStateForTests(sourceSrv.Cluster.Snapshot().SlotCount, sourceHTTP.URL, destHTTP.URL)
	require.NoError(t, sourceSrv.Cluster.ReplaceState(state))
	require.NoError(t, destSrv.Cluster.ReplaceState(state))

	tag := findHashTagForOwner(state, "a")
	require.NotEmpty(t, tag)
	slotToMove := ClusterKeySlot([]byte("{"+tag+"}"), state.SlotCount)
	nowMs := redisNowUnixMilli(time.Now())
	ns := redisNamespaceForDB(0, state.SlotCount)
	const keyCount = clusterImportBatchEntryLimit + 17
	for i := 0; i < keyCount; i++ {
		key := []byte("batch:{" + tag + "}:" + strconv.Itoa(i))
		require.NoError(t, PutRedisBytes(sourceSrv.DB, ns, key, []byte("value-"+strconv.Itoa(i)), nowMs))
	}

	reqBody, err := json.Marshal(clusterRebalanceRequest{
		SourceNodeID:      "a",
		DestinationNodeID: "b",
		StartSlot:         slotToMove,
		EndSlot:           slotToMove,
	})
	require.NoError(t, err)

	resp, err := http.Post(sourceHTTP.URL+"/api/v1/cluster/rebalance/execute", "application/json", bytes.NewReader(reqBody))
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	var jobResp clusterRebalanceJobResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&jobResp))
	_ = resp.Body.Close()

	var finalStatus clusterRebalanceJobResponse
	require.Eventually(t, func() bool {
		jobStatusResp, getErr := http.Get(sourceHTTP.URL + "/api/v1/cluster/rebalance/" + jobResp.JobID)
		if getErr != nil {
			return false
		}
		defer func() { _ = jobStatusResp.Body.Close() }()
		if decodeErr := json.NewDecoder(jobStatusResp.Body).Decode(&finalStatus); decodeErr != nil {
			return false
		}
		return finalStatus.Status == clusterRebalanceStatusDone
	}, 10*time.Second, 100*time.Millisecond)

	require.Greater(t, finalStatus.LastCommittedChunk, uint64(1))
	require.Equal(t, uint64(keyCount), finalStatus.KeysTransferred)
	require.Equal(t, uint64(keyCount), finalStatus.CleanupDeletedKeys)

	for i := 0; i < keyCount; i++ {
		key := []byte("batch:{" + tag + "}:" + strconv.Itoa(i))
		value, found, err := GetRedisBytes(destSrv.DB, ns, key, redisNowUnixMilli(time.Now()))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "value-"+strconv.Itoa(i), string(value))

		value, found, err = GetRedisBytes(sourceSrv.DB, ns, key, redisNowUnixMilli(time.Now()))
		require.NoError(t, err)
		require.False(t, found)
		require.Nil(t, value)
	}
}

func TestClusterJoinNodePropagatesState(t *testing.T) {
	tmpDir := t.TempDir()
	sourceSrv := openTestClusterServer(t, newTestClusterConfig(t, "a", filepath.Join(tmpDir, "join-source.db")))
	peerSrv := openTestClusterServer(t, newTestClusterConfig(t, "b", filepath.Join(tmpDir, "join-peer.db")))
	newNodeSrv := openTestClusterServer(t, newTestClusterConfigWithNodes(t, "c", filepath.Join(tmpDir, "join-new.db"), newTestClusterNodesABC()))

	sourceHTTP := httptest.NewServer(sourceSrv.buildRouter())
	defer sourceHTTP.Close()
	peerHTTP := httptest.NewServer(peerSrv.buildRouter())
	defer peerHTTP.Close()
	newNodeHTTP := httptest.NewServer(newNodeSrv.buildRouter())
	defer newNodeHTTP.Close()

	initialState := buildClusterStateForTests(sourceSrv.Cluster.Snapshot().SlotCount, sourceHTTP.URL, peerHTTP.URL)
	require.NoError(t, sourceSrv.Cluster.ReplaceState(initialState))
	require.NoError(t, peerSrv.Cluster.ReplaceState(initialState))

	reqBody, err := json.Marshal(clusterJoinNodeRequest{
		ID:           "c",
		RedisAddress: "127.0.0.1:7002",
		HTTPAddress:  newNodeHTTP.URL,
	})
	require.NoError(t, err)

	resp, err := http.Post(sourceHTTP.URL+"/api/v1/cluster/nodes/join", "application/json", bytes.NewReader(reqBody))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var status clusterStatusResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&status))
	_ = resp.Body.Close()
	require.Len(t, status.Nodes, 3)

	sourceState := sourceSrv.Cluster.Snapshot()
	peerState := peerSrv.Cluster.Snapshot()
	newNodeState := newNodeSrv.Cluster.Snapshot()
	require.Len(t, sourceState.Nodes, 3)
	require.Len(t, peerState.Nodes, 3)
	require.Len(t, newNodeState.Nodes, 3)
	_, found := sourceState.NodeByID("c")
	require.True(t, found)
	_, found = peerState.NodeByID("c")
	require.True(t, found)
	_, found = newNodeState.NodeByID("c")
	require.True(t, found)

	for _, owner := range sourceState.SlotOwners {
		require.NotEqual(t, "c", owner)
	}

	sourceSrv.RedisServer = NewRedisServer(sourceSrv.Config, sourceSrv.DB, sourceSrv.Logger)
	sourceSrv.RedisServer.Cluster = sourceSrv.Cluster
	reply, err := sourceSrv.RedisServer.executeCommand(0, [][]byte{[]byte("CLUSTER"), []byte("SHARDS")}, nil, redisNowUnixMilli(time.Now()), false)
	require.NoError(t, err)
	shards := reply.(redisArrayReply)
	require.Len(t, shards.values, 3)
}

func TestClusterReconcileTopologyAddsNodeWithoutResettingSlots(t *testing.T) {
	tmpDir := t.TempDir()
	topologyAB := newTestClusterTopologyAB()
	topologyABC := newTestClusterTopologyABC()

	sourceSrv := openTestClusterServer(t, newTestClusterConfigWithTopology(t, "a", filepath.Join(tmpDir, "reconcile-source.db"), topologyAB))
	peerSrv := openTestClusterServer(t, newTestClusterConfigWithTopology(t, "b", filepath.Join(tmpDir, "reconcile-peer.db"), topologyAB))
	newNodeSrv := openTestClusterServer(t, newTestClusterConfigWithTopology(t, "c", filepath.Join(tmpDir, "reconcile-new.db"), topologyABC))

	sourceHTTP := httptest.NewServer(sourceSrv.buildRouter())
	defer sourceHTTP.Close()
	peerHTTP := httptest.NewServer(peerSrv.buildRouter())
	defer peerHTTP.Close()
	newNodeHTTP := httptest.NewServer(newNodeSrv.buildRouter())
	defer newNodeHTTP.Close()

	initialState := buildClusterStateForTests(sourceSrv.Cluster.Snapshot().SlotCount, sourceHTTP.URL, peerHTTP.URL)
	require.NoError(t, sourceSrv.Cluster.ReplaceState(initialState))
	require.NoError(t, peerSrv.Cluster.ReplaceState(initialState))

	sourceSrv.Cluster.Topology = cloneClusterTopology(topologyABC)
	peerSrv.Cluster.Topology = cloneClusterTopology(topologyABC)
	newNodeSrv.Cluster.Topology = cloneClusterTopology(topologyABC)
	sourceSrv.Cluster.Topology.Nodes[0].HTTPAddress = sourceHTTP.URL
	sourceSrv.Cluster.Topology.Nodes[1].HTTPAddress = peerHTTP.URL
	sourceSrv.Cluster.Topology.Nodes[2].HTTPAddress = newNodeHTTP.URL
	peerSrv.Cluster.Topology.Nodes[0].HTTPAddress = sourceHTTP.URL
	peerSrv.Cluster.Topology.Nodes[1].HTTPAddress = peerHTTP.URL
	peerSrv.Cluster.Topology.Nodes[2].HTTPAddress = newNodeHTTP.URL
	newNodeSrv.Cluster.Topology.Nodes[0].HTTPAddress = sourceHTTP.URL
	newNodeSrv.Cluster.Topology.Nodes[1].HTTPAddress = peerHTTP.URL
	newNodeSrv.Cluster.Topology.Nodes[2].HTTPAddress = newNodeHTTP.URL

	resp, err := http.Post(sourceHTTP.URL+"/api/v1/cluster/reconcile-topology", "application/json", bytes.NewReader(nil))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var status clusterStatusResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&status))
	_ = resp.Body.Close()
	require.Len(t, status.Nodes, 3)

	sourceState := sourceSrv.Cluster.Snapshot()
	peerState := peerSrv.Cluster.Snapshot()
	newState := newNodeSrv.Cluster.Snapshot()
	require.Len(t, sourceState.Nodes, 3)
	require.Len(t, peerState.Nodes, 3)
	require.Len(t, newState.Nodes, 3)
	require.Equal(t, initialState.SlotOwners, sourceState.SlotOwners)
	require.Equal(t, initialState.SlotOwners, peerState.SlotOwners)
	require.Equal(t, initialState.SlotOwners, newState.SlotOwners)
	_, found := sourceState.NodeByID("c")
	require.True(t, found)
	_, found = peerState.NodeByID("c")
	require.True(t, found)
	_, found = newState.NodeByID("c")
	require.True(t, found)

	topologyResp, err := http.Get(sourceHTTP.URL + "/api/v1/cluster/topology")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, topologyResp.StatusCode)
	var topologyStatus clusterTopologyResponse
	require.NoError(t, json.NewDecoder(topologyResp.Body).Decode(&topologyStatus))
	_ = topologyResp.Body.Close()
	require.NotNil(t, topologyStatus.Topology)
	require.Equal(t, "test-cluster", topologyStatus.Topology.Name)
	require.Equal(t, sourceState.TopologyHash, topologyStatus.RuntimeHash)
}

func TestRedisClusterAskingRoute(t *testing.T) {
	tmpDir := t.TempDir()
	sourceCfg := newTestClusterConfig(t, "a", filepath.Join(tmpDir, "asking-source.db"))
	destCfg := newTestClusterConfig(t, "b", filepath.Join(tmpDir, "asking-dest.db"))

	sourceServer := openTestClusterServer(t, sourceCfg)
	destServer := openTestClusterServer(t, destCfg)
	sourceServer.RedisServer = NewRedisServer(sourceServer.Config, sourceServer.DB, sourceServer.Logger)
	sourceServer.RedisServer.Cluster = sourceServer.Cluster
	destServer.RedisServer = NewRedisServer(destServer.Config, destServer.DB, destServer.Logger)
	destServer.RedisServer.Cluster = destServer.Cluster

	state := buildClusterStateForTests(sourceCfg.Cluster.SlotCount, "http://127.0.0.1:17000", "http://127.0.0.1:17001")
	keyToMove := findKeyForOwner(state, "a")
	require.NotNil(t, keyToMove)
	slotToMove := ClusterKeySlot(keyToMove, state.SlotCount)
	state.Rebalancing = true
	state.PendingMove = &clusterPendingMove{
		SourceNodeID:      "a",
		DestinationNodeID: "b",
		StartSlot:         slotToMove,
		EndSlot:           slotToMove,
		Stage:             clusterMoveStageAsking,
	}
	require.NoError(t, sourceServer.Cluster.ReplaceState(state))
	require.NoError(t, destServer.Cluster.ReplaceState(state))

	nowMs := redisNowUnixMilli(time.Now())
	require.NoError(t, PutRedisBytes(destServer.DB, redisNamespaceForDB(0, state.SlotCount), keyToMove, []byte("migrated"), nowMs))

	_, err := sourceServer.RedisServer.executeCommand(0, [][]byte{[]byte("GET"), keyToMove}, nil, nowMs, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ASK")
	require.Contains(t, err.Error(), "127.0.0.1:7001")

	_, err = destServer.RedisServer.executeCommand(0, [][]byte{[]byte("GET"), keyToMove}, nil, nowMs, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "MOVED")
	require.Contains(t, err.Error(), "127.0.0.1:7000")

	reply, err := destServer.RedisServer.executeCommand(0, [][]byte{[]byte("GET"), keyToMove}, nil, nowMs, true)
	require.NoError(t, err)
	require.Equal(t, "migrated", string(reply.(redisBulkReply).value))
}

func TestClusterJoinNodeThenRebalanceToNewNode(t *testing.T) {
	tmpDir := t.TempDir()
	sourceSrv := openTestClusterServer(t, newTestClusterConfig(t, "a", filepath.Join(tmpDir, "rebalance-source.db")))
	peerSrv := openTestClusterServer(t, newTestClusterConfig(t, "b", filepath.Join(tmpDir, "rebalance-peer.db")))
	newNodeSrv := openTestClusterServer(t, newTestClusterConfigWithNodes(t, "c", filepath.Join(tmpDir, "rebalance-new.db"), newTestClusterNodesABC()))

	sourceHTTP := httptest.NewServer(sourceSrv.buildRouter())
	defer sourceHTTP.Close()
	peerHTTP := httptest.NewServer(peerSrv.buildRouter())
	defer peerHTTP.Close()
	newNodeHTTP := httptest.NewServer(newNodeSrv.buildRouter())
	defer newNodeHTTP.Close()

	initialState := buildClusterStateForTests(sourceSrv.Cluster.Snapshot().SlotCount, sourceHTTP.URL, peerHTTP.URL)
	require.NoError(t, sourceSrv.Cluster.ReplaceState(initialState))
	require.NoError(t, peerSrv.Cluster.ReplaceState(initialState))

	joinBody, err := json.Marshal(clusterJoinNodeRequest{
		ID:           "c",
		RedisAddress: "127.0.0.1:7002",
		HTTPAddress:  newNodeHTTP.URL,
	})
	require.NoError(t, err)

	joinResp, err := http.Post(sourceHTTP.URL+"/api/v1/cluster/nodes/join", "application/json", bytes.NewReader(joinBody))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, joinResp.StatusCode)
	_ = joinResp.Body.Close()

	joinedState := sourceSrv.Cluster.Snapshot()
	require.NotNil(t, joinedState)
	require.Len(t, joinedState.Nodes, 3)
	sourceSrv.RedisServer = NewRedisServer(sourceSrv.Config, sourceSrv.DB, sourceSrv.Logger)
	sourceSrv.RedisServer.Cluster = sourceSrv.Cluster
	peerSrv.RedisServer = NewRedisServer(peerSrv.Config, peerSrv.DB, peerSrv.Logger)
	peerSrv.RedisServer.Cluster = peerSrv.Cluster
	newNodeSrv.RedisServer = NewRedisServer(newNodeSrv.Config, newNodeSrv.DB, newNodeSrv.Logger)
	newNodeSrv.RedisServer.Cluster = newNodeSrv.Cluster

	keyToMove := findKeyForOwner(joinedState, "a")
	require.NotNil(t, keyToMove)
	slotToMove := ClusterKeySlot(keyToMove, joinedState.SlotCount)
	nowMs := redisNowUnixMilli(time.Now())
	require.NoError(t, PutRedisBytes(sourceSrv.DB, redisNamespaceForDB(0, joinedState.SlotCount), keyToMove, []byte("moved-to-new-node"), nowMs))

	reqBody, err := json.Marshal(clusterRebalanceRequest{
		SourceNodeID:      "a",
		DestinationNodeID: "c",
		StartSlot:         slotToMove,
		EndSlot:           slotToMove,
	})
	require.NoError(t, err)

	resp, err := http.Post(sourceHTTP.URL+"/api/v1/cluster/rebalance/execute", "application/json", bytes.NewReader(reqBody))
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	var jobResp clusterRebalanceJobResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&jobResp))
	_ = resp.Body.Close()

	require.Eventually(t, func() bool {
		jobStatusResp, getErr := http.Get(sourceHTTP.URL + "/api/v1/cluster/rebalance/" + jobResp.JobID)
		if getErr != nil {
			return false
		}
		defer func() { _ = jobStatusResp.Body.Close() }()
		var status clusterRebalanceJobResponse
		if decodeErr := json.NewDecoder(jobStatusResp.Body).Decode(&status); decodeErr != nil {
			return false
		}
		return status.Status == "done"
	}, 10*time.Second, 100*time.Millisecond)

	value, found, err := GetRedisBytes(newNodeSrv.DB, redisNamespaceForDB(0, joinedState.SlotCount), keyToMove, redisNowUnixMilli(time.Now()))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "moved-to-new-node", string(value))

	value, found, err = GetRedisBytes(sourceSrv.DB, redisNamespaceForDB(0, joinedState.SlotCount), keyToMove, redisNowUnixMilli(time.Now()))
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, value)

	sourceState := sourceSrv.Cluster.Snapshot()
	peerState := peerSrv.Cluster.Snapshot()
	newState := newNodeSrv.Cluster.Snapshot()
	require.Equal(t, "c", sourceState.SlotOwners[slotToMove])
	require.Equal(t, "c", peerState.SlotOwners[slotToMove])
	require.Equal(t, "c", newState.SlotOwners[slotToMove])
}

func TestClusterRebalanceMovesRedisKeysAcrossShards(t *testing.T) {
	tmpDir := t.TempDir()
	sourceSrv := openTestClusterServer(t, newTestClusterConfig(t, "a", filepath.Join(tmpDir, "source.db")))
	destSrv := openTestClusterServer(t, newTestClusterConfig(t, "b", filepath.Join(tmpDir, "dest.db")))

	sourceHTTP := httptest.NewServer(sourceSrv.buildRouter())
	defer sourceHTTP.Close()
	destHTTP := httptest.NewServer(destSrv.buildRouter())
	defer destHTTP.Close()

	state := buildClusterStateForTests(sourceSrv.Cluster.Snapshot().SlotCount, sourceHTTP.URL, destHTTP.URL)
	require.NoError(t, sourceSrv.Cluster.ReplaceState(state))
	require.NoError(t, destSrv.Cluster.ReplaceState(state))
	sourceSrv.RedisServer = NewRedisServer(sourceSrv.Config, sourceSrv.DB, sourceSrv.Logger)
	sourceSrv.RedisServer.Cluster = sourceSrv.Cluster
	destSrv.RedisServer = NewRedisServer(destSrv.Config, destSrv.DB, destSrv.Logger)
	destSrv.RedisServer.Cluster = destSrv.Cluster

	keyToMove := findKeyForOwner(state, "a")
	require.NotNil(t, keyToMove)
	slotToMove := ClusterKeySlot(keyToMove, state.SlotCount)

	nowMs := redisNowUnixMilli(time.Now())
	require.NoError(t, PutRedisBytes(sourceSrv.DB, redisNamespaceForDB(0, state.SlotCount), keyToMove, []byte("moved-value"), nowMs))

	reqBody, err := json.Marshal(clusterRebalanceRequest{
		SourceNodeID:      "a",
		DestinationNodeID: "b",
		StartSlot:         slotToMove,
		EndSlot:           slotToMove,
	})
	require.NoError(t, err)

	resp, err := http.Post(sourceHTTP.URL+"/api/v1/cluster/rebalance/execute", "application/json", bytes.NewReader(reqBody))
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	var jobResp clusterRebalanceJobResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&jobResp))
	_ = resp.Body.Close()

	require.Eventually(t, func() bool {
		jobStatusResp, getErr := http.Get(sourceHTTP.URL + "/api/v1/cluster/rebalance/" + jobResp.JobID)
		if getErr != nil {
			return false
		}
		defer func() { _ = jobStatusResp.Body.Close() }()
		var status clusterRebalanceJobResponse
		if decodeErr := json.NewDecoder(jobStatusResp.Body).Decode(&status); decodeErr != nil {
			return false
		}
		return status.Status == "done"
	}, 10*time.Second, 100*time.Millisecond)

	value, found, err := GetRedisBytes(destSrv.DB, redisNamespaceForDB(0, state.SlotCount), keyToMove, redisNowUnixMilli(time.Now()))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "moved-value", string(value))

	value, found, err = GetRedisBytes(sourceSrv.DB, redisNamespaceForDB(0, state.SlotCount), keyToMove, redisNowUnixMilli(time.Now()))
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, value)

	sourceState := sourceSrv.Cluster.Snapshot()
	destState := destSrv.Cluster.Snapshot()
	require.Equal(t, "b", sourceState.SlotOwners[slotToMove])
	require.Equal(t, "b", destState.SlotOwners[slotToMove])

	_, routeErr := sourceSrv.RedisServer.executeCommand(0, [][]byte{[]byte("GET"), keyToMove}, nil, redisNowUnixMilli(time.Now()), false)
	require.Error(t, routeErr)
	require.True(t, strings.Contains(routeErr.Error(), "MOVED"))
}
