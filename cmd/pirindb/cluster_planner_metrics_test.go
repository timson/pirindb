package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClusterDrainPlannerUsesClusterWideBytesAndMoveLimits(t *testing.T) {
	state := &clusterState{
		ClusterID: "planner-cluster",
		Version:   1,
		Epoch:     7,
		SlotCount: 128,
		Nodes: []clusterNodeState{
			{ID: "a", RedisAddress: "a:6379", HTTPAddress: "http://a:4321"},
			{ID: "b", RedisAddress: "b:6379", HTTPAddress: "http://b:4321"},
			{ID: "c", RedisAddress: "c:6379", HTTPAddress: "http://c:4321"},
		},
		SlotOwners: make([]string, 128),
	}
	for slot := range state.SlotOwners {
		switch {
		case slot < 60:
			state.SlotOwners[slot] = "a"
		case slot < 100:
			state.SlotOwners[slot] = "b"
		default:
			state.SlotOwners[slot] = "c"
		}
	}
	collectedAt := time.Now().UTC()
	metrics := clusterPlanningMetrics{
		CollectedAt:    collectedAt,
		SlotKeyCounts:  make([]int64, state.SlotCount),
		SlotByteCounts: make([]int64, state.SlotCount),
	}
	for slot, owner := range state.SlotOwners {
		metrics.SlotKeyCounts[slot] = 1
		switch owner {
		case "a":
			metrics.SlotByteCounts[slot] = 10
		case "b":
			metrics.SlotByteCounts[slot] = 1
		case "c":
			metrics.SlotByteCounts[slot] = 1000
		}
	}

	plan, err := buildClusterDrainNodePlanWithMetrics(state, "a", clusterDrainNodeRequest{
		NodeID:   "a",
		MaxMoves: 1,
	}, metrics)
	require.NoError(t, err)
	require.Equal(t, "estimated_logical_bytes", plan.WeightMetric)
	require.Equal(t, collectedAt, *plan.MetricsCollectedAt)
	require.Len(t, plan.Moves, 1)
	// Slot-only planning would prefer c (36 missing slots versus b's 24).
	// Byte-aware planning correctly chooses the much lighter b shard first.
	require.Equal(t, "b", plan.Moves[0].DestinationNodeID)
	require.Positive(t, plan.Moves[0].EstimatedBytes)

	limitedPlan, err := buildClusterDrainNodePlanWithMetrics(state, "a", clusterDrainNodeRequest{
		NodeID:          "a",
		MaxMoves:        1,
		MaxBytesPerMove: 100,
	}, metrics)
	require.NoError(t, err)
	require.Len(t, limitedPlan.Moves, 1)
	require.LessOrEqual(t, limitedPlan.Moves[0].EstimatedBytes, int64(100))

	metrics.SlotByteCounts[59] = 101
	_, err = buildClusterDrainNodePlanWithMetrics(state, "a", clusterDrainNodeRequest{
		NodeID:          "a",
		MaxMoves:        1,
		MaxBytesPerMove: 100,
	}, metrics)
	require.ErrorContains(t, err, "exceed max_bytes_per_move")
}

func TestClusterBytePlannerNeverOvershootsSlotDeficitOrDonorExcess(t *testing.T) {
	const slotCount = 16384
	state := &clusterState{
		SchemaVersion: clusterStateSchemaVersion,
		ClusterID:     "four-node-planner",
		Version:       2,
		Epoch:         2,
		SlotCount:     slotCount,
		Nodes: []clusterNodeState{
			{ID: "node-0", RedisAddress: "node-0:6379", HTTPAddress: "http://node-0:4321"},
			{ID: "node-1", RedisAddress: "node-1:6379", HTTPAddress: "http://node-1:4321"},
			{ID: "node-2", RedisAddress: "node-2:6379", HTTPAddress: "http://node-2:4321"},
			{ID: "node-3", RedisAddress: "node-3:6379", HTTPAddress: "http://node-3:4321"},
		},
		SlotOwners: make([]string, slotCount),
	}
	for slot := range state.SlotOwners {
		switch {
		case slot < 5462:
			state.SlotOwners[slot] = "node-0"
		case slot < 10923:
			state.SlotOwners[slot] = "node-1"
		default:
			state.SlotOwners[slot] = "node-2"
		}
	}
	metrics := clusterPlanningMetrics{
		CollectedAt:    time.Now().UTC(),
		SlotKeyCounts:  make([]int64, slotCount),
		SlotByteCounts: make([]int64, slotCount),
	}
	metrics.SlotKeyCounts[123] = 2
	metrics.SlotByteCounts[123] = 3324
	metrics.SlotKeyCounts[6000] = 1
	metrics.SlotByteCounts[6000] = 15
	metrics.SlotKeyCounts[12000] = 2
	metrics.SlotByteCounts[12000] = 669

	plan, err := buildClusterRebalancePlanForNodeWithMetrics(nil, state, "node-0", clusterRebalancePlanForNodeRequest{
		DestinationNodeID: "node-3",
	}, metrics, time.Now().UnixMilli())
	require.NoError(t, err)
	require.NotEmpty(t, plan.Moves)
	require.LessOrEqual(t, plan.PlannedSlots, plan.DeficitSlots)
	plannedBySource := make(map[string]int)
	for _, move := range plan.Moves {
		plannedBySource[move.SourceNodeID] += move.SlotCount
	}
	current := clusterNodeSlotCounts(state)
	target := clusterTargetSlotCounts(state)
	for source, planned := range plannedBySource {
		require.LessOrEqual(t, planned, current[source]-target[source])
	}
}

func TestClusterBytePlannerUsesSlotGuardrailWhenByteAndSlotTargetsConflict(t *testing.T) {
	const slotCount = 16384
	state := &clusterState{
		SchemaVersion: clusterStateSchemaVersion,
		ClusterID:     "four-node-guardrail",
		Version:       3,
		Epoch:         6,
		SlotCount:     slotCount,
		Nodes: []clusterNodeState{
			{ID: "node-0", RedisAddress: "node-0:6379", HTTPAddress: "http://node-0:4321"},
			{ID: "node-1", RedisAddress: "node-1:6379", HTTPAddress: "http://node-1:4321"},
			{ID: "node-2", RedisAddress: "node-2:6379", HTTPAddress: "http://node-2:4321"},
			{ID: "node-3", RedisAddress: "node-3:6379", HTTPAddress: "http://node-3:4321"},
		},
		SlotOwners: make([]string, slotCount),
	}
	for slot := range state.SlotOwners {
		switch {
		case slot < 4096:
			state.SlotOwners[slot] = "node-0"
		case slot < 5462:
			state.SlotOwners[slot] = "node-3"
		case slot < 10923:
			state.SlotOwners[slot] = "node-1"
		default:
			state.SlotOwners[slot] = "node-2"
		}
	}
	metrics := clusterPlanningMetrics{
		CollectedAt:    time.Now().UTC(),
		SlotKeyCounts:  make([]int64, slotCount),
		SlotByteCounts: make([]int64, slotCount),
	}
	// node-0 owns most bytes but has no slot excess. The two slot-excess
	// donors are below their byte targets, so exact byte and slot balance
	// cannot both be achieved by another weighted move.
	metrics.SlotKeyCounts[123] = 2
	metrics.SlotByteCounts[123] = 3324
	metrics.SlotKeyCounts[6000] = 1
	metrics.SlotByteCounts[6000] = 15
	metrics.SlotKeyCounts[12000] = 2
	metrics.SlotByteCounts[12000] = 669

	plan, err := buildClusterRebalancePlanForNodeWithMetrics(nil, state, "node-0", clusterRebalancePlanForNodeRequest{
		DestinationNodeID: "node-3",
		MaxMoves:          1,
	}, metrics, time.Now().UnixMilli())
	require.NoError(t, err)
	require.Equal(t, "slot_count_guardrail", plan.WeightMetric)
	require.Len(t, plan.Moves, 1)
	require.LessOrEqual(t, plan.Moves[0].SlotCount, plan.DeficitSlots)
	require.Contains(t, []string{"node-1", "node-2"}, plan.Moves[0].SourceNodeID)
	require.LessOrEqual(t, plan.Moves[0].SlotCount, 1365)
	require.NotNil(t, plan.MetricsCollectedAt)
}

func TestClusterOrchestrationPersistenceKeepsPlanningLimits(t *testing.T) {
	scaleRequest := &clusterRebalancePlanForNodeRequest{
		DestinationNodeID: "c",
		MaxMoves:          3,
		MaxSlotsPerSource: 17,
		MaxBytesPerMove:   64 * 1024 * 1024,
	}
	drainRequest := &clusterDrainNodeRequest{
		NodeID:                 "d",
		MaxMoves:               2,
		MaxSlotsPerDestination: 11,
		MaxBytesPerMove:        32 * 1024 * 1024,
	}
	job := &clusterAutoRebalanceJob{
		SchemaVersion: clusterOrchestrationSchemaVersion,
		ID:            "limits-job",
		Kind:          "scale-out",
		Status:        clusterRebalanceStatusQueued,
		ScaleRequest:  scaleRequest,
		DrainRequest:  drainRequest,
	}
	raw, err := encodeClusterOrchestrationJob(job)
	require.NoError(t, err)
	decoded, err := decodeClusterOrchestrationJob(raw)
	require.NoError(t, err)
	require.Equal(t, scaleRequest, decoded.ScaleRequest)
	require.Equal(t, drainRequest, decoded.DrainRequest)
}

func TestClusterOrchestrationRejectsStaleEpochWithMachineCode(t *testing.T) {
	srv := openTestClusterServer(t, newTestClusterConfig(t, "a", filepath.Join(t.TempDir(), "stale-plan.db")))
	state := srv.Cluster.Snapshot()
	require.NotNil(t, state)
	slot := -1
	for index, owner := range state.SlotOwners {
		if owner == "a" {
			slot = index
			break
		}
	}
	require.NotEqual(t, -1, slot)
	source, found := state.NodeByID("a")
	require.True(t, found)
	destination, found := state.NodeByID("b")
	require.True(t, found)
	job := &clusterAutoRebalanceJob{
		SchemaVersion: clusterOrchestrationSchemaVersion,
		ID:            "stale-parent",
		RequestedNode: "b",
		ClusterID:     state.ClusterID,
		PlanEpoch:     state.Epoch + 1,
		Kind:          "scale-out",
		Status:        clusterRebalanceStatusQueued,
		Steps: []clusterAutoRebalanceStep{{Move: clusterRebalancePlanMoveResponse{
			SourceNode:        source,
			DestinationNode:   destination,
			SourceNodeID:      source.ID,
			DestinationNodeID: destination.ID,
			StartSlot:         slot,
			EndSlot:           slot,
			SlotCount:         1,
		}}},
	}

	srv.runClusterAutoRebalanceJob(context.Background(), job)
	snapshot := job.snapshot()
	require.Equal(t, clusterRebalanceStatusFailed, snapshot.Status)
	require.Equal(t, "stale_plan", snapshot.ErrorCode)
	require.Contains(t, snapshot.Error, "stale plan epoch")
	require.Equal(t, "a", srv.Cluster.Snapshot().SlotOwners[slot])
}
