package main

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/timson/pirindb/storage"
)

type clusterRebalancePlanForNodeRequest struct {
	DestinationNodeID      string `json:"destination_node_id"`
	MaxMoves               int    `json:"max_moves,omitempty"`
	MaxSlotsPerSource      int    `json:"max_slots_per_source,omitempty"`
	MaxBytesPerMove        int64  `json:"max_bytes_per_move,omitempty"`
	MaxParallelMoves       int    `json:"max_parallel_moves,omitempty"`
	AllowSlotCountFallback bool   `json:"allow_slot_count_fallback,omitempty"`
}

type clusterRebalancePlanMoveResponse struct {
	SourceNode        clusterNodeState `json:"source_node"`
	DestinationNode   clusterNodeState `json:"destination_node"`
	SourceNodeID      string           `json:"source_node_id"`
	DestinationNodeID string           `json:"destination_node_id"`
	StartSlot         int              `json:"start_slot"`
	EndSlot           int              `json:"end_slot"`
	SlotCount         int              `json:"slot_count"`
	EstimatedKeyCount int64            `json:"estimated_key_count"`
	EstimatedBytes    int64            `json:"estimated_bytes"`
	SubmitOnSource    bool             `json:"submit_on_source"`
}

type clusterRebalancePlanForNodeResponse struct {
	LocalNodeID           string                             `json:"local_node_id"`
	Version               uint64                             `json:"version"`
	Epoch                 uint64                             `json:"epoch"`
	SlotCount             int                                `json:"slot_count"`
	WeightMetric          string                             `json:"weight_metric"`
	DestinationNode       clusterNodeState                   `json:"destination_node"`
	CurrentOwnedSlots     int                                `json:"current_owned_slots"`
	TargetOwnedSlots      int                                `json:"target_owned_slots"`
	DeficitSlots          int                                `json:"deficit_slots"`
	PlannedSlots          int                                `json:"planned_slots"`
	RemainingDeficitSlots int                                `json:"remaining_deficit_slots"`
	CurrentOwnedKeys      int64                              `json:"current_owned_keys"`
	TargetOwnedKeys       int64                              `json:"target_owned_keys"`
	DeficitKeys           int64                              `json:"deficit_keys"`
	PlannedKeys           int64                              `json:"planned_keys"`
	RemainingDeficitKeys  int64                              `json:"remaining_deficit_keys"`
	CurrentOwnedBytes     int64                              `json:"current_owned_bytes"`
	TargetOwnedBytes      int64                              `json:"target_owned_bytes"`
	DeficitBytes          int64                              `json:"deficit_bytes"`
	PlannedBytes          int64                              `json:"planned_bytes"`
	RemainingDeficitBytes int64                              `json:"remaining_deficit_bytes"`
	NodeEstimatedBytes    map[string]int64                   `json:"node_estimated_bytes,omitempty"`
	TargetEstimatedBytes  map[string]int64                   `json:"target_estimated_bytes,omitempty"`
	MetricsCollectedAt    *time.Time                         `json:"metrics_collected_at,omitempty"`
	Complete              bool                               `json:"complete"`
	NodeSlotCounts        map[string]int                     `json:"node_slot_counts"`
	TargetSlotCounts      map[string]int                     `json:"target_slot_counts"`
	NodeKeyCounts         map[string]int64                   `json:"node_key_counts,omitempty"`
	TargetKeyCounts       map[string]int64                   `json:"target_key_counts,omitempty"`
	Moves                 []clusterRebalancePlanMoveResponse `json:"moves"`
}

type clusterDrainNodeRequest struct {
	NodeID                 string `json:"node_id"`
	MaxMoves               int    `json:"max_moves,omitempty"`
	MaxSlotsPerDestination int    `json:"max_slots_per_destination,omitempty"`
	MaxBytesPerMove        int64  `json:"max_bytes_per_move,omitempty"`
	MaxParallelMoves       int    `json:"max_parallel_moves,omitempty"`
	AllowSlotCountFallback bool   `json:"allow_slot_count_fallback,omitempty"`
}

type clusterDrainNodeResponse struct {
	LocalNodeID          string                             `json:"local_node_id"`
	Version              uint64                             `json:"version"`
	Epoch                uint64                             `json:"epoch"`
	SlotCount            int                                `json:"slot_count"`
	WeightMetric         string                             `json:"weight_metric"`
	SourceNode           clusterNodeState                   `json:"source_node"`
	CurrentOwnedSlots    int                                `json:"current_owned_slots"`
	PlannedSlots         int                                `json:"planned_slots"`
	RemainingOwnedSlots  int                                `json:"remaining_owned_slots"`
	CurrentOwnedBytes    int64                              `json:"current_owned_bytes"`
	PlannedBytes         int64                              `json:"planned_bytes"`
	RemainingOwnedBytes  int64                              `json:"remaining_owned_bytes"`
	Complete             bool                               `json:"complete"`
	NodeSlotCounts       map[string]int                     `json:"node_slot_counts"`
	TargetSlotCounts     map[string]int                     `json:"target_slot_counts"`
	NodeEstimatedBytes   map[string]int64                   `json:"node_estimated_bytes,omitempty"`
	TargetEstimatedBytes map[string]int64                   `json:"target_estimated_bytes,omitempty"`
	MetricsCollectedAt   *time.Time                         `json:"metrics_collected_at,omitempty"`
	Moves                []clusterRebalancePlanMoveResponse `json:"moves"`
}

type clusterAutoRebalanceJob struct {
	mu             sync.Mutex
	SchemaVersion  int
	ID             string
	IdempotencyKey string
	RequestedNode  string
	ClusterID      string
	PlanEpoch      uint64
	CurrentStep    int
	Kind           string
	Status         string
	Error          string
	ErrorCode      string
	Plan           clusterRebalancePlanForNodeResponse
	ScaleRequest   *clusterRebalancePlanForNodeRequest
	DrainRequest   *clusterDrainNodeRequest
	StartedAt      *time.Time
	UpdatedAt      *time.Time
	FinishedAt     *time.Time
	Steps          []clusterAutoRebalanceStep
}

type clusterAutoRebalanceStep struct {
	Move            clusterRebalancePlanMoveResponse `json:"move"`
	RebalanceJobID  string                           `json:"rebalance_job_id,omitempty"`
	RebalanceStatus string                           `json:"rebalance_status,omitempty"`
	Error           string                           `json:"error,omitempty"`
}

type clusterAutoRebalanceJobResponse struct {
	SchemaVersion  int                                 `json:"schema_version"`
	JobID          string                              `json:"job_id"`
	IdempotencyKey string                              `json:"idempotency_key,omitempty"`
	Kind           string                              `json:"kind,omitempty"`
	RequestedNode  string                              `json:"requested_node_id,omitempty"`
	ClusterID      string                              `json:"cluster_id,omitempty"`
	PlanEpoch      uint64                              `json:"plan_epoch"`
	CurrentStep    int                                 `json:"current_step"`
	Status         string                              `json:"status"`
	Error          string                              `json:"error,omitempty"`
	ErrorCode      string                              `json:"error_code,omitempty"`
	Plan           clusterRebalancePlanForNodeResponse `json:"plan"`
	ScaleRequest   *clusterRebalancePlanForNodeRequest `json:"scale_request,omitempty"`
	DrainRequest   *clusterDrainNodeRequest            `json:"drain_request,omitempty"`
	Steps          []clusterAutoRebalanceStep          `json:"steps"`
	StartedAt      *time.Time                          `json:"started_at,omitempty"`
	UpdatedAt      *time.Time                          `json:"updated_at,omitempty"`
	FinishedAt     *time.Time                          `json:"finished_at,omitempty"`
}

func (job *clusterAutoRebalanceJob) snapshot() clusterAutoRebalanceJobResponse {
	job.mu.Lock()
	defer job.mu.Unlock()
	steps := make([]clusterAutoRebalanceStep, 0, len(job.Steps))
	steps = append(steps, job.Steps...)
	var scaleRequest *clusterRebalancePlanForNodeRequest
	if job.ScaleRequest != nil {
		cloned := *job.ScaleRequest
		scaleRequest = &cloned
	}
	var drainRequest *clusterDrainNodeRequest
	if job.DrainRequest != nil {
		cloned := *job.DrainRequest
		drainRequest = &cloned
	}
	return clusterAutoRebalanceJobResponse{
		SchemaVersion:  job.SchemaVersion,
		JobID:          job.ID,
		IdempotencyKey: job.IdempotencyKey,
		Kind:           job.Kind,
		RequestedNode:  job.RequestedNode,
		ClusterID:      job.ClusterID,
		PlanEpoch:      job.PlanEpoch,
		CurrentStep:    job.CurrentStep,
		Status:         job.Status,
		Error:          job.Error,
		ErrorCode:      job.ErrorCode,
		Plan:           job.Plan,
		ScaleRequest:   scaleRequest,
		DrainRequest:   drainRequest,
		Steps:          steps,
		StartedAt:      cloneTimePointer(job.StartedAt),
		UpdatedAt:      cloneTimePointer(job.UpdatedAt),
		FinishedAt:     cloneTimePointer(job.FinishedAt),
	}
}

func cloneTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func (job *clusterAutoRebalanceJob) active() bool {
	if job == nil {
		return false
	}
	job.mu.Lock()
	defer job.mu.Unlock()
	return job.Status == clusterRebalanceStatusQueued || job.Status == clusterRebalanceStatusRunning
}

func (job *clusterAutoRebalanceJob) markRunning() {
	job.mu.Lock()
	defer job.mu.Unlock()
	now := time.Now().UTC()
	job.Status = clusterRebalanceStatusRunning
	job.Error = ""
	job.ErrorCode = ""
	job.UpdatedAt = &now
	if job.StartedAt == nil {
		job.StartedAt = &now
	}
}

func (job *clusterAutoRebalanceJob) markDone() {
	job.mu.Lock()
	defer job.mu.Unlock()
	now := time.Now().UTC()
	job.Status = clusterRebalanceStatusDone
	job.CurrentStep = len(job.Steps)
	job.UpdatedAt = &now
	job.FinishedAt = &now
}

func (job *clusterAutoRebalanceJob) markFailed(err error) {
	job.markFailedWithCode("orchestration_failed", err)
}

func (job *clusterAutoRebalanceJob) markFailedWithCode(code string, err error) {
	job.mu.Lock()
	defer job.mu.Unlock()
	now := time.Now().UTC()
	job.Status = clusterRebalanceStatusFailed
	job.Error = err.Error()
	job.ErrorCode = code
	job.UpdatedAt = &now
	job.FinishedAt = &now
}

func (job *clusterAutoRebalanceJob) updateStep(index int, fn func(step *clusterAutoRebalanceStep)) {
	job.mu.Lock()
	defer job.mu.Unlock()
	if index < 0 || index >= len(job.Steps) {
		return
	}
	fn(&job.Steps[index])
	now := time.Now().UTC()
	job.UpdatedAt = &now
}

func (job *clusterAutoRebalanceJob) advanceStep(next int) {
	job.mu.Lock()
	defer job.mu.Unlock()
	job.CurrentStep = next
	now := time.Now().UTC()
	job.UpdatedAt = &now
}

func decodeClusterRebalancePlanForNodeRequest(r *http.Request) (clusterRebalancePlanForNodeRequest, error) {
	var req clusterRebalancePlanForNodeRequest
	if err := decodeStrictClusterJSON(r, &req); err != nil {
		return clusterRebalancePlanForNodeRequest{}, err
	}
	req.DestinationNodeID = strings.TrimSpace(req.DestinationNodeID)
	return req, nil
}

func validateClusterRebalancePlanForNodeRequest(state *clusterState, req clusterRebalancePlanForNodeRequest) error {
	if state == nil {
		return errors.New("cluster state is unavailable")
	}
	if state.Rebalancing {
		return errors.New("cluster is already rebalancing")
	}
	if req.DestinationNodeID == "" {
		return errors.New("destination_node_id is required")
	}
	if _, ok := state.NodeByID(req.DestinationNodeID); !ok {
		return errors.New("unknown destination node")
	}
	if req.MaxMoves < 0 {
		return errors.New("max_moves must be non-negative")
	}
	if req.MaxSlotsPerSource < 0 {
		return errors.New("max_slots_per_source must be non-negative")
	}
	if req.MaxBytesPerMove < 0 {
		return errors.New("max_bytes_per_move must be non-negative")
	}
	if req.MaxParallelMoves < 0 {
		return errors.New("max_parallel_moves must be non-negative")
	}
	if req.MaxParallelMoves > 1 {
		return errors.New("max_parallel_moves greater than 1 is not supported yet")
	}
	return nil
}

func buildClusterRebalancePlanForNodeWithMetrics(db *storage.DB, state *clusterState, localNodeID string, req clusterRebalancePlanForNodeRequest, metrics clusterPlanningMetrics, nowMs int64) (clusterRebalancePlanForNodeResponse, error) {
	if err := validateClusterRebalancePlanForNodeRequest(state, req); err != nil {
		return clusterRebalancePlanForNodeResponse{}, err
	}
	if len(metrics.SlotKeyCounts) != state.SlotCount || len(metrics.SlotByteCounts) != state.SlotCount {
		return clusterRebalancePlanForNodeResponse{}, errors.New("cluster-wide planning metrics are incomplete")
	}
	totalBytes := int64(0)
	totalKeys := int64(0)
	nodeBytes := make(map[string]int64, len(state.Nodes))
	nodeKeys := make(map[string]int64, len(state.Nodes))
	for _, node := range state.Nodes {
		nodeBytes[node.ID] = 0
		nodeKeys[node.ID] = 0
	}
	for slot, owner := range state.SlotOwners {
		if metrics.SlotByteCounts[slot] < 0 || metrics.SlotKeyCounts[slot] < 0 {
			return clusterRebalancePlanForNodeResponse{}, errors.New("cluster planning metrics contain a negative weight")
		}
		nodeBytes[owner] += metrics.SlotByteCounts[slot]
		nodeKeys[owner] += metrics.SlotKeyCounts[slot]
		totalBytes += metrics.SlotByteCounts[slot]
		totalKeys += metrics.SlotKeyCounts[slot]
	}
	if totalBytes == 0 {
		return buildClusterSlotCountOnlyPlan(state, localNodeID, req)
	}

	destination, _ := state.NodeByID(req.DestinationNodeID)
	currentSlots := clusterNodeSlotCounts(state)
	targetSlots := clusterTargetSlotCounts(state)
	targetBytes := clusterTargetInt64Counts(state.Nodes, totalBytes)
	targetKeys := clusterTargetKeyCounts(state, totalKeys)
	deficitBytes := targetBytes[destination.ID] - nodeBytes[destination.ID]
	if deficitBytes < 0 {
		deficitBytes = 0
	}
	deficitSlots := targetSlots[destination.ID] - currentSlots[destination.ID]
	if deficitSlots < 0 {
		deficitSlots = 0
	}
	collectedAt := metrics.CollectedAt.UTC()
	plan := clusterRebalancePlanForNodeResponse{
		LocalNodeID:           localNodeID,
		Version:               state.Version,
		Epoch:                 state.Epoch,
		SlotCount:             state.SlotCount,
		WeightMetric:          "estimated_logical_bytes",
		DestinationNode:       destination,
		CurrentOwnedSlots:     currentSlots[destination.ID],
		TargetOwnedSlots:      targetSlots[destination.ID],
		DeficitSlots:          deficitSlots,
		RemainingDeficitSlots: deficitSlots,
		CurrentOwnedKeys:      nodeKeys[destination.ID],
		TargetOwnedKeys:       targetKeys[destination.ID],
		DeficitKeys:           max(int64(0), targetKeys[destination.ID]-nodeKeys[destination.ID]),
		RemainingDeficitKeys:  max(int64(0), targetKeys[destination.ID]-nodeKeys[destination.ID]),
		CurrentOwnedBytes:     nodeBytes[destination.ID],
		TargetOwnedBytes:      targetBytes[destination.ID],
		DeficitBytes:          deficitBytes,
		RemainingDeficitBytes: deficitBytes,
		NodeSlotCounts:        currentSlots,
		TargetSlotCounts:      targetSlots,
		NodeKeyCounts:         nodeKeys,
		TargetKeyCounts:       targetKeys,
		NodeEstimatedBytes:    nodeBytes,
		TargetEstimatedBytes:  targetBytes,
		MetricsCollectedAt:    &collectedAt,
		Moves:                 make([]clusterRebalancePlanMoveResponse, 0),
	}
	if deficitBytes == 0 {
		// Bytes are already balanced; retain the slot-count guardrail.
		return buildClusterSlotCountGuardrailPlan(state, localNodeID, req, metrics, nodeBytes, targetBytes, nodeKeys, targetKeys)
	}

	rangesByNode := make(map[string][]clusterSlotRange, len(state.Nodes))
	for _, slotRange := range clusterStateSlotRanges(state) {
		rangesByNode[slotRange.Node.ID] = append(rangesByNode[slotRange.Node.ID], slotRange)
	}
	remaining := deficitBytes
	remainingSlots := deficitSlots
	for _, node := range state.Nodes {
		if node.ID == destination.ID || remaining == 0 || remainingSlots == 0 {
			continue
		}
		excess := nodeBytes[node.ID] - targetBytes[node.ID]
		sourceSlotExcess := currentSlots[node.ID] - targetSlots[node.ID]
		if excess <= 0 || sourceSlotExcess <= 0 {
			continue
		}
		toMove := min(excess, remaining)
		slotsMoved := 0
		nodeRanges := rangesByNode[node.ID]
		for index := len(nodeRanges) - 1; index >= 0 && toMove > 0 && remainingSlots > 0; index-- {
			allowedSlots := min(sourceSlotExcess-slotsMoved, remainingSlots)
			if req.MaxSlotsPerSource > 0 {
				allowedSlots = min(allowedSlots, req.MaxSlotsPerSource-slotsMoved)
			}
			if allowedSlots <= 0 {
				break
			}
			move, movedBytes, movedKeys, ok, err := takeWeightedByteTailRange(
				nodeRanges[index], metrics.SlotByteCounts, metrics.SlotKeyCounts, toMove,
				req.MaxBytesPerMove, allowedSlots, 0,
			)
			if err != nil {
				return clusterRebalancePlanForNodeResponse{}, err
			}
			if !ok {
				continue
			}
			move.SourceNode = node
			move.DestinationNode = destination
			move.SourceNodeID = node.ID
			move.DestinationNodeID = destination.ID
			move.EstimatedBytes = movedBytes
			move.EstimatedKeyCount = movedKeys
			move.SubmitOnSource = node.ID == localNodeID
			plan.Moves = append(plan.Moves, move)
			plan.PlannedBytes += movedBytes
			plan.PlannedKeys += movedKeys
			plan.PlannedSlots += move.SlotCount
			remaining -= movedBytes
			if remaining < 0 {
				remaining = 0
			}
			toMove -= movedBytes
			slotsMoved += move.SlotCount
			remainingSlots -= move.SlotCount
			if req.MaxMoves > 0 && len(plan.Moves) >= req.MaxMoves {
				break
			}
		}
		if req.MaxMoves > 0 && len(plan.Moves) >= req.MaxMoves {
			break
		}
	}
	plan.RemainingDeficitBytes = remaining
	plan.RemainingDeficitSlots = remainingSlots
	plan.RemainingDeficitKeys = max(int64(0), plan.DeficitKeys-plan.PlannedKeys)
	plan.Complete = remaining == 0 && plan.RemainingDeficitSlots == 0
	if len(plan.Moves) == 0 {
		// Sparse or highly skewed data can leave every slot-excess donor at or
		// below its byte target. Exact byte and slot balance are then mutually
		// exclusive. Finish the required slot balance with an explicitly labelled
		// guardrail plan while retaining the cluster-wide byte estimates.
		return buildClusterSlotCountGuardrailPlan(state, localNodeID, req, metrics, nodeBytes, targetBytes, nodeKeys, targetKeys)
	}
	return plan, nil
}

func buildClusterSlotCountGuardrailPlan(
	state *clusterState,
	localNodeID string,
	req clusterRebalancePlanForNodeRequest,
	metrics clusterPlanningMetrics,
	nodeBytes map[string]int64,
	targetBytes map[string]int64,
	nodeKeys map[string]int64,
	targetKeys map[string]int64,
) (clusterRebalancePlanForNodeResponse, error) {
	plan, err := buildClusterSlotCountOnlyPlan(state, localNodeID, req)
	if err != nil {
		return clusterRebalancePlanForNodeResponse{}, err
	}
	plan.WeightMetric = "slot_count_guardrail"
	plan.NodeEstimatedBytes = nodeBytes
	plan.TargetEstimatedBytes = targetBytes
	plan.NodeKeyCounts = nodeKeys
	plan.TargetKeyCounts = targetKeys
	collectedAt := metrics.CollectedAt.UTC()
	plan.MetricsCollectedAt = &collectedAt
	plan.CurrentOwnedBytes = nodeBytes[req.DestinationNodeID]
	plan.TargetOwnedBytes = targetBytes[req.DestinationNodeID]
	plan.DeficitBytes = max(int64(0), plan.TargetOwnedBytes-plan.CurrentOwnedBytes)
	plan.CurrentOwnedKeys = nodeKeys[req.DestinationNodeID]
	plan.TargetOwnedKeys = targetKeys[req.DestinationNodeID]
	plan.DeficitKeys = max(int64(0), plan.TargetOwnedKeys-plan.CurrentOwnedKeys)

	for index := range plan.Moves {
		move := &plan.Moves[index]
		for slot := move.StartSlot; slot <= move.EndSlot; slot++ {
			move.EstimatedBytes += metrics.SlotByteCounts[slot]
			move.EstimatedKeyCount += metrics.SlotKeyCounts[slot]
		}
		plan.PlannedBytes += move.EstimatedBytes
		plan.PlannedKeys += move.EstimatedKeyCount
	}
	plan.RemainingDeficitBytes = max(int64(0), plan.DeficitBytes-plan.PlannedBytes)
	plan.RemainingDeficitKeys = max(int64(0), plan.DeficitKeys-plan.PlannedKeys)
	return plan, nil
}

func buildClusterSlotCountOnlyPlan(state *clusterState, localNodeID string, req clusterRebalancePlanForNodeRequest) (clusterRebalancePlanForNodeResponse, error) {
	if err := validateClusterRebalancePlanForNodeRequest(state, req); err != nil {
		return clusterRebalancePlanForNodeResponse{}, err
	}
	destination, _ := state.NodeByID(req.DestinationNodeID)
	currentCounts := clusterNodeSlotCounts(state)
	targetCounts := clusterTargetSlotCounts(state)
	deficit := max(0, targetCounts[destination.ID]-currentCounts[destination.ID])
	plan := clusterRebalancePlanForNodeResponse{
		LocalNodeID:           localNodeID,
		Version:               state.Version,
		Epoch:                 state.Epoch,
		SlotCount:             state.SlotCount,
		WeightMetric:          "slot_count",
		DestinationNode:       destination,
		CurrentOwnedSlots:     currentCounts[destination.ID],
		TargetOwnedSlots:      targetCounts[destination.ID],
		DeficitSlots:          deficit,
		RemainingDeficitSlots: deficit,
		Complete:              deficit == 0,
		NodeSlotCounts:        currentCounts,
		TargetSlotCounts:      targetCounts,
		Moves:                 make([]clusterRebalancePlanMoveResponse, 0),
	}
	if deficit == 0 {
		return plan, nil
	}
	rangesByNode := make(map[string][]clusterSlotRange, len(state.Nodes))
	for _, slotRange := range clusterStateSlotRanges(state) {
		rangesByNode[slotRange.Node.ID] = append(rangesByNode[slotRange.Node.ID], slotRange)
	}
	remaining := deficit
	for _, node := range state.Nodes {
		if node.ID == destination.ID || remaining == 0 {
			continue
		}
		excess := currentCounts[node.ID] - targetCounts[node.ID]
		if req.MaxSlotsPerSource > 0 {
			excess = min(excess, req.MaxSlotsPerSource)
		}
		for index := len(rangesByNode[node.ID]) - 1; index >= 0 && excess > 0 && remaining > 0; index-- {
			slotRange := rangesByNode[node.ID][index]
			take := min(slotRange.EndSlot-slotRange.StartSlot+1, min(excess, remaining))
			if take <= 0 {
				continue
			}
			plan.Moves = append(plan.Moves, clusterRebalancePlanMoveResponse{
				SourceNode: node, DestinationNode: destination,
				SourceNodeID: node.ID, DestinationNodeID: destination.ID,
				StartSlot: slotRange.EndSlot - take + 1, EndSlot: slotRange.EndSlot,
				SlotCount: take, SubmitOnSource: node.ID == localNodeID,
			})
			plan.PlannedSlots += take
			remaining -= take
			excess -= take
			if req.MaxMoves > 0 && len(plan.Moves) >= req.MaxMoves {
				break
			}
		}
		if req.MaxMoves > 0 && len(plan.Moves) >= req.MaxMoves {
			break
		}
	}
	plan.RemainingDeficitSlots = remaining
	plan.Complete = remaining == 0
	return plan, nil
}

func clusterTargetInt64Counts(nodes []clusterNodeState, total int64) map[string]int64 {
	targets := make(map[string]int64, len(nodes))
	if len(nodes) == 0 {
		return targets
	}
	base := total / int64(len(nodes))
	remainder := total % int64(len(nodes))
	for index, node := range nodes {
		targets[node.ID] = base
		if int64(index) < remainder {
			targets[node.ID]++
		}
	}
	return targets
}

func takeWeightedByteTailRange(slotRange clusterSlotRange, slotBytes []int64, slotKeys []int64, targetBytes int64, maxBytes int64, maxSlots int, slotsAlreadyMoved int) (clusterRebalancePlanMoveResponse, int64, int64, bool, error) {
	if targetBytes <= 0 || slotRange.EndSlot < slotRange.StartSlot {
		return clusterRebalancePlanMoveResponse{}, 0, 0, false, nil
	}
	allowedSlots := slotRange.EndSlot - slotRange.StartSlot + 1
	if maxSlots > 0 {
		allowedSlots = min(allowedSlots, maxSlots-slotsAlreadyMoved)
		if allowedSlots <= 0 {
			return clusterRebalancePlanMoveResponse{}, 0, 0, false, nil
		}
	}
	start := slotRange.EndSlot
	movedBytes := int64(0)
	movedKeys := int64(0)
	movedSlots := 0
	for slot := slotRange.EndSlot; slot >= slotRange.StartSlot && movedSlots < allowedSlots; slot-- {
		weight := slotBytes[slot]
		if maxBytes > 0 && weight > maxBytes {
			return clusterRebalancePlanMoveResponse{}, 0, 0, false, fmt.Errorf("slot %d estimated bytes %d exceed max_bytes_per_move %d", slot, weight, maxBytes)
		}
		if maxBytes > 0 && movedSlots > 0 && movedBytes+weight > maxBytes {
			break
		}
		movedBytes += weight
		movedKeys += slotKeys[slot]
		movedSlots++
		start = slot
		if movedBytes >= targetBytes {
			break
		}
	}
	if movedSlots == 0 {
		return clusterRebalancePlanMoveResponse{}, 0, 0, false, nil
	}
	return clusterRebalancePlanMoveResponse{StartSlot: start, EndSlot: slotRange.EndSlot, SlotCount: movedSlots}, movedBytes, movedKeys, true, nil
}

func decodeClusterDrainNodeRequest(r *http.Request) (clusterDrainNodeRequest, error) {
	var req clusterDrainNodeRequest
	if err := decodeStrictClusterJSON(r, &req); err != nil {
		return clusterDrainNodeRequest{}, err
	}
	req.NodeID = strings.TrimSpace(req.NodeID)
	return req, nil
}

func validateClusterDrainNodeRequest(state *clusterState, req clusterDrainNodeRequest) error {
	if state == nil {
		return errors.New("cluster state is unavailable")
	}
	if state.Rebalancing {
		return errors.New("cluster is already rebalancing")
	}
	if req.NodeID == "" {
		return errors.New("node_id is required")
	}
	if _, ok := state.NodeByID(req.NodeID); !ok {
		return errors.New("unknown node")
	}
	if len(state.Nodes) <= 1 {
		return errors.New("cannot drain the last cluster node")
	}
	if req.MaxMoves < 0 {
		return errors.New("max_moves must be non-negative")
	}
	if req.MaxSlotsPerDestination < 0 {
		return errors.New("max_slots_per_destination must be non-negative")
	}
	if req.MaxBytesPerMove < 0 {
		return errors.New("max_bytes_per_move must be non-negative")
	}
	if req.MaxParallelMoves < 0 {
		return errors.New("max_parallel_moves must be non-negative")
	}
	if req.MaxParallelMoves > 1 {
		return errors.New("max_parallel_moves greater than 1 is not supported yet")
	}
	return nil
}

func buildClusterRebalancePlanForNode(db *storage.DB, state *clusterState, localNodeID string, req clusterRebalancePlanForNodeRequest, nowMs int64) (clusterRebalancePlanForNodeResponse, error) {
	if err := validateClusterRebalancePlanForNodeRequest(state, req); err != nil {
		return clusterRebalancePlanForNodeResponse{}, err
	}
	destinationNode, _ := state.NodeByID(req.DestinationNodeID)
	currentCounts := clusterNodeSlotCounts(state)
	targetCounts := clusterTargetSlotCounts(state)
	currentOwned := currentCounts[req.DestinationNodeID]
	targetOwned := targetCounts[req.DestinationNodeID]
	deficit := targetOwned - currentOwned
	if deficit < 0 {
		deficit = 0
	}

	plan := clusterRebalancePlanForNodeResponse{
		LocalNodeID:           localNodeID,
		Version:               state.Version,
		Epoch:                 state.Epoch,
		SlotCount:             state.SlotCount,
		WeightMetric:          "slot_count",
		DestinationNode:       destinationNode,
		CurrentOwnedSlots:     currentOwned,
		TargetOwnedSlots:      targetOwned,
		DeficitSlots:          deficit,
		RemainingDeficitSlots: deficit,
		Complete:              deficit == 0,
		NodeSlotCounts:        currentCounts,
		TargetSlotCounts:      targetCounts,
		Moves:                 make([]clusterRebalancePlanMoveResponse, 0),
	}
	if deficit == 0 {
		return plan, nil
	}

	slotKeyCounts, nodeKeyCounts, totalKeys, err := estimateClusterKeyWeights(db, state, nowMs)
	if err != nil {
		return clusterRebalancePlanForNodeResponse{}, err
	}
	if totalKeys > 0 {
		targetKeyCounts := clusterTargetKeyCounts(state, totalKeys)
		currentOwnedKeys := nodeKeyCounts[req.DestinationNodeID]
		targetOwnedKeys := targetKeyCounts[req.DestinationNodeID]
		deficitKeys := targetOwnedKeys - currentOwnedKeys
		if deficitKeys < 0 {
			deficitKeys = 0
		}
		plan.WeightMetric = "visible_key_count"
		plan.NodeKeyCounts = nodeKeyCounts
		plan.TargetKeyCounts = targetKeyCounts
		plan.CurrentOwnedKeys = currentOwnedKeys
		plan.TargetOwnedKeys = targetOwnedKeys
		plan.DeficitKeys = deficitKeys
		plan.RemainingDeficitKeys = deficitKeys

		weightedMoves, plannedKeys, remainingKeys := buildWeightedDestinationMoves(state, localNodeID, req, slotKeyCounts, nodeKeyCounts, targetKeyCounts)
		if len(weightedMoves) > 0 {
			plan.Moves = weightedMoves
			plan.PlannedSlots = 0
			for _, move := range weightedMoves {
				plan.PlannedSlots += move.SlotCount
			}
			plan.PlannedKeys = plannedKeys
			plan.RemainingDeficitKeys = remainingKeys
			plan.Complete = remainingKeys == 0
			return plan, nil
		}
	}

	ranges := clusterStateSlotRanges(state)
	rangesByNode := make(map[string][]clusterSlotRange, len(state.Nodes))
	for _, slotRange := range ranges {
		rangesByNode[slotRange.Node.ID] = append(rangesByNode[slotRange.Node.ID], slotRange)
	}

	remaining := deficit
	for _, node := range state.Nodes {
		if node.ID == req.DestinationNodeID || remaining == 0 {
			continue
		}
		excess := currentCounts[node.ID] - targetCounts[node.ID]
		if excess <= 0 {
			continue
		}
		if req.MaxSlotsPerSource > 0 && excess > req.MaxSlotsPerSource {
			excess = req.MaxSlotsPerSource
		}
		toMove := excess
		if toMove > remaining {
			toMove = remaining
		}
		nodeRanges := rangesByNode[node.ID]
		for i := len(nodeRanges) - 1; i >= 0 && toMove > 0 && remaining > 0; i-- {
			slotRange := nodeRanges[i]
			rangeSlots := slotRange.EndSlot - slotRange.StartSlot + 1
			if rangeSlots <= 0 {
				continue
			}
			take := rangeSlots
			if take > toMove {
				take = toMove
			}
			move := clusterRebalancePlanMoveResponse{
				SourceNode:        node,
				DestinationNode:   destinationNode,
				SourceNodeID:      node.ID,
				DestinationNodeID: destinationNode.ID,
				StartSlot:         slotRange.EndSlot - take + 1,
				EndSlot:           slotRange.EndSlot,
				SlotCount:         take,
				EstimatedKeyCount: 0,
				SubmitOnSource:    node.ID == localNodeID,
			}
			plan.Moves = append(plan.Moves, move)
			plan.PlannedSlots += take
			remaining -= take
			toMove -= take
			plan.RemainingDeficitSlots = remaining
			if req.MaxMoves > 0 && len(plan.Moves) >= req.MaxMoves {
				plan.Complete = remaining == 0
				return plan, nil
			}
		}
	}
	plan.Complete = remaining == 0
	return plan, nil
}

func buildClusterDrainNodePlan(state *clusterState, localNodeID string, req clusterDrainNodeRequest) (clusterDrainNodeResponse, error) {
	if err := validateClusterDrainNodeRequest(state, req); err != nil {
		return clusterDrainNodeResponse{}, err
	}
	sourceNode, _ := state.NodeByID(req.NodeID)
	currentCounts := clusterNodeSlotCounts(state)
	targetCounts := clusterTargetSlotCountsExcludingNode(state, req.NodeID)

	remaining := currentCounts[req.NodeID]
	plan := clusterDrainNodeResponse{
		LocalNodeID:         localNodeID,
		Version:             state.Version,
		Epoch:               state.Epoch,
		SlotCount:           state.SlotCount,
		WeightMetric:        "slot_count",
		SourceNode:          sourceNode,
		CurrentOwnedSlots:   currentCounts[req.NodeID],
		RemainingOwnedSlots: remaining,
		Complete:            remaining == 0,
		NodeSlotCounts:      currentCounts,
		TargetSlotCounts:    targetCounts,
		Moves:               make([]clusterRebalancePlanMoveResponse, 0),
	}
	if remaining == 0 {
		return plan, nil
	}

	ranges := clusterStateSlotRanges(state)
	sourceRanges := make([]clusterSlotRange, 0)
	for _, slotRange := range ranges {
		if slotRange.Node.ID == req.NodeID {
			sourceRanges = append(sourceRanges, slotRange)
		}
	}
	type recipientDeficit struct {
		node    clusterNodeState
		deficit int
	}
	recipients := make([]recipientDeficit, 0, len(state.Nodes)-1)
	for _, node := range state.Nodes {
		if node.ID == req.NodeID {
			continue
		}
		deficit := targetCounts[node.ID] - currentCounts[node.ID]
		if deficit < 0 {
			deficit = 0
		}
		recipients = append(recipients, recipientDeficit{node: node, deficit: deficit})
	}
	sort.Slice(recipients, func(i, j int) bool {
		if recipients[i].deficit == recipients[j].deficit {
			return recipients[i].node.ID < recipients[j].node.ID
		}
		return recipients[i].deficit > recipients[j].deficit
	})
	rangeIndex := len(sourceRanges) - 1
	for _, recipient := range recipients {
		toMove := recipient.deficit
		if req.MaxSlotsPerDestination > 0 && toMove > req.MaxSlotsPerDestination {
			toMove = req.MaxSlotsPerDestination
		}
		if toMove <= 0 {
			continue
		}
		for rangeIndex >= 0 && toMove > 0 && remaining > 0 {
			slotRange := sourceRanges[rangeIndex]
			rangeSlots := slotRange.EndSlot - slotRange.StartSlot + 1
			take := rangeSlots
			if take > toMove {
				take = toMove
			}
			move := clusterRebalancePlanMoveResponse{
				SourceNode:        sourceNode,
				DestinationNode:   recipient.node,
				SourceNodeID:      sourceNode.ID,
				DestinationNodeID: recipient.node.ID,
				StartSlot:         slotRange.EndSlot - take + 1,
				EndSlot:           slotRange.EndSlot,
				SlotCount:         take,
				EstimatedKeyCount: 0,
				SubmitOnSource:    sourceNode.ID == localNodeID,
			}
			plan.Moves = append(plan.Moves, move)
			plan.PlannedSlots += take
			remaining -= take
			toMove -= take
			if take == rangeSlots {
				rangeIndex--
			} else {
				sourceRanges[rangeIndex].EndSlot -= take
			}
			if req.MaxMoves > 0 && len(plan.Moves) >= req.MaxMoves {
				plan.RemainingOwnedSlots = remaining
				plan.Complete = remaining == 0
				return plan, nil
			}
		}
	}
	for rangeIndex >= 0 && remaining > 0 {
		slotRange := sourceRanges[rangeIndex]
		for _, recipient := range recipients {
			if remaining == 0 {
				break
			}
			rangeSlots := slotRange.EndSlot - slotRange.StartSlot + 1
			if rangeSlots <= 0 {
				break
			}
			take := rangeSlots
			if req.MaxSlotsPerDestination > 0 && take > req.MaxSlotsPerDestination {
				take = req.MaxSlotsPerDestination
			}
			move := clusterRebalancePlanMoveResponse{
				SourceNode:        sourceNode,
				DestinationNode:   recipient.node,
				SourceNodeID:      sourceNode.ID,
				DestinationNodeID: recipient.node.ID,
				StartSlot:         slotRange.EndSlot - take + 1,
				EndSlot:           slotRange.EndSlot,
				SlotCount:         take,
				EstimatedKeyCount: 0,
				SubmitOnSource:    sourceNode.ID == localNodeID,
			}
			plan.Moves = append(plan.Moves, move)
			plan.PlannedSlots += take
			remaining -= take
			if req.MaxMoves > 0 && len(plan.Moves) >= req.MaxMoves {
				plan.RemainingOwnedSlots = remaining
				plan.Complete = remaining == 0
				return plan, nil
			}
			break
		}
		rangeIndex--
	}
	plan.RemainingOwnedSlots = remaining
	plan.Complete = remaining == 0
	return plan, nil
}

func buildClusterDrainNodePlanWithMetrics(state *clusterState, localNodeID string, req clusterDrainNodeRequest, metrics clusterPlanningMetrics) (clusterDrainNodeResponse, error) {
	if err := validateClusterDrainNodeRequest(state, req); err != nil {
		return clusterDrainNodeResponse{}, err
	}
	if len(metrics.SlotKeyCounts) != state.SlotCount || len(metrics.SlotByteCounts) != state.SlotCount {
		return clusterDrainNodeResponse{}, errors.New("cluster-wide planning metrics are incomplete")
	}

	currentSlots := clusterNodeSlotCounts(state)
	targetSlots := clusterTargetSlotCountsExcludingNode(state, req.NodeID)
	currentBytes := make(map[string]int64, len(state.Nodes))
	targetBytes := make(map[string]int64, len(state.Nodes))
	remainingNodes := make([]clusterNodeState, 0, len(state.Nodes)-1)
	totalBytes := int64(0)
	for _, node := range state.Nodes {
		currentBytes[node.ID] = 0
		targetBytes[node.ID] = 0
		if node.ID != req.NodeID {
			remainingNodes = append(remainingNodes, node)
		}
	}
	for slot, owner := range state.SlotOwners {
		if metrics.SlotByteCounts[slot] < 0 || metrics.SlotKeyCounts[slot] < 0 {
			return clusterDrainNodeResponse{}, errors.New("cluster planning metrics contain a negative weight")
		}
		currentBytes[owner] += metrics.SlotByteCounts[slot]
		totalBytes += metrics.SlotByteCounts[slot]
	}
	for nodeID, target := range clusterTargetInt64Counts(remainingNodes, totalBytes) {
		targetBytes[nodeID] = target
	}
	collectedAt := metrics.CollectedAt.UTC()
	if totalBytes == 0 {
		plan, err := buildClusterDrainNodePlan(state, localNodeID, req)
		if err != nil {
			return clusterDrainNodeResponse{}, err
		}
		plan.WeightMetric = "slot_count_guardrail"
		plan.NodeEstimatedBytes = currentBytes
		plan.TargetEstimatedBytes = targetBytes
		plan.MetricsCollectedAt = &collectedAt
		return plan, nil
	}

	source, _ := state.NodeByID(req.NodeID)
	remainingSlots := currentSlots[req.NodeID]
	remainingBytes := currentBytes[req.NodeID]
	plan := clusterDrainNodeResponse{
		LocalNodeID:          localNodeID,
		Version:              state.Version,
		Epoch:                state.Epoch,
		SlotCount:            state.SlotCount,
		WeightMetric:         "estimated_logical_bytes",
		SourceNode:           source,
		CurrentOwnedSlots:    remainingSlots,
		RemainingOwnedSlots:  remainingSlots,
		CurrentOwnedBytes:    remainingBytes,
		RemainingOwnedBytes:  remainingBytes,
		Complete:             remainingSlots == 0,
		NodeSlotCounts:       clusterNodeSlotCounts(state),
		TargetSlotCounts:     targetSlots,
		NodeEstimatedBytes:   make(map[string]int64, len(currentBytes)),
		TargetEstimatedBytes: targetBytes,
		MetricsCollectedAt:   &collectedAt,
		Moves:                make([]clusterRebalancePlanMoveResponse, 0),
	}
	for nodeID, value := range currentBytes {
		plan.NodeEstimatedBytes[nodeID] = value
	}
	if remainingSlots == 0 {
		return plan, nil
	}

	sourceRanges := make([]clusterSlotRange, 0)
	for _, slotRange := range clusterStateSlotRanges(state) {
		if slotRange.Node.ID == req.NodeID {
			sourceRanges = append(sourceRanges, slotRange)
		}
	}
	movedSlotsByDestination := make(map[string]int, len(remainingNodes))
	for remainingSlots > 0 && len(sourceRanges) > 0 {
		type recipientCandidate struct {
			node        clusterNodeState
			byteDeficit int64
			slotDeficit int
		}
		candidates := make([]recipientCandidate, 0, len(remainingNodes))
		for _, node := range remainingNodes {
			slotDeficit := targetSlots[node.ID] - currentSlots[node.ID]
			if slotDeficit <= 0 {
				continue
			}
			if req.MaxSlotsPerDestination > 0 {
				slotDeficit = min(slotDeficit, req.MaxSlotsPerDestination-movedSlotsByDestination[node.ID])
				if slotDeficit <= 0 {
					continue
				}
			}
			candidates = append(candidates, recipientCandidate{
				node:        node,
				byteDeficit: targetBytes[node.ID] - currentBytes[node.ID],
				slotDeficit: slotDeficit,
			})
		}
		if len(candidates) == 0 {
			break
		}
		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].byteDeficit != candidates[j].byteDeficit {
				return candidates[i].byteDeficit > candidates[j].byteDeficit
			}
			if candidates[i].slotDeficit != candidates[j].slotDeficit {
				return candidates[i].slotDeficit > candidates[j].slotDeficit
			}
			return candidates[i].node.ID < candidates[j].node.ID
		})

		recipient := candidates[0]
		rangeIndex := len(sourceRanges) - 1
		sourceRange := sourceRanges[rangeIndex]
		allowedSlots := min(sourceRange.EndSlot-sourceRange.StartSlot+1, recipient.slotDeficit)
		targetMoveBytes := recipient.byteDeficit
		if targetMoveBytes <= 0 {
			targetMoveBytes = 1
		}
		if recipient.slotDeficit >= remainingSlots {
			// This recipient must take the remainder to satisfy the slot-count
			// guardrail, even if its byte target is reached earlier in the range.
			targetMoveBytes = math.MaxInt64
		}
		move, movedBytes, movedKeys, ok, err := takeWeightedByteTailRange(
			sourceRange,
			metrics.SlotByteCounts,
			metrics.SlotKeyCounts,
			targetMoveBytes,
			req.MaxBytesPerMove,
			allowedSlots,
			0,
		)
		if err != nil {
			return clusterDrainNodeResponse{}, err
		}
		if !ok {
			break
		}
		move.SourceNode = source
		move.DestinationNode = recipient.node
		move.SourceNodeID = source.ID
		move.DestinationNodeID = recipient.node.ID
		move.EstimatedBytes = movedBytes
		move.EstimatedKeyCount = movedKeys
		move.SubmitOnSource = source.ID == localNodeID
		plan.Moves = append(plan.Moves, move)
		plan.PlannedSlots += move.SlotCount
		plan.PlannedBytes += movedBytes
		remainingSlots -= move.SlotCount
		remainingBytes = max(int64(0), remainingBytes-movedBytes)
		currentSlots[source.ID] -= move.SlotCount
		currentSlots[recipient.node.ID] += move.SlotCount
		currentBytes[source.ID] -= movedBytes
		currentBytes[recipient.node.ID] += movedBytes
		movedSlotsByDestination[recipient.node.ID] += move.SlotCount
		if move.StartSlot == sourceRange.StartSlot {
			sourceRanges = sourceRanges[:rangeIndex]
		} else {
			sourceRanges[rangeIndex].EndSlot = move.StartSlot - 1
		}
		if req.MaxMoves > 0 && len(plan.Moves) >= req.MaxMoves {
			break
		}
	}
	plan.RemainingOwnedSlots = remainingSlots
	plan.RemainingOwnedBytes = remainingBytes
	plan.Complete = remainingSlots == 0
	if len(plan.Moves) == 0 && !plan.Complete {
		return clusterDrainNodeResponse{}, errors.New("cluster byte weights cannot produce a safe contiguous drain move within configured limits")
	}
	return plan, nil
}

func clusterNodeSlotCounts(state *clusterState) map[string]int {
	counts := make(map[string]int, len(state.Nodes))
	for _, node := range state.Nodes {
		counts[node.ID] = 0
	}
	for _, owner := range state.SlotOwners {
		counts[owner]++
	}
	return counts
}

func clusterTargetSlotCounts(state *clusterState) map[string]int {
	targets := make(map[string]int, len(state.Nodes))
	if state == nil || len(state.Nodes) == 0 {
		return targets
	}
	base := state.SlotCount / len(state.Nodes)
	remainder := state.SlotCount % len(state.Nodes)
	for idx, node := range state.Nodes {
		target := base
		if idx < remainder {
			target++
		}
		targets[node.ID] = target
	}
	return targets
}

func clusterTargetSlotCountsExcludingNode(state *clusterState, excludedNodeID string) map[string]int {
	targets := make(map[string]int)
	if state == nil {
		return targets
	}
	remainingNodes := make([]clusterNodeState, 0, len(state.Nodes))
	for _, node := range state.Nodes {
		if node.ID == excludedNodeID {
			continue
		}
		remainingNodes = append(remainingNodes, node)
	}
	if len(remainingNodes) == 0 {
		return targets
	}
	base := state.SlotCount / len(remainingNodes)
	remainder := state.SlotCount % len(remainingNodes)
	for idx, node := range remainingNodes {
		target := base
		if idx < remainder {
			target++
		}
		targets[node.ID] = target
	}
	targets[excludedNodeID] = 0
	return targets
}

func clusterTargetKeyCounts(state *clusterState, totalKeys int64) map[string]int64 {
	targets := make(map[string]int64, len(state.Nodes))
	if state == nil || len(state.Nodes) == 0 {
		return targets
	}
	base := totalKeys / int64(len(state.Nodes))
	remainder := totalKeys % int64(len(state.Nodes))
	for idx, node := range state.Nodes {
		target := base
		if int64(idx) < remainder {
			target++
		}
		targets[node.ID] = target
	}
	return targets
}

func estimateClusterKeyWeights(db *storage.DB, state *clusterState, nowMs int64) ([]int64, map[string]int64, int64, error) {
	slotKeyCounts := make([]int64, state.SlotCount)
	totalKeys := int64(0)
	err := db.View(func(tx *storage.Tx) error {
		for dbIndex := redisDatabaseMin; dbIndex <= redisDatabaseMax; dbIndex++ {
			ns := redisNamespaceForDB(dbIndex, state.SlotCount)
			bucket, err := tx.GetBucket(ns.slotIndexBucket)
			if errors.Is(err, storage.ErrBucketNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			cursor := bucket.Cursor()
			for indexKey, _ := cursor.First(); indexKey != nil; indexKey, _ = cursor.Next() {
				slot, key, err := decodeRedisSlotIndexKey(indexKey)
				if err != nil {
					return err
				}
				keyType, err := redisKeyTypeTx(tx, ns, key, nowMs)
				if err != nil {
					return err
				}
				if keyType == redisKeyTypeNone {
					continue
				}
				slotKeyCounts[slot]++
				totalKeys++
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, 0, err
	}
	nodeKeyCounts := make(map[string]int64, len(state.Nodes))
	for _, node := range state.Nodes {
		nodeKeyCounts[node.ID] = 0
	}
	for slot, count := range slotKeyCounts {
		owner := state.SlotOwners[slot]
		nodeKeyCounts[owner] += count
	}
	return slotKeyCounts, nodeKeyCounts, totalKeys, nil
}

func buildWeightedDestinationMoves(state *clusterState, localNodeID string, req clusterRebalancePlanForNodeRequest, slotKeyCounts []int64, currentKeyCounts map[string]int64, targetKeyCounts map[string]int64) ([]clusterRebalancePlanMoveResponse, int64, int64) {
	destinationNode, _ := state.NodeByID(req.DestinationNodeID)
	remainingKeys := targetKeyCounts[req.DestinationNodeID] - currentKeyCounts[req.DestinationNodeID]
	if remainingKeys < 0 {
		remainingKeys = 0
	}
	moves := make([]clusterRebalancePlanMoveResponse, 0)
	plannedKeys := int64(0)
	rangesByNode := make(map[string][]clusterSlotRange, len(state.Nodes))
	for _, slotRange := range clusterStateSlotRanges(state) {
		rangesByNode[slotRange.Node.ID] = append(rangesByNode[slotRange.Node.ID], slotRange)
	}
	for _, node := range state.Nodes {
		if node.ID == req.DestinationNodeID || remainingKeys == 0 {
			continue
		}
		excessKeys := currentKeyCounts[node.ID] - targetKeyCounts[node.ID]
		if excessKeys <= 0 {
			continue
		}
		toMoveKeys := excessKeys
		if toMoveKeys > remainingKeys {
			toMoveKeys = remainingKeys
		}
		nodeRanges := rangesByNode[node.ID]
		slotsMovedForNode := 0
		for i := len(nodeRanges) - 1; i >= 0 && toMoveKeys > 0 && remainingKeys > 0; i-- {
			slotRange := nodeRanges[i]
			moveStart, moveEnd, movedKeys, movedSlots, ok := takeWeightedTailRange(slotRange, slotKeyCounts, toMoveKeys, req.MaxSlotsPerSource, slotsMovedForNode)
			if !ok {
				continue
			}
			moves = append(moves, clusterRebalancePlanMoveResponse{
				SourceNode:        node,
				DestinationNode:   destinationNode,
				SourceNodeID:      node.ID,
				DestinationNodeID: destinationNode.ID,
				StartSlot:         moveStart,
				EndSlot:           moveEnd,
				SlotCount:         movedSlots,
				EstimatedKeyCount: movedKeys,
				SubmitOnSource:    node.ID == localNodeID,
			})
			plannedKeys += movedKeys
			remainingKeys -= movedKeys
			if remainingKeys < 0 {
				remainingKeys = 0
			}
			toMoveKeys -= movedKeys
			if toMoveKeys < 0 {
				toMoveKeys = 0
			}
			slotsMovedForNode += movedSlots
			if req.MaxMoves > 0 && len(moves) >= req.MaxMoves {
				return moves, plannedKeys, remainingKeys
			}
		}
	}
	return moves, plannedKeys, remainingKeys
}

func takeWeightedTailRange(slotRange clusterSlotRange, slotKeyCounts []int64, targetKeys int64, maxSlotsPerSource int, slotsMovedForNode int) (int, int, int64, int, bool) {
	if targetKeys <= 0 || slotRange.EndSlot < slotRange.StartSlot {
		return 0, 0, 0, 0, false
	}
	rangeSlotLimit := slotRange.EndSlot - slotRange.StartSlot + 1
	if maxSlotsPerSource > 0 {
		remainingSlotBudget := maxSlotsPerSource - slotsMovedForNode
		if remainingSlotBudget <= 0 {
			return 0, 0, 0, 0, false
		}
		if rangeSlotLimit > remainingSlotBudget {
			rangeSlotLimit = remainingSlotBudget
		}
	}
	movedKeys := int64(0)
	movedSlots := 0
	start := slotRange.EndSlot
	for slot := slotRange.EndSlot; slot >= slotRange.StartSlot && movedSlots < rangeSlotLimit; slot-- {
		movedKeys += slotKeyCounts[slot]
		movedSlots++
		start = slot
		if movedKeys >= targetKeys {
			break
		}
	}
	if movedSlots == 0 || movedKeys == 0 {
		return 0, 0, 0, 0, false
	}
	return start, slotRange.EndSlot, movedKeys, movedSlots, true
}

func clusterStateSlotRanges(state *clusterState) []clusterSlotRange {
	if state == nil || len(state.SlotOwners) == 0 {
		return nil
	}
	ranges := make([]clusterSlotRange, 0)
	startSlot := 0
	currentOwner := state.SlotOwners[0]
	for slot := 1; slot <= len(state.SlotOwners); slot++ {
		if slot < len(state.SlotOwners) && state.SlotOwners[slot] == currentOwner {
			continue
		}
		node, _ := state.NodeByID(currentOwner)
		ranges = append(ranges, clusterSlotRange{
			StartSlot: startSlot,
			EndSlot:   slot - 1,
			Node:      node,
		})
		if slot < len(state.SlotOwners) {
			startSlot = slot
			currentOwner = state.SlotOwners[slot]
		}
	}
	return ranges
}
