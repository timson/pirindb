package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/render"
)

type clusterStatusResponse struct {
	ClusterID                  string                            `json:"cluster_id"`
	Version                    uint64                            `json:"version"`
	Epoch                      uint64                            `json:"epoch"`
	LocalNodeID                string                            `json:"local_node_id"`
	SlotCount                  int                               `json:"slot_count"`
	SupportedTransferProtocols []uint16                          `json:"supported_transfer_protocol_versions"`
	RuntimeTopologyName        string                            `json:"runtime_topology_name,omitempty"`
	RuntimeTopologyHash        string                            `json:"runtime_topology_hash,omitempty"`
	ConfiguredTopologyName     string                            `json:"configured_topology_name,omitempty"`
	ConfiguredTopologyHash     string                            `json:"configured_topology_hash,omitempty"`
	ConfiguredNodeIDs          []string                          `json:"configured_node_ids,omitempty"`
	Rebalancing                bool                              `json:"rebalancing"`
	PendingMove                *clusterPendingMove               `json:"pending_move,omitempty"`
	Nodes                      []clusterNodeState                `json:"nodes"`
	Conditions                 []clusterStatusCondition          `json:"conditions,omitempty"`
	NodeStatuses               []clusterNodeStatusSummary        `json:"node_statuses,omitempty"`
	ActiveRebalanceJobs        []clusterRebalanceJobResponse     `json:"active_rebalance_jobs,omitempty"`
	ActiveOrchestrationJobs    []clusterAutoRebalanceJobResponse `json:"active_orchestration_jobs,omitempty"`
	ImportStagingKeys          uint64                            `json:"import_staging_keys"`
}

type clusterStatusCondition struct {
	Type    string `json:"type"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

type clusterNodeStatusSummary struct {
	NodeID               string `json:"node_id"`
	RedisAddress         string `json:"redis_address"`
	HTTPAddress          string `json:"http_address"`
	OwnedSlots           int    `json:"owned_slots"`
	TargetSlots          int    `json:"target_slots"`
	IsLocal              bool   `json:"is_local"`
	ConfiguredInTopology bool   `json:"configured_in_topology"`
	Draining             bool   `json:"draining"`
	Receiving            bool   `json:"receiving"`
}

type clusterTopologyResponse struct {
	LocalNodeID      string                 `json:"local_node_id"`
	TopologyFile     string                 `json:"topology_file,omitempty"`
	Topology         *ClusterTopologyConfig `json:"topology,omitempty"`
	RuntimeVersion   uint64                 `json:"runtime_version"`
	RuntimeEpoch     uint64                 `json:"runtime_epoch"`
	RuntimeSlotCount int                    `json:"runtime_slot_count"`
	RuntimeName      string                 `json:"runtime_topology_name,omitempty"`
	RuntimeHash      string                 `json:"runtime_topology_hash,omitempty"`
	ConfiguredHash   string                 `json:"configured_topology_hash,omitempty"`
}

type clusterRebalanceRequest struct {
	SourceNodeID      string `json:"source_node_id"`
	DestinationNodeID string `json:"destination_node_id"`
	StartSlot         int    `json:"start_slot"`
	EndSlot           int    `json:"end_slot"`
}

type clusterJoinNodeRequest struct {
	ID           string `json:"id"`
	RedisAddress string `json:"redis_address"`
	HTTPAddress  string `json:"http_address"`
}

type clusterRemoveNodeRequest struct {
	ID string `json:"id"`
}

type clusterRebalancePlanResponse struct {
	SourceNode      clusterNodeState `json:"source_node"`
	DestinationNode clusterNodeState `json:"destination_node"`
	StartSlot       int              `json:"start_slot"`
	EndSlot         int              `json:"end_slot"`
	Version         uint64           `json:"version"`
	LocalNodeID     string           `json:"local_node_id"`
	SubmitOnSource  bool             `json:"submit_on_source"`
}

type clusterRebalanceDeleteRequest struct {
	StartSlot int `json:"start_slot"`
	EndSlot   int `json:"end_slot"`
}

type clusterSlotVerificationResponse struct {
	NodeID            string                `json:"node_id"`
	Epoch             uint64                `json:"epoch"`
	StartSlot         int                   `json:"start_slot"`
	EndSlot           int                   `json:"end_slot"`
	LogicalKeyCount   uint64                `json:"logical_key_count"`
	LastChunkComplete bool                  `json:"last_chunk_complete"`
	LastChunkManifest clusterImportManifest `json:"last_chunk_manifest"`
}

type clusterImportCleanupRequest struct {
	JobID     string `json:"job_id"`
	LastChunk uint64 `json:"last_chunk"`
}

type clusterHTTPStatusError struct {
	StatusCode int
	Message    string
}

func (err *clusterHTTPStatusError) Error() string {
	return err.Message
}

const (
	clusterTransferHeaderClusterID         = "X-Pirin-Cluster-ID"
	clusterTransferHeaderMoveID            = "X-Pirin-Move-ID"
	clusterTransferHeaderSourceNodeID      = "X-Pirin-Source-Node-ID"
	clusterTransferHeaderDestinationNodeID = "X-Pirin-Destination-Node-ID"
	clusterTransferHeaderEpoch             = "X-Pirin-Cluster-Epoch"
)

type clusterRebalanceJob struct {
	mu                        sync.Mutex
	SchemaVersion             int
	ID                        string
	Status                    string
	Phase                     string
	Error                     string
	Move                      clusterPendingMove
	ChunkEntryLimit           int // Legacy persisted key-count limit; retained for v0 migration.
	ChunkTargetBytes          int64
	ChunkMaxBytes             int64
	ChunkMaxRecords           int
	SnapshotWatermark         uint64
	EstimateInitialized       bool
	EstimatedTotalKeys        uint64
	LastSentChunk             uint64
	LastCommittedChunk        uint64
	LastDeltaAppliedSeq       uint64
	DeltaMutationsApplied     uint64
	KeysTransferred           uint64
	BytesTransferred          uint64
	RecordsTransferred        uint64
	OversizedLogicalKeyChunks uint64
	RetryCount                uint64
	LastRetryReason           string
	CopyCursorDB              int
	CopyCursorKey             []byte
	CleanupCursorDB           int
	CleanupCursorKey          []byte
	CleanupDeletedKeys        uint64
	StartedAt                 *time.Time
	FinishedAt                *time.Time
}

type clusterRebalanceJobResponse struct {
	SchemaVersion              int                `json:"schema_version"`
	JobID                      string             `json:"job_id"`
	Status                     string             `json:"status"`
	Phase                      string             `json:"phase"`
	Error                      string             `json:"error,omitempty"`
	Move                       clusterPendingMove `json:"move"`
	ChunkEntryLimit            int                `json:"chunk_entry_limit,omitempty"`
	ChunkTargetBytes           int64              `json:"chunk_target_bytes"`
	ChunkMaxBytes              int64              `json:"chunk_max_bytes"`
	ChunkMaxRecords            int                `json:"chunk_max_records"`
	SnapshotWatermark          uint64             `json:"snapshot_watermark"`
	EstimateInitialized        bool               `json:"estimate_initialized"`
	EstimatedTotalKeys         uint64             `json:"estimated_total_keys"`
	EstimatedKeysRemaining     uint64             `json:"estimated_keys_remaining"`
	EstimatedBytesRemaining    uint64             `json:"estimated_bytes_remaining"`
	EstimatedSecondsRemaining  float64            `json:"estimated_seconds_remaining"`
	LastSentChunk              uint64             `json:"last_sent_chunk"`
	LastCommittedChunk         uint64             `json:"last_committed_chunk"`
	LastDeltaAppliedSeq        uint64             `json:"last_delta_applied_seq"`
	DeltaMutationsApplied      uint64             `json:"delta_mutations_applied"`
	KeysTransferred            uint64             `json:"keys_transferred"`
	BytesTransferred           uint64             `json:"bytes_transferred"`
	RecordsTransferred         uint64             `json:"records_transferred"`
	OversizedLogicalKeyChunks  uint64             `json:"oversized_logical_key_chunks"`
	RetryCount                 uint64             `json:"retry_count"`
	LastRetryReason            string             `json:"last_retry_reason,omitempty"`
	DirtyKeyLag                uint64             `json:"dirty_key_lag"`
	TransferRateBytesPerSecond float64            `json:"transfer_rate_bytes_per_second"`
	CopyCursorDB               int                `json:"copy_cursor_db"`
	CopyCursorKey              []byte             `json:"copy_cursor_key,omitempty"`
	CleanupCursorDB            int                `json:"cleanup_cursor_db"`
	CleanupCursorKey           []byte             `json:"cleanup_cursor_key,omitempty"`
	CleanupDeletedKeys         uint64             `json:"cleanup_deleted_keys"`
	StartedAt                  *time.Time         `json:"started_at,omitempty"`
	FinishedAt                 *time.Time         `json:"finished_at,omitempty"`
}

func (job *clusterRebalanceJob) snapshot() clusterRebalanceJobResponse {
	job.mu.Lock()
	defer job.mu.Unlock()
	response := clusterRebalanceJobResponse{
		SchemaVersion:             job.SchemaVersion,
		JobID:                     job.ID,
		Status:                    job.Status,
		Phase:                     job.Phase,
		Error:                     job.Error,
		Move:                      job.Move,
		ChunkEntryLimit:           job.ChunkEntryLimit,
		ChunkTargetBytes:          job.ChunkTargetBytes,
		ChunkMaxBytes:             job.ChunkMaxBytes,
		ChunkMaxRecords:           job.ChunkMaxRecords,
		SnapshotWatermark:         job.SnapshotWatermark,
		EstimateInitialized:       job.EstimateInitialized,
		EstimatedTotalKeys:        job.EstimatedTotalKeys,
		LastSentChunk:             job.LastSentChunk,
		LastCommittedChunk:        job.LastCommittedChunk,
		LastDeltaAppliedSeq:       job.LastDeltaAppliedSeq,
		DeltaMutationsApplied:     job.DeltaMutationsApplied,
		KeysTransferred:           job.KeysTransferred,
		BytesTransferred:          job.BytesTransferred,
		RecordsTransferred:        job.RecordsTransferred,
		OversizedLogicalKeyChunks: job.OversizedLogicalKeyChunks,
		RetryCount:                job.RetryCount,
		LastRetryReason:           job.LastRetryReason,
		CopyCursorDB:              job.CopyCursorDB,
		CopyCursorKey:             cloneBytes(job.CopyCursorKey),
		CleanupCursorDB:           job.CleanupCursorDB,
		CleanupCursorKey:          cloneBytes(job.CleanupCursorKey),
		CleanupDeletedKeys:        job.CleanupDeletedKeys,
		StartedAt:                 job.StartedAt,
		FinishedAt:                job.FinishedAt,
	}
	if job.StartedAt != nil {
		end := time.Now()
		if job.FinishedAt != nil {
			end = *job.FinishedAt
		}
		seconds := end.Sub(*job.StartedAt).Seconds()
		if seconds > 0 {
			response.TransferRateBytesPerSecond = float64(job.BytesTransferred) / seconds
		}
	}
	if job.EstimateInitialized && job.EstimatedTotalKeys > job.KeysTransferred {
		response.EstimatedKeysRemaining = job.EstimatedTotalKeys - job.KeysTransferred
		if job.KeysTransferred > 0 {
			averageBytesPerKey := float64(job.BytesTransferred) / float64(job.KeysTransferred)
			response.EstimatedBytesRemaining = uint64(averageBytesPerKey * float64(response.EstimatedKeysRemaining))
			if response.TransferRateBytesPerSecond > 0 {
				response.EstimatedSecondsRemaining = float64(response.EstimatedBytesRemaining) / response.TransferRateBytesPerSecond
			}
		}
	}
	return response
}

func (job *clusterRebalanceJob) markRunning() {
	job.mu.Lock()
	defer job.mu.Unlock()
	now := time.Now().UTC()
	job.Status = "running"
	job.Error = ""
	if job.StartedAt == nil {
		job.StartedAt = &now
	}
}

func (job *clusterRebalanceJob) markDone() {
	job.mu.Lock()
	defer job.mu.Unlock()
	now := time.Now().UTC()
	job.Status = "done"
	job.Phase = "done"
	job.FinishedAt = &now
}

func (job *clusterRebalanceJob) markFailed(err error) {
	job.mu.Lock()
	defer job.mu.Unlock()
	now := time.Now().UTC()
	job.Status = "failed"
	job.Error = err.Error()
	job.FinishedAt = &now
}

func (srv *Server) logClusterRebalancePhase(job *clusterRebalanceJob, message string) {
	if srv == nil || srv.Logger == nil || job == nil {
		return
	}
	snapshot := job.snapshot()
	clusterID := ""
	epoch := uint64(0)
	if srv.Cluster != nil {
		if state := srv.Cluster.Snapshot(); state != nil {
			clusterID = state.ClusterID
			epoch = state.Epoch
		}
	}
	srv.Logger.Info(message,
		"cluster_id", clusterID,
		"job_id", snapshot.JobID,
		"move_id", snapshot.Move.ID,
		"source", snapshot.Move.SourceNodeID,
		"destination", snapshot.Move.DestinationNodeID,
		"epoch", epoch,
		"phase", snapshot.Phase,
		"status", snapshot.Status,
	)
}

func (srv *Server) requireClusterManager(w http.ResponseWriter, r *http.Request) (*ClusterManager, *clusterState, bool) {
	if srv.clusterErr != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusInternalServerError, srv.clusterErr.Error()))
		return nil, nil, false
	}
	if srv.Cluster == nil {
		_ = render.Render(w, r, ErrStatus(http.StatusBadRequest, "cluster mode is disabled"))
		return nil, nil, false
	}
	state := srv.Cluster.Snapshot()
	if state == nil {
		_ = render.Render(w, r, ErrStatus(http.StatusInternalServerError, "cluster state is unavailable"))
		return nil, nil, false
	}
	return srv.Cluster, state, true
}

func buildClusterStatusResponse(cluster *ClusterManager, state *clusterState, cfg *Config) clusterStatusResponse {
	response := clusterStatusResponse{
		ClusterID:                  state.ClusterID,
		Version:                    state.Version,
		Epoch:                      state.Epoch,
		LocalNodeID:                cluster.LocalNodeID,
		SlotCount:                  state.SlotCount,
		SupportedTransferProtocols: []uint16{clusterTransferProtocolVersion},
		RuntimeTopologyName:        state.TopologyName,
		RuntimeTopologyHash:        state.TopologyHash,
		Rebalancing:                state.Rebalancing,
		PendingMove:                state.PendingMove,
		Nodes:                      append([]clusterNodeState(nil), state.Nodes...),
	}

	slotCounts := clusterNodeSlotCounts(state)
	targetSlotCounts := clusterTargetSlotCounts(state)
	configuredNodeIDs := make(map[string]struct{})
	topologyAligned := true
	topologyReason := "NoTopologyConfigured"
	topologyMessage := "cluster topology file is not configured"
	if cluster != nil && cluster.ConfiguredTopology() != nil {
		topology := cluster.ConfiguredTopology()
		response.ConfiguredTopologyName = topology.Name
		response.ConfiguredTopologyHash = clusterTopologyHash(topology)
		desiredNodes, err := buildClusterMembershipFromTopology(topology)
		if err != nil {
			topologyAligned = false
			topologyReason = "InvalidConfiguredTopology"
			topologyMessage = err.Error()
		} else {
			for _, node := range desiredNodes {
				configuredNodeIDs[node.ID] = struct{}{}
				response.ConfiguredNodeIDs = append(response.ConfiguredNodeIDs, node.ID)
			}
			sort.Strings(response.ConfiguredNodeIDs)
			if state.TopologyHash != clusterTopologyHash(topology) {
				topologyAligned = false
				topologyReason = "TopologyHashMismatch"
				topologyMessage = "runtime topology hash does not match configured topology"
			} else if len(desiredNodes) != len(state.Nodes) {
				topologyAligned = false
				topologyReason = "MembershipMismatch"
				topologyMessage = "runtime membership differs from configured topology"
			} else {
				for _, desired := range desiredNodes {
					current, ok := state.NodeByID(desired.ID)
					if !ok || current.RedisAddress != desired.RedisAddress || current.HTTPAddress != desired.HTTPAddress {
						topologyAligned = false
						topologyReason = "MembershipMismatch"
						topologyMessage = "runtime membership differs from configured topology"
						break
					}
				}
				if topologyAligned {
					topologyReason = "HashMatch"
					topologyMessage = "runtime topology matches configured topology"
				}
			}
		}
	}

	slotBalanced := true
	for _, node := range state.Nodes {
		if slotCounts[node.ID] != targetSlotCounts[node.ID] {
			slotBalanced = false
			break
		}
	}

	response.Conditions = []clusterStatusCondition{
		{
			Type:    "Available",
			Status:  "True",
			Reason:  "StateLoaded",
			Message: "cluster state is available",
		},
		{
			Type:    "TopologyAligned",
			Status:  boolToConditionStatus(topologyAligned),
			Reason:  topologyReason,
			Message: topologyMessage,
		},
		{
			Type:    "Rebalancing",
			Status:  boolToConditionStatus(state.Rebalancing),
			Reason:  clusterRebalanceConditionReason(state.Rebalancing),
			Message: clusterRebalanceConditionMessage(state),
		},
		{
			Type:    "SlotBalanced",
			Status:  boolToConditionStatus(slotBalanced),
			Reason:  clusterSlotBalanceReason(slotBalanced),
			Message: clusterSlotBalanceMessage(slotBalanced),
		},
	}

	response.NodeStatuses = make([]clusterNodeStatusSummary, 0, len(state.Nodes))
	for _, node := range state.Nodes {
		_, configured := configuredNodeIDs[node.ID]
		if cluster == nil || cluster.ConfiguredTopology() == nil {
			configured = true
		}
		draining := state.PendingMove != nil && state.PendingMove.SourceNodeID == node.ID
		receiving := state.PendingMove != nil && state.PendingMove.DestinationNodeID == node.ID
		response.NodeStatuses = append(response.NodeStatuses, clusterNodeStatusSummary{
			NodeID:               node.ID,
			RedisAddress:         node.RedisAddress,
			HTTPAddress:          node.HTTPAddress,
			OwnedSlots:           slotCounts[node.ID],
			TargetSlots:          targetSlotCounts[node.ID],
			IsLocal:              node.ID == cluster.LocalNodeID,
			ConfiguredInTopology: configured,
			Draining:             draining,
			Receiving:            receiving,
		})
	}

	return response
}

func boolToConditionStatus(value bool) string {
	if value {
		return "True"
	}
	return "False"
}

func clusterRebalanceConditionReason(active bool) string {
	if active {
		return "MoveInProgress"
	}
	return "Idle"
}

func clusterRebalanceConditionMessage(state *clusterState) string {
	if state != nil && state.Rebalancing && state.PendingMove != nil {
		return fmt.Sprintf("moving slots %d-%d from %s to %s", state.PendingMove.StartSlot, state.PendingMove.EndSlot, state.PendingMove.SourceNodeID, state.PendingMove.DestinationNodeID)
	}
	return "no rebalance is running"
}

func clusterSlotBalanceReason(balanced bool) string {
	if balanced {
		return "EvenSlotOwnership"
	}
	return "UnevenSlotOwnership"
}

func clusterSlotBalanceMessage(balanced bool) string {
	if balanced {
		return "slot ownership matches the even per-node target"
	}
	return "slot ownership differs from the even per-node target"
}

func (srv *Server) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	cluster, state, ok := srv.requireClusterManager(w, r)
	if !ok {
		return
	}
	// Refresh only the configured topology cache. Runtime membership is not
	// changed by a status read; the operator uses this to confirm that every
	// projected ConfigMap revision is visible before it calls reconciliation.
	if _, err := cluster.RefreshConfiguredTopology(); err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusInternalServerError, "refresh configured topology: "+err.Error()))
		return
	}
	response := buildClusterStatusResponse(cluster, state, srv.Config)
	if stagingKeys, err := countClusterImportStagingKeys(srv.DB); err == nil {
		response.ImportStagingKeys = stagingKeys
	}
	srv.rebalanceMu.Lock()
	for _, job := range srv.rebalanceJobs {
		if job.active() {
			response.ActiveRebalanceJobs = append(response.ActiveRebalanceJobs, srv.rebalanceJobSnapshotWithLag(job))
		}
	}
	srv.rebalanceMu.Unlock()
	srv.autoMu.Lock()
	for _, job := range srv.autoJobs {
		if job.active() {
			response.ActiveOrchestrationJobs = append(response.ActiveOrchestrationJobs, job.snapshot())
		}
	}
	srv.autoMu.Unlock()
	render.JSON(w, r, response)
}

func (srv *Server) handleClusterTopology(w http.ResponseWriter, r *http.Request) {
	cluster, state, ok := srv.requireClusterManager(w, r)
	if !ok {
		return
	}
	if _, err := cluster.RefreshConfiguredTopology(); err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusInternalServerError, "refresh configured topology: "+err.Error()))
		return
	}
	configuredTopology := cluster.ConfiguredTopology()
	configuredHash := ""
	if configuredTopology != nil {
		configuredHash = clusterTopologyHash(configuredTopology)
	}
	render.JSON(w, r, clusterTopologyResponse{
		LocalNodeID:      cluster.LocalNodeID,
		TopologyFile:     strings.TrimSpace(srv.Config.Cluster.TopologyFile),
		Topology:         configuredTopology,
		RuntimeVersion:   state.Version,
		RuntimeEpoch:     state.Epoch,
		RuntimeSlotCount: state.SlotCount,
		RuntimeName:      state.TopologyName,
		RuntimeHash:      state.TopologyHash,
		ConfiguredHash:   configuredHash,
	})
}

func (srv *Server) handleClusterReconcileTopology(w http.ResponseWriter, r *http.Request) {
	cluster, state, ok := srv.requireClusterManager(w, r)
	if !ok {
		return
	}
	if cluster.ConfiguredTopology() == nil {
		_ = render.Render(w, r, ErrStatus(http.StatusBadRequest, "cluster topology file is not configured"))
		return
	}
	originalState := state.Clone()
	updatedState, err := cluster.ReconcileConfiguredTopology()
	if err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusBadRequest, err.Error()))
		return
	}
	if updatedState.Version != originalState.Version || updatedState.Epoch != originalState.Epoch {
		if err = srv.pushClusterStateToPeers(r.Context(), updatedState); err != nil {
			_ = cluster.ReplaceState(originalState)
			_ = srv.pushClusterStateToPeers(r.Context(), originalState)
			joblessErr := err
			_ = render.Render(w, r, ErrStatus(http.StatusBadGateway, joblessErr.Error()))
			return
		}
	}
	render.JSON(w, r, buildClusterStatusResponse(cluster, updatedState, srv.Config))
}

func (srv *Server) handleClusterSlots(w http.ResponseWriter, r *http.Request) {
	cluster, _, ok := srv.requireClusterManager(w, r)
	if !ok {
		return
	}
	render.JSON(w, r, cluster.SlotRanges())
}

func (srv *Server) handleClusterJoinNode(w http.ResponseWriter, r *http.Request) {
	cluster, state, ok := srv.requireClusterManager(w, r)
	if !ok {
		return
	}

	var req clusterJoinNodeRequest
	if err := decodeStrictClusterJSON(r, &req); err != nil {
		_ = render.Render(w, r, ErrInvalidRequest())
		return
	}

	node := clusterNodeState{
		ID:           strings.TrimSpace(req.ID),
		RedisAddress: strings.TrimSpace(req.RedisAddress),
		HTTPAddress:  strings.TrimSpace(req.HTTPAddress),
	}
	originalState := state.Clone()
	updatedState, err := cluster.AddNode(node)
	if err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusBadRequest, err.Error()))
		return
	}
	if err = srv.pushClusterStateToPeers(r.Context(), updatedState); err != nil {
		_ = cluster.ReplaceState(originalState)
		_ = srv.pushClusterStateToPeers(r.Context(), originalState)
		_ = srv.postClusterState(r.Context(), node, originalState)
		_ = render.Render(w, r, ErrStatus(http.StatusBadGateway, err.Error()))
		return
	}

	render.JSON(w, r, buildClusterStatusResponse(cluster, updatedState, srv.Config))
}

func (srv *Server) handleClusterRemoveNode(w http.ResponseWriter, r *http.Request) {
	cluster, state, ok := srv.requireClusterManager(w, r)
	if !ok {
		return
	}
	var req clusterRemoveNodeRequest
	if err := decodeStrictClusterJSON(r, &req); err != nil {
		_ = render.Render(w, r, ErrInvalidRequest())
		return
	}
	nodeID := strings.TrimSpace(req.ID)
	if nodeID == "" {
		_ = render.Render(w, r, ErrStatus(http.StatusBadRequest, "id is required"))
		return
	}
	originalState := state.Clone()
	removedNode, _ := originalState.NodeByID(nodeID)
	updatedState, err := cluster.RemoveNode(nodeID)
	if err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusBadRequest, err.Error()))
		return
	}
	if err = srv.pushClusterStateToPeers(r.Context(), updatedState); err != nil {
		_ = cluster.ReplaceState(originalState)
		_ = srv.pushClusterStateToPeers(r.Context(), originalState)
		_ = render.Render(w, r, ErrStatus(http.StatusBadGateway, err.Error()))
		return
	}
	if removedNode.ID != "" {
		_ = srv.postClusterState(r.Context(), removedNode, updatedState)
	}
	render.JSON(w, r, buildClusterStatusResponse(cluster, updatedState, srv.Config))
}

func (srv *Server) decodeClusterRebalanceRequest(r *http.Request) (clusterPendingMove, error) {
	var req clusterRebalanceRequest
	if err := decodeStrictClusterJSON(r, &req); err != nil {
		return clusterPendingMove{}, err
	}
	return clusterPendingMove{
		SourceNodeID:      req.SourceNodeID,
		DestinationNodeID: req.DestinationNodeID,
		StartSlot:         req.StartSlot,
		EndSlot:           req.EndSlot,
	}, nil
}

func (srv *Server) handleClusterRebalancePlan(w http.ResponseWriter, r *http.Request) {
	cluster, state, ok := srv.requireClusterManager(w, r)
	if !ok {
		return
	}
	move, err := srv.decodeClusterRebalanceRequest(r)
	if err != nil {
		_ = render.Render(w, r, ErrInvalidRequest())
		return
	}
	if err = validateClusterRebalanceMove(state, move); err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusBadRequest, err.Error()))
		return
	}
	sourceNode, _ := state.NodeByID(move.SourceNodeID)
	destinationNode, _ := state.NodeByID(move.DestinationNodeID)
	render.JSON(w, r, clusterRebalancePlanResponse{
		SourceNode:      sourceNode,
		DestinationNode: destinationNode,
		StartSlot:       move.StartSlot,
		EndSlot:         move.EndSlot,
		Version:         state.Version,
		LocalNodeID:     cluster.LocalNodeID,
		SubmitOnSource:  move.SourceNodeID == cluster.LocalNodeID,
	})
}

func (srv *Server) handleClusterRebalancePlanForNode(w http.ResponseWriter, r *http.Request) {
	cluster, state, ok := srv.requireClusterManager(w, r)
	if !ok {
		return
	}
	req, err := decodeClusterRebalancePlanForNodeRequest(r)
	if err != nil {
		_ = render.Render(w, r, ErrInvalidRequest())
		return
	}
	metrics, metricsErr := srv.collectClusterPlanningMetrics(r.Context(), state)
	var plan clusterRebalancePlanForNodeResponse
	if metricsErr != nil && req.AllowSlotCountFallback {
		plan, err = buildClusterRebalancePlanForNode(srv.DB, state, cluster.LocalNodeID, req, redisNowUnixMilli(time.Now()))
	} else if metricsErr != nil {
		err = fmt.Errorf("cluster-wide metrics are required: %w", metricsErr)
	} else {
		plan, err = buildClusterRebalancePlanForNodeWithMetrics(srv.DB, state, cluster.LocalNodeID, req, metrics, redisNowUnixMilli(time.Now()))
	}
	if err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusBadRequest, err.Error()))
		return
	}
	render.JSON(w, r, plan)
}

func (srv *Server) handleClusterRebalanceExecute(w http.ResponseWriter, r *http.Request) {
	_, state, ok := srv.requireClusterManager(w, r)
	if !ok {
		return
	}
	move, err := srv.decodeClusterRebalanceRequest(r)
	if err != nil {
		_ = render.Render(w, r, ErrInvalidRequest())
		return
	}
	if err = validateClusterRebalanceMove(state, move); err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusBadRequest, err.Error()))
		return
	}
	if move.SourceNodeID != srv.Cluster.LocalNodeID {
		_ = render.Render(w, r, ErrStatus(http.StatusBadRequest, "rebalance execute must be submitted to the source shard"))
		return
	}
	if !srv.allowClusterAdminJobStart() {
		writeClusterAPIError(w, http.StatusTooManyRequests, "cluster_job_rate_limited", "too many cluster jobs were started in the current window")
		return
	}
	job, err := srv.enqueueClusterRebalanceJob(move)
	if err != nil {
		status := http.StatusInternalServerError
		if strings.Contains(err.Error(), "running") || strings.Contains(err.Error(), "rebalancing") {
			status = http.StatusConflict
		}
		_ = render.Render(w, r, ErrStatus(status, err.Error()))
		return
	}
	if err = srv.startBackgroundJob(func(ctx context.Context) {
		srv.runClusterRebalanceJob(ctx, job)
	}); err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusServiceUnavailable, err.Error()))
		return
	}

	render.Status(r, http.StatusAccepted)
	render.JSON(w, r, job.snapshot())
}

func (srv *Server) handleClusterRebalanceAuto(w http.ResponseWriter, r *http.Request) {
	cluster, state, ok := srv.requireClusterManager(w, r)
	if !ok {
		return
	}
	req, err := decodeClusterRebalancePlanForNodeRequest(r)
	if err != nil {
		_ = render.Render(w, r, ErrInvalidRequest())
		return
	}
	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	existing, err := srv.orchestrationJobByIdempotencyKey("scale-out", idempotencyKey, req.DestinationNodeID)
	if err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusConflict, err.Error()))
		return
	}
	if existing != nil {
		snapshot := existing.snapshot()
		if snapshot.ScaleRequest != nil && *snapshot.ScaleRequest != req {
			_ = render.Render(w, r, ErrStatus(http.StatusConflict, "idempotency key was already used with different scale limits"))
			return
		}
		if existing.active() {
			render.Status(r, http.StatusAccepted)
		}
		render.JSON(w, r, existing.snapshot())
		return
	}
	if !srv.allowClusterAdminJobStart() {
		writeClusterAPIError(w, http.StatusTooManyRequests, "cluster_job_rate_limited", "too many cluster jobs were started in the current window")
		return
	}
	metrics, metricsErr := srv.collectClusterPlanningMetrics(r.Context(), state)
	if metricsErr != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusServiceUnavailable, "cluster-wide metrics are required: "+metricsErr.Error()))
		return
	}
	planningReq := req
	planningReq.MaxMoves = 1
	plan, err := buildClusterRebalancePlanForNodeWithMetrics(srv.DB, state, cluster.LocalNodeID, planningReq, metrics, redisNowUnixMilli(time.Now()))
	if err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusBadRequest, err.Error()))
		return
	}
	autoJob := &clusterAutoRebalanceJob{
		SchemaVersion:  clusterOrchestrationSchemaVersion,
		ID:             strconv.FormatInt(time.Now().UnixNano(), 10),
		IdempotencyKey: idempotencyKey,
		RequestedNode:  req.DestinationNodeID,
		ClusterID:      state.ClusterID,
		PlanEpoch:      state.Epoch,
		Kind:           "scale-out",
		Status:         clusterRebalanceStatusQueued,
		Plan:           plan,
		ScaleRequest:   &req,
		Steps:          make([]clusterAutoRebalanceStep, 0, len(plan.Moves)),
	}
	for _, move := range plan.Moves {
		autoJob.Steps = append(autoJob.Steps, clusterAutoRebalanceStep{Move: move})
	}

	if err = srv.registerClusterOrchestrationJob(autoJob); err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusConflict, err.Error()))
		return
	}

	if len(plan.Moves) == 0 {
		autoJob.markRunning()
		autoJob.markDone()
		if err = srv.persistClusterOrchestrationJob(autoJob); err != nil {
			_ = render.Render(w, r, ErrStatus(http.StatusInternalServerError, err.Error()))
			return
		}
		render.JSON(w, r, autoJob.snapshot())
		return
	}

	if err = srv.startBackgroundJob(func(ctx context.Context) {
		srv.runClusterAutoRebalanceJob(ctx, autoJob)
	}); err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusServiceUnavailable, err.Error()))
		return
	}
	render.Status(r, http.StatusAccepted)
	render.JSON(w, r, autoJob.snapshot())
}

func (srv *Server) handleClusterDrainNodePlan(w http.ResponseWriter, r *http.Request) {
	cluster, state, ok := srv.requireClusterManager(w, r)
	if !ok {
		return
	}
	req, err := decodeClusterDrainNodeRequest(r)
	if err != nil {
		_ = render.Render(w, r, ErrInvalidRequest())
		return
	}
	metrics, metricsErr := srv.collectClusterPlanningMetrics(r.Context(), state)
	var plan clusterDrainNodeResponse
	if metricsErr != nil && req.AllowSlotCountFallback {
		plan, err = buildClusterDrainNodePlan(state, cluster.LocalNodeID, req)
	} else if metricsErr != nil {
		err = fmt.Errorf("cluster-wide metrics are required: %w", metricsErr)
	} else {
		plan, err = buildClusterDrainNodePlanWithMetrics(state, cluster.LocalNodeID, req, metrics)
	}
	if err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusBadRequest, err.Error()))
		return
	}
	render.JSON(w, r, plan)
}

func (srv *Server) handleClusterDrainNode(w http.ResponseWriter, r *http.Request) {
	cluster, state, ok := srv.requireClusterManager(w, r)
	if !ok {
		return
	}
	req, err := decodeClusterDrainNodeRequest(r)
	if err != nil {
		_ = render.Render(w, r, ErrInvalidRequest())
		return
	}
	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	existing, err := srv.orchestrationJobByIdempotencyKey("drain", idempotencyKey, req.NodeID)
	if err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusConflict, err.Error()))
		return
	}
	if existing != nil {
		snapshot := existing.snapshot()
		if snapshot.DrainRequest != nil && *snapshot.DrainRequest != req {
			_ = render.Render(w, r, ErrStatus(http.StatusConflict, "idempotency key was already used with different drain limits"))
			return
		}
		if existing.active() {
			render.Status(r, http.StatusAccepted)
		}
		render.JSON(w, r, existing.snapshot())
		return
	}
	if !srv.allowClusterAdminJobStart() {
		writeClusterAPIError(w, http.StatusTooManyRequests, "cluster_job_rate_limited", "too many cluster jobs were started in the current window")
		return
	}
	planningReq := req
	planningReq.MaxMoves = 1
	planningReq.AllowSlotCountFallback = false
	metrics, metricsErr := srv.collectClusterPlanningMetrics(r.Context(), state)
	if metricsErr != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusServiceUnavailable, "cluster-wide metrics are required: "+metricsErr.Error()))
		return
	}
	plan, err := buildClusterDrainNodePlanWithMetrics(state, cluster.LocalNodeID, planningReq, metrics)
	if err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusBadRequest, err.Error()))
		return
	}
	autoJob := &clusterAutoRebalanceJob{
		SchemaVersion:  clusterOrchestrationSchemaVersion,
		ID:             strconv.FormatInt(time.Now().UnixNano(), 10),
		IdempotencyKey: idempotencyKey,
		RequestedNode:  req.NodeID,
		ClusterID:      state.ClusterID,
		PlanEpoch:      state.Epoch,
		Kind:           "drain",
		Status:         clusterRebalanceStatusQueued,
		Plan:           clusterDrainPlanAsRebalancePlan(plan),
		DrainRequest:   &req,
		Steps:          make([]clusterAutoRebalanceStep, 0, len(plan.Moves)),
	}
	for _, move := range plan.Moves {
		autoJob.Steps = append(autoJob.Steps, clusterAutoRebalanceStep{Move: move})
	}

	if err = srv.registerClusterOrchestrationJob(autoJob); err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusConflict, err.Error()))
		return
	}

	if len(plan.Moves) == 0 {
		autoJob.markRunning()
		autoJob.markDone()
		if err = srv.persistClusterOrchestrationJob(autoJob); err != nil {
			_ = render.Render(w, r, ErrStatus(http.StatusInternalServerError, err.Error()))
			return
		}
		render.JSON(w, r, autoJob.snapshot())
		return
	}

	if err = srv.startBackgroundJob(func(ctx context.Context) {
		srv.runClusterAutoRebalanceJob(ctx, autoJob)
	}); err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusServiceUnavailable, err.Error()))
		return
	}
	render.Status(r, http.StatusAccepted)
	render.JSON(w, r, autoJob.snapshot())
}

func clusterDrainPlanAsRebalancePlan(plan clusterDrainNodeResponse) clusterRebalancePlanForNodeResponse {
	return clusterRebalancePlanForNodeResponse{
		LocalNodeID:           plan.LocalNodeID,
		Version:               plan.Version,
		Epoch:                 plan.Epoch,
		SlotCount:             plan.SlotCount,
		WeightMetric:          plan.WeightMetric,
		CurrentOwnedSlots:     plan.CurrentOwnedSlots,
		PlannedSlots:          plan.PlannedSlots,
		RemainingDeficitSlots: plan.RemainingOwnedSlots,
		CurrentOwnedBytes:     plan.CurrentOwnedBytes,
		DeficitBytes:          plan.CurrentOwnedBytes,
		PlannedBytes:          plan.PlannedBytes,
		RemainingDeficitBytes: plan.RemainingOwnedBytes,
		NodeEstimatedBytes:    plan.NodeEstimatedBytes,
		TargetEstimatedBytes:  plan.TargetEstimatedBytes,
		MetricsCollectedAt:    plan.MetricsCollectedAt,
		Complete:              plan.Complete,
		NodeSlotCounts:        plan.NodeSlotCounts,
		TargetSlotCounts:      plan.TargetSlotCounts,
		Moves:                 plan.Moves,
	}
}

func (srv *Server) handleClusterAutoRebalanceJobStatus(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "jobID")
	srv.autoMu.Lock()
	job, ok := srv.autoJobs[jobID]
	srv.autoMu.Unlock()
	if !ok {
		_ = render.Render(w, r, ErrStatus(http.StatusNotFound, "auto rebalance job not found"))
		return
	}
	render.JSON(w, r, job.snapshot())
}

func (srv *Server) handleClusterDrainJobStatus(w http.ResponseWriter, r *http.Request) {
	srv.handleClusterAutoRebalanceJobStatus(w, r)
}

func (srv *Server) handleClusterRebalanceJobStatus(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "jobID")
	srv.rebalanceMu.Lock()
	job, ok := srv.rebalanceJobs[jobID]
	srv.rebalanceMu.Unlock()
	if !ok {
		_ = render.Render(w, r, ErrStatus(http.StatusNotFound, "rebalance job not found"))
		return
	}
	render.JSON(w, r, srv.rebalanceJobSnapshotWithLag(job))
}

func (srv *Server) rebalanceJobSnapshotWithLag(job *clusterRebalanceJob) clusterRebalanceJobResponse {
	response := job.snapshot()
	last, err := clusterDeltaLastSequence(srv.DB, response.Move.ID)
	if err == nil && last > response.LastDeltaAppliedSeq {
		response.DirtyKeyLag = last - response.LastDeltaAppliedSeq
	}
	return response
}

func validateClusterRebalanceMove(state *clusterState, move clusterPendingMove) error {
	if move.SourceNodeID == "" || move.DestinationNodeID == "" {
		return errors.New("source_node_id and destination_node_id are required")
	}
	if move.SourceNodeID == move.DestinationNodeID {
		return errors.New("source and destination nodes must differ")
	}
	if move.StartSlot < 0 || move.EndSlot < move.StartSlot || move.EndSlot >= state.SlotCount {
		return errors.New("slot range is invalid")
	}
	if _, ok := state.NodeByID(move.SourceNodeID); !ok {
		return fmt.Errorf("unknown source node %q", move.SourceNodeID)
	}
	if _, ok := state.NodeByID(move.DestinationNodeID); !ok {
		return fmt.Errorf("unknown destination node %q", move.DestinationNodeID)
	}
	for slot := move.StartSlot; slot <= move.EndSlot; slot++ {
		if state.SlotOwners[slot] != move.SourceNodeID {
			return fmt.Errorf("slot %d is owned by %s, not %s", slot, state.SlotOwners[slot], move.SourceNodeID)
		}
	}
	return nil
}

func (srv *Server) enqueueClusterRebalanceJob(move clusterPendingMove) (*clusterRebalanceJob, error) {
	job := &clusterRebalanceJob{
		SchemaVersion:    clusterRebalanceJobSchemaVersion,
		ID:               strconv.FormatInt(time.Now().UnixNano(), 10),
		Status:           clusterRebalanceStatusQueued,
		Phase:            clusterRebalancePhaseCopying,
		Move:             move,
		ChunkTargetBytes: clusterTransferChunkTargetBytes(srv.Config),
		ChunkMaxBytes:    clusterTransferChunkMaxBytes(srv.Config),
		ChunkMaxRecords:  clusterTransferChunkMaxRecords(srv.Config),
		CopyCursorDB:     redisDatabaseMin,
		CleanupCursorDB:  redisDatabaseMin,
	}
	job.Move.ID = job.ID

	srv.rebalanceMu.Lock()
	defer srv.rebalanceMu.Unlock()
	for _, existing := range srv.rebalanceJobs {
		if existing.active() {
			return nil, errors.New("another rebalance job is already running")
		}
	}
	srv.rebalanceJobs[job.ID] = job
	if err := srv.persistRebalanceJob(job); err != nil {
		delete(srv.rebalanceJobs, job.ID)
		return nil, err
	}
	return job, nil
}

func (srv *Server) runClusterAutoRebalanceJob(ctx context.Context, job *clusterAutoRebalanceJob) {
	if job == nil || contextCancelled(ctx) {
		return
	}
	fail := func(err error) {
		job.markFailed(err)
		_ = srv.persistClusterOrchestrationJob(job)
	}
	failCode := func(code string, err error) {
		job.markFailedWithCode(code, err)
		_ = srv.persistClusterOrchestrationJob(job)
	}
	state := srv.Cluster.Snapshot()
	if state == nil {
		fail(errors.New("cluster state is unavailable"))
		return
	}
	initial := job.snapshot()
	if initial.ClusterID != "" && initial.ClusterID != state.ClusterID {
		fail(errors.New("orchestration job belongs to a different cluster"))
		return
	}
	job.markRunning()
	if err := srv.persistClusterOrchestrationJob(job); err != nil {
		fail(err)
		return
	}
	for {
		snapshot := job.snapshot()
		if snapshot.CurrentStep >= len(snapshot.Steps) {
			added, err := srv.appendNextClusterOrchestrationStep(ctx, job)
			if err != nil {
				if contextCancelled(ctx) {
					_ = srv.persistClusterOrchestrationJob(job)
					return
				}
				fail(err)
				return
			}
			if !added {
				break
			}
			snapshot = job.snapshot()
		}
		idx := snapshot.CurrentStep
		if contextCancelled(ctx) {
			_ = srv.persistClusterOrchestrationJob(job)
			return
		}
		step := snapshot.Steps[idx]
		sourceNode := step.Move.SourceNode
		rebalanceJobID := step.RebalanceJobID
		if rebalanceJobID == "" {
			currentState := srv.Cluster.Snapshot()
			if currentState == nil {
				fail(errors.New("cluster state is unavailable"))
				return
			}
			if snapshot.PlanEpoch != 0 && currentState.Epoch != snapshot.PlanEpoch {
				failCode("stale_plan", fmt.Errorf("stale plan epoch %d; current epoch is %d", snapshot.PlanEpoch, currentState.Epoch))
				return
			}
			move := clusterPendingMove{
				SourceNodeID:      step.Move.SourceNodeID,
				DestinationNodeID: step.Move.DestinationNodeID,
				StartSlot:         step.Move.StartSlot,
				EndSlot:           step.Move.EndSlot,
			}
			if err := validateClusterRebalanceMove(currentState, move); err != nil {
				failCode("stale_plan", fmt.Errorf("stale orchestration plan at step %d: %w", idx, err))
				return
			}
			rebalanceJob, err := srv.submitClusterRebalanceMove(ctx, sourceNode, step.Move)
			if err != nil {
				if contextCancelled(ctx) {
					_ = srv.persistClusterOrchestrationJob(job)
					return
				}
				job.updateStep(idx, func(step *clusterAutoRebalanceStep) { step.Error = err.Error() })
				fail(err)
				return
			}
			rebalanceJobID = rebalanceJob.JobID
			job.updateStep(idx, func(step *clusterAutoRebalanceStep) {
				step.RebalanceJobID = rebalanceJob.JobID
				step.RebalanceStatus = rebalanceJob.Status
			})
			if err = srv.persistClusterOrchestrationJob(job); err != nil {
				fail(err)
				return
			}
		}
		finalStatus, err := srv.waitForClusterRebalanceJob(ctx, sourceNode, rebalanceJobID)
		if err != nil {
			if contextCancelled(ctx) {
				_ = srv.persistClusterOrchestrationJob(job)
				return
			}
			job.updateStep(idx, func(step *clusterAutoRebalanceStep) {
				step.RebalanceStatus = clusterRebalanceStatusFailed
				step.Error = err.Error()
			})
			fail(err)
			return
		}
		job.updateStep(idx, func(step *clusterAutoRebalanceStep) {
			step.RebalanceStatus = finalStatus.Status
			if finalStatus.Error != "" {
				step.Error = finalStatus.Error
			}
		})
		if finalStatus.Status != clusterRebalanceStatusDone {
			err = errors.New("rebalance step did not complete successfully")
			fail(err)
			return
		}
		job.advanceStep(idx + 1)
		if err = srv.persistClusterOrchestrationJob(job); err != nil {
			fail(err)
			return
		}
	}
	job.markDone()
	_ = srv.persistClusterOrchestrationJob(job)
}

func (srv *Server) appendNextClusterOrchestrationStep(ctx context.Context, job *clusterAutoRebalanceJob) (bool, error) {
	snapshot := job.snapshot()
	state := srv.Cluster.Snapshot()
	if state == nil {
		return false, errors.New("cluster state is unavailable")
	}
	if state.Rebalancing {
		return false, errors.New("cannot recalculate orchestration plan while a move is active")
	}
	var (
		plan clusterRebalancePlanForNodeResponse
		move clusterRebalancePlanMoveResponse
	)
	switch snapshot.Kind {
	case "scale-out":
		currentSlots := clusterNodeSlotCounts(state)
		targetSlots := clusterTargetSlotCounts(state)
		if currentSlots[snapshot.RequestedNode] >= targetSlots[snapshot.RequestedNode] {
			return false, nil
		}
		metrics, err := srv.collectClusterPlanningMetrics(ctx, state)
		if err != nil {
			return false, fmt.Errorf("cluster-wide metrics are required: %w", err)
		}
		planningRequest := clusterRebalancePlanForNodeRequest{DestinationNodeID: snapshot.RequestedNode}
		if snapshot.ScaleRequest != nil {
			planningRequest = *snapshot.ScaleRequest
		}
		planningRequest.DestinationNodeID = snapshot.RequestedNode
		planningRequest.MaxMoves = 1
		planningRequest.AllowSlotCountFallback = false
		plan, err = buildClusterRebalancePlanForNodeWithMetrics(srv.DB, state, srv.Cluster.LocalNodeID, planningRequest, metrics, redisNowUnixMilli(time.Now()))
		if err != nil {
			return false, err
		}
		if len(plan.Moves) == 0 {
			return false, nil
		}
		move = plan.Moves[0]
	case "drain":
		if clusterNodeSlotCounts(state)[snapshot.RequestedNode] == 0 {
			return false, nil
		}
		metrics, err := srv.collectClusterPlanningMetrics(ctx, state)
		if err != nil {
			return false, fmt.Errorf("cluster-wide metrics are required: %w", err)
		}
		planningRequest := clusterDrainNodeRequest{NodeID: snapshot.RequestedNode}
		if snapshot.DrainRequest != nil {
			planningRequest = *snapshot.DrainRequest
		}
		planningRequest.NodeID = snapshot.RequestedNode
		planningRequest.MaxMoves = 1
		planningRequest.AllowSlotCountFallback = false
		drainPlan, err := buildClusterDrainNodePlanWithMetrics(state, srv.Cluster.LocalNodeID, planningRequest, metrics)
		if err != nil {
			return false, err
		}
		if len(drainPlan.Moves) == 0 {
			return false, nil
		}
		move = drainPlan.Moves[0]
		plan = clusterDrainPlanAsRebalancePlan(drainPlan)
	default:
		return false, fmt.Errorf("unsupported orchestration job kind %q", snapshot.Kind)
	}
	job.mu.Lock()
	if job.CurrentStep < len(job.Steps) {
		job.Steps = job.Steps[:job.CurrentStep]
	}
	job.Plan = plan
	job.PlanEpoch = state.Epoch
	job.Steps = append(job.Steps, clusterAutoRebalanceStep{Move: move})
	now := time.Now().UTC()
	job.UpdatedAt = &now
	job.mu.Unlock()
	if err := srv.persistClusterOrchestrationJob(job); err != nil {
		return false, err
	}
	return true, nil
}

func (srv *Server) runClusterRebalanceJob(ctx context.Context, job *clusterRebalanceJob) {
	if contextCancelled(ctx) {
		return
	}
	defer func() {
		snapshot := job.snapshot()
		if snapshot.Status == clusterRebalanceStatusFailed {
			srv.logClusterRebalancePhase(job, "cluster rebalance failed")
		}
		if snapshot.Status != clusterRebalanceStatusFailed || snapshot.LastSentChunk == 0 {
			return
		}
		state := srv.Cluster.Snapshot()
		if state == nil {
			return
		}
		destination, found := state.NodeByID(snapshot.Move.DestinationNodeID)
		if !found {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := srv.postClusterImportCleanup(cleanupCtx, destination, snapshot.JobID, snapshot.LastSentChunk); err != nil && srv.Logger != nil {
			srv.Logger.Warn("failed to clean destination staging for terminal rebalance", "job_id", snapshot.JobID, "destination", destination.ID, "error", err)
		}
	}()
	job.markRunning()
	srv.logClusterRebalancePhase(job, "cluster rebalance worker started")
	_ = srv.persistRebalanceJob(job)
	if job.Move.ID == "" {
		job.mu.Lock()
		job.Move.ID = job.ID
		job.mu.Unlock()
		_ = srv.persistRebalanceJob(job)
	}

	originalState := srv.Cluster.Snapshot()
	if originalState == nil {
		job.markFailed(errors.New("cluster state is unavailable"))
		_ = srv.persistRebalanceJob(job)
		return
	}

	state := srv.Cluster.Snapshot()
	if state == nil {
		job.markFailed(errors.New("cluster state is unavailable"))
		_ = srv.persistRebalanceJob(job)
		return
	}
	if state.Rebalancing && state.PendingMove != nil &&
		state.PendingMove.ID == "" &&
		state.PendingMove.SourceNodeID == job.Move.SourceNodeID &&
		state.PendingMove.DestinationNodeID == job.Move.DestinationNodeID &&
		state.PendingMove.StartSlot == job.Move.StartSlot &&
		state.PendingMove.EndSlot == job.Move.EndSlot {
		normalizedState := state.Clone()
		normalizedState.PendingMove.ID = job.Move.ID
		if err := srv.Cluster.ReplaceState(normalizedState); err != nil {
			job.markFailed(err)
			_ = srv.persistRebalanceJob(job)
			return
		}
		state = normalizedState
		originalState = normalizedState.Clone()
	}

	if job.Phase == "" {
		job.Phase = clusterRebalancePhaseCopying
	}
	if job.Phase == clusterRebalancePhaseCopying && !job.EstimateInitialized {
		totalKeys, countErr := countClusterSlotRangeKeys(srv.DB, state.SlotCount, job.Move.StartSlot, job.Move.EndSlot, redisNowUnixMilli(time.Now()))
		if countErr != nil {
			job.markFailed(countErr)
			_ = srv.persistRebalanceJob(job)
			return
		}
		job.mu.Lock()
		job.EstimatedTotalKeys = totalKeys
		job.EstimateInitialized = true
		job.mu.Unlock()
		if err := srv.persistRebalanceJob(job); err != nil {
			job.markFailed(err)
			_ = srv.persistRebalanceJob(job)
			return
		}
	}
	if state.Rebalancing && state.PendingMove != nil && state.PendingMove.ID == job.Move.ID {
		switch state.PendingMove.Stage {
		case clusterMoveStageCatchup:
			if job.Phase == clusterRebalancePhaseCopying {
				job.mu.Lock()
				job.Phase = clusterRebalancePhaseCatchup
				job.mu.Unlock()
				srv.logClusterRebalancePhase(job, "cluster rebalance resumed at catch-up")
				_ = srv.persistRebalanceJob(job)
			}
		case clusterMoveStageAsking:
			if job.Phase == clusterRebalancePhaseCopying || job.Phase == clusterRebalancePhaseCatchup {
				job.mu.Lock()
				job.Phase = clusterRebalancePhaseAsking
				job.mu.Unlock()
				srv.logClusterRebalancePhase(job, "cluster rebalance resumed at handoff")
				_ = srv.persistRebalanceJob(job)
			}
		}
	}

	var workingState *clusterState
	var err error
	if job.Phase == clusterRebalancePhaseCopying {
		if contextCancelled(ctx) {
			_ = srv.persistRebalanceJob(job)
			return
		}
		if !(state.Rebalancing && state.PendingMove != nil &&
			state.PendingMove.ID == job.Move.ID &&
			state.PendingMove.SourceNodeID == job.Move.SourceNodeID &&
			state.PendingMove.DestinationNodeID == job.Move.DestinationNodeID &&
			state.PendingMove.StartSlot == job.Move.StartSlot &&
			state.PendingMove.EndSlot == job.Move.EndSlot) {
			workingState, err = srv.Cluster.BeginRebalance(job.Move)
			if err != nil {
				job.markFailed(err)
				_ = srv.persistRebalanceJob(job)
				return
			}
			if err = srv.pushClusterStateToPeers(ctx, workingState); err != nil {
				if contextCancelled(ctx) {
					_ = srv.persistRebalanceJob(job)
					return
				}
				_, _ = srv.Cluster.AbortRebalance()
				job.markFailed(err)
				_ = srv.persistRebalanceJob(job)
				return
			}
		} else {
			workingState = state
		}

		destinationNode, _ := workingState.NodeByID(job.Move.DestinationNodeID)
		chunkIndex := job.LastCommittedChunk + 1
		for {
			if contextCancelled(ctx) {
				_ = srv.persistRebalanceJob(job)
				return
			}
			job.mu.Lock()
			job.LastSentChunk = chunkIndex
			job.SnapshotWatermark = job.KeysTransferred
			job.mu.Unlock()
			if err = srv.persistRebalanceJob(job); err != nil {
				job.markFailed(err)
				_ = srv.persistRebalanceJob(job)
				return
			}

			nowMs := redisNowUnixMilli(time.Now())
			stats, chunkErr := srv.postClusterSlotImportChunkWithPolicy(
				ctx,
				destinationNode,
				job.ID,
				chunkIndex,
				workingState.SlotCount,
				job.Move.StartSlot,
				job.Move.EndSlot,
				nowMs,
				job.CopyCursorDB,
				job.CopyCursorKey,
				clusterTransferChunkPolicy{
					TargetBytes: job.ChunkTargetBytes,
					MaxBytes:    job.ChunkMaxBytes,
					MaxRecords:  uint64(job.ChunkMaxRecords),
					MaxKeys:     job.ChunkEntryLimit,
				},
			)
			if chunkErr != nil {
				if contextCancelled(ctx) {
					_ = srv.persistRebalanceJob(job)
					return
				}
				job.markFailed(chunkErr)
				_ = srv.persistRebalanceJob(job)
				return
			}

			job.mu.Lock()
			job.LastCommittedChunk = chunkIndex
			job.KeysTransferred += stats.EntriesTransferred
			job.BytesTransferred += stats.BytesTransferred
			job.RecordsTransferred += stats.RecordsTransferred
			if stats.OversizedLogicalKey {
				job.OversizedLogicalKeyChunks++
			}
			job.RetryCount += stats.Retries
			if stats.LastRetryReason != "" {
				job.LastRetryReason = stats.LastRetryReason
			}
			job.SnapshotWatermark = job.KeysTransferred
			job.CopyCursorDB = stats.NextCursorDB
			job.CopyCursorKey = cloneBytes(stats.NextCursorKey)
			job.mu.Unlock()
			if err = srv.persistRebalanceJob(job); err != nil {
				job.markFailed(err)
				_ = srv.persistRebalanceJob(job)
				return
			}
			if stats.EOF {
				job.mu.Lock()
				job.Phase = clusterRebalancePhaseCatchup
				job.mu.Unlock()
				srv.logClusterRebalancePhase(job, "cluster rebalance base copy completed")
				if err = srv.persistRebalanceJob(job); err != nil {
					job.markFailed(err)
					_ = srv.persistRebalanceJob(job)
					return
				}
				break
			}
			chunkIndex++
		}
	}

	if job.Phase == clusterRebalancePhaseCatchup {
		if contextCancelled(ctx) {
			_ = srv.persistRebalanceJob(job)
			return
		}
		catchupState := srv.Cluster.Snapshot()
		if catchupState == nil {
			job.markFailed(errors.New("cluster state is unavailable"))
			_ = srv.persistRebalanceJob(job)
			return
		}
		if !(catchupState.Rebalancing && catchupState.PendingMove != nil &&
			catchupState.PendingMove.ID == job.Move.ID &&
			catchupState.PendingMove.Stage == clusterMoveStageCatchup) {
			catchupState, err = srv.Cluster.SetRebalanceStage(clusterMoveStageCatchup)
			if err != nil {
				job.markFailed(err)
				_ = srv.persistRebalanceJob(job)
				return
			}
			if err = srv.pushClusterStateToPeers(ctx, catchupState); err != nil {
				_, _ = srv.Cluster.AbortRebalance()
				_ = srv.pushClusterStateToPeers(context.Background(), originalState)
				if contextCancelled(ctx) {
					_ = srv.persistRebalanceJob(job)
					return
				}
				job.markFailed(err)
				_ = srv.persistRebalanceJob(job)
				return
			}
		}

		destinationNode, _ := catchupState.NodeByID(job.Move.DestinationNodeID)
		for {
			if contextCancelled(ctx) {
				_ = srv.persistRebalanceJob(job)
				return
			}
			batch, loadErr := loadClusterDeltaBatch(srv.DB, job.Move.ID, job.LastDeltaAppliedSeq, clusterDeltaBatchEntryLimit)
			if loadErr != nil {
				job.markFailed(loadErr)
				_ = srv.persistRebalanceJob(job)
				return
			}
			if len(batch.Mutations) == 0 {
				if err = srv.verifyClusterSlotRange(ctx, destinationNode, job); err != nil {
					job.markFailed(err)
					_ = srv.persistRebalanceJob(job)
					return
				}
				job.mu.Lock()
				job.Phase = clusterRebalancePhaseAsking
				job.mu.Unlock()
				srv.logClusterRebalancePhase(job, "cluster rebalance catch-up verified")
				if err = srv.persistRebalanceJob(job); err != nil {
					job.markFailed(err)
					_ = srv.persistRebalanceJob(job)
					return
				}
				break
			}
			chunkIndex := job.LastCommittedChunk + 1
			job.mu.Lock()
			job.LastSentChunk = chunkIndex
			job.mu.Unlock()
			if err = srv.persistRebalanceJob(job); err != nil {
				job.markFailed(err)
				_ = srv.persistRebalanceJob(job)
				return
			}
			stats, streamErr := srv.postClusterDirtyKeyBatch(
				ctx,
				destinationNode,
				job.ID,
				chunkIndex,
				catchupState.SlotCount,
				job.Move.StartSlot,
				job.Move.EndSlot,
				redisNowUnixMilli(time.Now()),
				batch,
			)
			if streamErr != nil {
				if contextCancelled(ctx) {
					_ = srv.persistRebalanceJob(job)
					return
				}
				job.markFailed(streamErr)
				_ = srv.persistRebalanceJob(job)
				return
			}
			job.mu.Lock()
			job.LastCommittedChunk = chunkIndex
			job.LastDeltaAppliedSeq = batch.LastSeq
			job.DeltaMutationsApplied += uint64(batch.RecordCount)
			job.BytesTransferred += stats.BytesTransferred
			job.RecordsTransferred += stats.RecordsTransferred
			job.RetryCount += stats.Retries
			if stats.LastRetryReason != "" {
				job.LastRetryReason = stats.LastRetryReason
			}
			job.mu.Unlock()
			if err = srv.persistRebalanceJob(job); err != nil {
				job.markFailed(err)
				_ = srv.persistRebalanceJob(job)
				return
			}
		}
	}

	if job.Phase == clusterRebalancePhaseAsking {
		if contextCancelled(ctx) {
			_ = srv.persistRebalanceJob(job)
			return
		}
		askingState := srv.Cluster.Snapshot()
		if askingState == nil {
			job.markFailed(errors.New("cluster state is unavailable"))
			_ = srv.persistRebalanceJob(job)
			return
		}
		if !(askingState.Rebalancing && askingState.PendingMove != nil &&
			askingState.PendingMove.ID == job.Move.ID &&
			askingState.PendingMove.Stage == clusterMoveStageAsking) {
			var stageErr error
			askingState, stageErr = srv.Cluster.SetRebalanceStage(clusterMoveStageAsking)
			if stageErr != nil {
				job.markFailed(stageErr)
				_ = srv.persistRebalanceJob(job)
				return
			}
			if err = srv.pushClusterStateToPeers(ctx, askingState); err != nil {
				_, _ = srv.Cluster.AbortRebalance()
				_ = srv.pushClusterStateToPeers(context.Background(), originalState)
				if contextCancelled(ctx) {
					_ = srv.persistRebalanceJob(job)
					return
				}
				job.markFailed(err)
				_ = srv.persistRebalanceJob(job)
				return
			}
		}

		finalState, finishErr := srv.Cluster.FinishRebalance(job.Move)
		if finishErr != nil {
			_, _ = srv.Cluster.AbortRebalance()
			_ = srv.pushClusterStateToPeers(context.Background(), originalState)
			job.markFailed(finishErr)
			_ = srv.persistRebalanceJob(job)
			return
		}
		if err = srv.pushClusterStateToPeers(ctx, finalState); err != nil {
			if contextCancelled(ctx) {
				_ = srv.persistRebalanceJob(job)
				return
			}
			job.markFailed(err)
			_ = srv.persistRebalanceJob(job)
			return
		}
		job.mu.Lock()
		job.Phase = clusterRebalancePhaseCleanup
		job.mu.Unlock()
		srv.logClusterRebalancePhase(job, "cluster rebalance ownership changed; cleanup started")
		if err = srv.persistRebalanceJob(job); err != nil {
			job.markFailed(err)
			_ = srv.persistRebalanceJob(job)
			return
		}
	}

	if job.Phase == clusterRebalancePhaseCleanup {
		if contextCancelled(ctx) {
			_ = srv.persistRebalanceJob(job)
			return
		}
		finalState := srv.Cluster.Snapshot()
		if finalState == nil {
			job.markFailed(errors.New("cluster state is unavailable"))
			_ = srv.persistRebalanceJob(job)
			return
		}
		for {
			if contextCancelled(ctx) {
				_ = srv.persistRebalanceJob(job)
				return
			}
			stats, cleanupErr := deleteClusterSlotRangeChunk(
				srv.DB,
				finalState.SlotCount,
				job.Move.StartSlot,
				job.Move.EndSlot,
				redisNowUnixMilli(time.Now()),
				job.CleanupCursorDB,
				job.CleanupCursorKey,
				job.ChunkEntryLimit,
			)
			if cleanupErr != nil {
				job.markFailed(cleanupErr)
				_ = srv.persistRebalanceJob(job)
				return
			}
			job.mu.Lock()
			job.CleanupDeletedKeys += stats.DeletedKeys
			job.CleanupCursorDB = stats.NextCursorDB
			job.CleanupCursorKey = cloneBytes(stats.NextCursorKey)
			job.mu.Unlock()
			if err = srv.persistRebalanceJob(job); err != nil {
				job.markFailed(err)
				_ = srv.persistRebalanceJob(job)
				return
			}
			if stats.EOF {
				break
			}
		}
	}
	if err = deleteClusterDeltaMoveData(srv.DB, job.Move.ID); err != nil {
		job.markFailed(err)
		_ = srv.persistRebalanceJob(job)
		return
	}
	finalSnapshot := job.snapshot()
	if finalState := srv.Cluster.Snapshot(); finalState != nil {
		if destination, found := finalState.NodeByID(finalSnapshot.Move.DestinationNodeID); found {
			if cleanupErr := srv.postClusterImportCleanup(ctx, destination, finalSnapshot.JobID, finalSnapshot.LastCommittedChunk); cleanupErr != nil {
				srv.Logger.Warn("destination import receipt cleanup failed", "job_id", finalSnapshot.JobID, "destination", destination.ID, "error", cleanupErr)
			}
		}
	}
	job.markDone()
	srv.logClusterRebalancePhase(job, "cluster rebalance completed")
	_ = srv.persistRebalanceJob(job)
}

func (srv *Server) pushClusterStateToPeers(ctx context.Context, state *clusterState) error {
	for _, node := range state.Nodes {
		if contextCancelled(ctx) {
			return ctx.Err()
		}
		if node.ID == srv.Cluster.LocalNodeID {
			continue
		}
		if err := srv.postClusterState(ctx, node, state); err != nil {
			return err
		}
	}
	return nil
}

func (srv *Server) clusterTransferIdentity(destinationNodeID string, moveID string) (clusterTransferIdentity, error) {
	if srv.Cluster == nil {
		return clusterTransferIdentity{}, errors.New("cluster mode is disabled")
	}
	state := srv.Cluster.Snapshot()
	if state == nil {
		return clusterTransferIdentity{}, errors.New("cluster state is unavailable")
	}
	if moveID == "" || destinationNodeID == "" {
		return clusterTransferIdentity{}, errors.New("cluster transfer identity is incomplete")
	}
	if _, ok := state.NodeByID(destinationNodeID); !ok {
		return clusterTransferIdentity{}, fmt.Errorf("unknown cluster transfer destination %q", destinationNodeID)
	}
	return clusterTransferIdentity{
		ClusterID:         state.ClusterID,
		MoveID:            moveID,
		SourceNodeID:      srv.Cluster.LocalNodeID,
		DestinationNodeID: destinationNodeID,
		Epoch:             state.Epoch,
	}, nil
}

func setClusterTransferIdentityHeaders(header http.Header, identity clusterTransferIdentity) {
	header.Set(clusterTransferHeaderClusterID, identity.ClusterID)
	header.Set(clusterTransferHeaderMoveID, identity.MoveID)
	header.Set(clusterTransferHeaderSourceNodeID, identity.SourceNodeID)
	header.Set(clusterTransferHeaderDestinationNodeID, identity.DestinationNodeID)
	header.Set(clusterTransferHeaderEpoch, strconv.FormatUint(identity.Epoch, 10))
}

func expectedClusterTransferIdentity(r *http.Request, cluster *ClusterManager, state *clusterState, receiptContext clusterImportReceiptContext) (clusterTransferIdentity, error) {
	if receiptContext.JobID == "" {
		return clusterTransferIdentity{}, nil
	}
	identity := clusterTransferIdentity{
		ClusterID:         strings.TrimSpace(r.Header.Get(clusterTransferHeaderClusterID)),
		MoveID:            strings.TrimSpace(r.Header.Get(clusterTransferHeaderMoveID)),
		SourceNodeID:      strings.TrimSpace(r.Header.Get(clusterTransferHeaderSourceNodeID)),
		DestinationNodeID: strings.TrimSpace(r.Header.Get(clusterTransferHeaderDestinationNodeID)),
	}
	epoch, err := strconv.ParseUint(strings.TrimSpace(r.Header.Get(clusterTransferHeaderEpoch)), 10, 64)
	if err != nil {
		return clusterTransferIdentity{}, errors.New("cluster transfer epoch header is invalid")
	}
	identity.Epoch = epoch
	if identity.ClusterID == "" || identity.MoveID == "" || identity.SourceNodeID == "" || identity.DestinationNodeID == "" {
		return clusterTransferIdentity{}, errors.New("cluster transfer identity headers are required")
	}
	if identity.MoveID != receiptContext.JobID {
		return clusterTransferIdentity{}, errors.New("cluster transfer move id does not match receipt job")
	}
	if state == nil || identity.ClusterID != state.ClusterID || identity.Epoch != state.Epoch {
		return clusterTransferIdentity{}, errors.New("cluster transfer cluster id or epoch is stale")
	}
	if cluster == nil || identity.DestinationNodeID != cluster.LocalNodeID {
		return clusterTransferIdentity{}, errors.New("cluster transfer destination does not match local node")
	}
	if _, ok := state.NodeByID(identity.SourceNodeID); !ok {
		return clusterTransferIdentity{}, errors.New("cluster transfer source node is unknown")
	}
	if state.Rebalancing && state.PendingMove != nil {
		move := state.PendingMove
		if move.ID != identity.MoveID || move.SourceNodeID != identity.SourceNodeID || move.DestinationNodeID != identity.DestinationNodeID {
			return clusterTransferIdentity{}, errors.New("cluster transfer identity does not match active move")
		}
	}
	return identity, nil
}

func (srv *Server) postClusterState(ctx context.Context, node clusterNodeState, state *clusterState) error {
	body, err := json.Marshal(state)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(node.HTTPAddress, "/")+"/api/v1/cluster/internal/state", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if srv.Cluster != nil {
		req.Header.Set(clusterTransferHeaderSourceNodeID, srv.Cluster.LocalNodeID)
		req.Header.Set(clusterTransferHeaderClusterID, state.ClusterID)
		req.Header.Set(clusterTransferHeaderEpoch, strconv.FormatUint(state.Epoch, 10))
	}
	resp, err := srv.clusterHTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		payload, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		return fmt.Errorf("state sync to %s failed: %s", node.ID, strings.TrimSpace(string(payload)))
	}
	return nil
}

func (srv *Server) postClusterSlotImportChunk(ctx context.Context, node clusterNodeState, jobID string, chunkIndex uint64, slotCount int, startSlot int, endSlot int, nowMs int64, cursorDB int, cursorKey []byte, limit int) (clusterSlotTransferStats, error) {
	return srv.postClusterSlotImportChunkWithPolicy(ctx, node, jobID, chunkIndex, slotCount, startSlot, endSlot, nowMs, cursorDB, cursorKey, clusterTransferChunkPolicy{MaxKeys: limit})
}

func (srv *Server) postClusterSlotImportChunkWithPolicy(ctx context.Context, node clusterNodeState, jobID string, chunkIndex uint64, slotCount int, startSlot int, endSlot int, nowMs int64, cursorDB int, cursorKey []byte, policy clusterTransferChunkPolicy) (clusterSlotTransferStats, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		stats, err := srv.postClusterSlotImportChunkAttempt(ctx, node, jobID, chunkIndex, slotCount, startSlot, endSlot, nowMs, cursorDB, cursorKey, policy)
		if err == nil {
			stats.Retries = uint64(attempt)
			if lastErr != nil {
				stats.LastRetryReason = lastErr.Error()
			}
			return stats, nil
		}
		lastErr = err
		manifest, manifestErr := srv.fetchClusterImportManifest(ctx, node, jobID, chunkIndex)
		if manifestErr == nil && manifest.Complete && stats.RecordsTransferred > 0 {
			stats.Retries = uint64(attempt + 1)
			stats.LastRetryReason = err.Error()
			return stats, nil
		}
		var statusErr *clusterHTTPStatusError
		if errors.As(err, &statusErr) && statusErr.StatusCode < 500 {
			return clusterSlotTransferStats{}, err
		}
		if manifestErr == nil && manifest.LastSequence > 0 {
			return clusterSlotTransferStats{}, fmt.Errorf("cluster import stopped after committed frame %d and cannot be regenerated safely: %w", manifest.LastSequence, err)
		}
		if attempt < 2 {
			if err = waitClusterTransferRetry(ctx, attempt); err != nil {
				return clusterSlotTransferStats{}, err
			}
		}
	}
	return clusterSlotTransferStats{}, lastErr
}

func (srv *Server) postClusterSlotImportChunkAttempt(ctx context.Context, node clusterNodeState, jobID string, chunkIndex uint64, slotCount int, startSlot int, endSlot int, nowMs int64, cursorDB int, cursorKey []byte, policy clusterTransferChunkPolicy) (clusterSlotTransferStats, error) {
	identity, err := srv.clusterTransferIdentity(node.ID, jobID)
	if err != nil {
		return clusterSlotTransferStats{}, err
	}
	pr, pw := io.Pipe()
	resultCh := make(chan struct {
		stats clusterSlotTransferStats
		err   error
	}, 1)
	go func() {
		stats, err := streamClusterSlotRangeChunkWithIdentityAndPolicy(srv.DB, pw, slotCount, startSlot, endSlot, nowMs, cursorDB, cursorKey, policy, identity)
		_ = pw.CloseWithError(err)
		resultCh <- struct {
			stats clusterSlotTransferStats
			err   error
		}{stats: stats, err: err}
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/api/v1/cluster/internal/slot-import?job_id=%s&chunk_index=%d", strings.TrimRight(node.HTTPAddress, "/"), jobID, chunkIndex), pr)
	if err != nil {
		_ = pr.CloseWithError(err)
		streamResult := <-resultCh
		return streamResult.stats, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	setClusterTransferIdentityHeaders(req.Header, identity)
	resp, err := srv.clusterHTTPClient.Do(req)
	if err != nil {
		_ = pr.CloseWithError(err)
		streamResult := <-resultCh
		return streamResult.stats, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		responseErr := &clusterHTTPStatusError{StatusCode: resp.StatusCode, Message: fmt.Sprintf("slot import to %s failed: %s", node.ID, strings.TrimSpace(string(body)))}
		_ = pr.CloseWithError(responseErr)
		streamResult := <-resultCh
		return streamResult.stats, responseErr
	}
	var importStats clusterSlotTransferStats
	if err = json.NewDecoder(resp.Body).Decode(&importStats); err != nil {
		_ = pr.CloseWithError(err)
		streamResult := <-resultCh
		return streamResult.stats, err
	}
	streamResult := <-resultCh
	if streamResult.err != nil {
		return clusterSlotTransferStats{}, streamResult.err
	}
	if importStats.BytesTransferred == 0 {
		importStats.BytesTransferred = streamResult.stats.BytesTransferred
	}
	if importStats.EntriesTransferred == 0 {
		importStats.EntriesTransferred = streamResult.stats.EntriesTransferred
	}
	if importStats.RecordsTransferred == 0 {
		importStats.RecordsTransferred = streamResult.stats.RecordsTransferred
	}
	importStats.OversizedLogicalKey = streamResult.stats.OversizedLogicalKey
	importStats.EOF = streamResult.stats.EOF
	importStats.NextCursorDB = streamResult.stats.NextCursorDB
	importStats.NextCursorKey = cloneBytes(streamResult.stats.NextCursorKey)
	return importStats, nil
}

func (srv *Server) postClusterDirtyKeyBatch(ctx context.Context, node clusterNodeState, jobID string, chunkIndex uint64, slotCount int, startSlot int, endSlot int, nowMs int64, batch clusterDeltaBatch) (clusterSlotTransferStats, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		stats, err := srv.postClusterDirtyKeyBatchAttempt(ctx, node, jobID, chunkIndex, slotCount, startSlot, endSlot, nowMs, batch)
		if err == nil {
			stats.Retries = uint64(attempt)
			if lastErr != nil {
				stats.LastRetryReason = lastErr.Error()
			}
			return stats, nil
		}
		lastErr = err
		manifest, manifestErr := srv.fetchClusterImportManifest(ctx, node, jobID, chunkIndex)
		if manifestErr == nil && manifest.Complete && stats.RecordsTransferred > 0 {
			stats.Retries = uint64(attempt + 1)
			stats.LastRetryReason = err.Error()
			return stats, nil
		}
		var statusErr *clusterHTTPStatusError
		if errors.As(err, &statusErr) && statusErr.StatusCode < 500 {
			return clusterSlotTransferStats{}, err
		}
		if manifestErr == nil && manifest.LastSequence > 0 {
			return clusterSlotTransferStats{}, fmt.Errorf("cluster dirty-key import stopped after committed frame %d: %w", manifest.LastSequence, err)
		}
		if attempt < 2 {
			if err = waitClusterTransferRetry(ctx, attempt); err != nil {
				return clusterSlotTransferStats{}, err
			}
		}
	}
	return clusterSlotTransferStats{}, lastErr
}

func (srv *Server) postClusterDirtyKeyBatchAttempt(ctx context.Context, node clusterNodeState, jobID string, chunkIndex uint64, slotCount int, startSlot int, endSlot int, nowMs int64, batch clusterDeltaBatch) (clusterSlotTransferStats, error) {
	identity, err := srv.clusterTransferIdentity(node.ID, jobID)
	if err != nil {
		return clusterSlotTransferStats{}, err
	}
	pr, pw := io.Pipe()
	resultCh := make(chan struct {
		stats clusterSlotTransferStats
		err   error
	}, 1)
	go func() {
		stats, err := streamClusterDirtyKeyBatchWithIdentity(srv.DB, pw, slotCount, startSlot, endSlot, nowMs, batch, identity)
		_ = pw.CloseWithError(err)
		resultCh <- struct {
			stats clusterSlotTransferStats
			err   error
		}{stats: stats, err: err}
	}()

	requestURL := fmt.Sprintf("%s/api/v1/cluster/internal/slot-import?job_id=%s&chunk_index=%d", strings.TrimRight(node.HTTPAddress, "/"), jobID, chunkIndex)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, pr)
	if err != nil {
		_ = pr.CloseWithError(err)
		streamResult := <-resultCh
		return streamResult.stats, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	setClusterTransferIdentityHeaders(req.Header, identity)
	resp, err := srv.clusterHTTPClient.Do(req)
	if err != nil {
		_ = pr.CloseWithError(err)
		streamResult := <-resultCh
		return streamResult.stats, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		responseErr := &clusterHTTPStatusError{StatusCode: resp.StatusCode, Message: fmt.Sprintf("dirty-key import to %s failed: %s", node.ID, strings.TrimSpace(string(body)))}
		_ = pr.CloseWithError(responseErr)
		streamResult := <-resultCh
		return streamResult.stats, responseErr
	}
	var importStats clusterSlotTransferStats
	if err = json.NewDecoder(resp.Body).Decode(&importStats); err != nil {
		_ = pr.CloseWithError(err)
		streamResult := <-resultCh
		return streamResult.stats, err
	}
	streamResult := <-resultCh
	if streamResult.err != nil {
		return clusterSlotTransferStats{}, streamResult.err
	}
	if importStats.BytesTransferred == 0 {
		importStats.BytesTransferred = streamResult.stats.BytesTransferred
	}
	if importStats.EntriesTransferred == 0 {
		importStats.EntriesTransferred = streamResult.stats.EntriesTransferred
	}
	if importStats.RecordsTransferred == 0 {
		importStats.RecordsTransferred = streamResult.stats.RecordsTransferred
	}
	return importStats, nil
}

func waitClusterTransferRetry(ctx context.Context, attempt int) error {
	jitter := time.Duration(time.Now().UnixNano() % int64(50*time.Millisecond))
	delay := time.Duration(attempt+1)*50*time.Millisecond + jitter
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (srv *Server) fetchClusterImportManifest(ctx context.Context, destination clusterNodeState, jobID string, chunkIndex uint64) (clusterImportManifest, error) {
	requestURL := fmt.Sprintf("%s/api/v1/cluster/internal/import-receipt?job_id=%s&chunk_index=%d", strings.TrimRight(destination.HTTPAddress, "/"), jobID, chunkIndex)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return clusterImportManifest{}, err
	}
	identity, err := srv.clusterTransferIdentity(destination.ID, jobID)
	if err != nil {
		return clusterImportManifest{}, err
	}
	setClusterTransferIdentityHeaders(req.Header, identity)
	resp, err := srv.clusterHTTPClient.Do(req)
	if err != nil {
		return clusterImportManifest{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		return clusterImportManifest{}, &clusterHTTPStatusError{StatusCode: resp.StatusCode, Message: fmt.Sprintf("import receipt from %s failed: %s", destination.ID, strings.TrimSpace(string(body)))}
	}
	var manifest clusterImportManifest
	if err = json.NewDecoder(io.LimitReader(resp.Body, 1024*1024)).Decode(&manifest); err != nil {
		return clusterImportManifest{}, err
	}
	return manifest, nil
}

func (srv *Server) verifyClusterSlotRange(ctx context.Context, destination clusterNodeState, job *clusterRebalanceJob) error {
	if job == nil {
		return errors.New("cluster transfer job is unavailable")
	}
	snapshot := job.snapshot()
	if snapshot.LastCommittedChunk == 0 {
		return errors.New("cluster transfer has no committed chunk to verify")
	}
	state := srv.Cluster.Snapshot()
	if state == nil {
		return errors.New("cluster state is unavailable")
	}
	nowMs := redisNowUnixMilli(time.Now())
	sourceCount, err := countClusterSlotRangeKeys(srv.DB, state.SlotCount, snapshot.Move.StartSlot, snapshot.Move.EndSlot, nowMs)
	if err != nil {
		return err
	}
	requestURL := fmt.Sprintf(
		"%s/api/v1/cluster/internal/slot-verify?start_slot=%d&end_slot=%d&job_id=%s&chunk_index=%d",
		strings.TrimRight(destination.HTTPAddress, "/"), snapshot.Move.StartSlot, snapshot.Move.EndSlot, snapshot.JobID, snapshot.LastCommittedChunk,
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return err
	}
	identity, err := srv.clusterTransferIdentity(destination.ID, snapshot.JobID)
	if err != nil {
		return err
	}
	setClusterTransferIdentityHeaders(req.Header, identity)
	resp, err := srv.clusterHTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		return fmt.Errorf("slot verification on %s failed: %s", destination.ID, strings.TrimSpace(string(body)))
	}
	var verification clusterSlotVerificationResponse
	if err = json.NewDecoder(io.LimitReader(resp.Body, 1024*1024)).Decode(&verification); err != nil {
		return err
	}
	if verification.Epoch != state.Epoch {
		return fmt.Errorf("slot verification epoch %d does not match source epoch %d", verification.Epoch, state.Epoch)
	}
	if !verification.LastChunkComplete {
		return errors.New("destination import manifest is not complete")
	}
	if verification.LogicalKeyCount != sourceCount {
		return fmt.Errorf("slot verification key count mismatch: source=%d destination=%d", sourceCount, verification.LogicalKeyCount)
	}
	return nil
}

func (srv *Server) postClusterImportCleanup(ctx context.Context, destination clusterNodeState, jobID string, lastChunk uint64) error {
	body, err := json.Marshal(clusterImportCleanupRequest{JobID: jobID, LastChunk: lastChunk})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(destination.HTTPAddress, "/")+"/api/v1/cluster/internal/import-cleanup", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	identity, err := srv.clusterTransferIdentity(destination.ID, jobID)
	if err != nil {
		return err
	}
	setClusterTransferIdentityHeaders(req.Header, identity)
	resp, err := srv.clusterHTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		responseBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		return fmt.Errorf("import receipt cleanup on %s failed: %s", destination.ID, strings.TrimSpace(string(responseBody)))
	}
	return nil
}

func (srv *Server) submitClusterRebalanceMove(ctx context.Context, node clusterNodeState, move clusterRebalancePlanMoveResponse) (clusterRebalanceJobResponse, error) {
	if srv.Cluster != nil && node.ID == srv.Cluster.LocalNodeID {
		job, err := srv.enqueueClusterRebalanceJob(clusterPendingMove{
			SourceNodeID:      move.SourceNodeID,
			DestinationNodeID: move.DestinationNodeID,
			StartSlot:         move.StartSlot,
			EndSlot:           move.EndSlot,
		})
		if err != nil {
			return clusterRebalanceJobResponse{}, err
		}
		if err = srv.startBackgroundJob(func(jobCtx context.Context) {
			srv.runClusterRebalanceJob(jobCtx, job)
		}); err != nil {
			return clusterRebalanceJobResponse{}, err
		}
		return job.snapshot(), nil
	}
	reqBody, err := json.Marshal(clusterRebalanceRequest{
		SourceNodeID:      move.SourceNodeID,
		DestinationNodeID: move.DestinationNodeID,
		StartSlot:         move.StartSlot,
		EndSlot:           move.EndSlot,
	})
	if err != nil {
		return clusterRebalanceJobResponse{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(node.HTTPAddress, "/")+"/api/v1/cluster/rebalance/execute", bytes.NewReader(reqBody))
	if err != nil {
		return clusterRebalanceJobResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := srv.clusterHTTPClient.Do(req)
	if err != nil {
		return clusterRebalanceJobResponse{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		payload, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		return clusterRebalanceJobResponse{}, fmt.Errorf("rebalance submit to %s failed: %s", node.ID, strings.TrimSpace(string(payload)))
	}
	var jobResp clusterRebalanceJobResponse
	if err = json.NewDecoder(resp.Body).Decode(&jobResp); err != nil {
		return clusterRebalanceJobResponse{}, err
	}
	return jobResp, nil
}

func (srv *Server) fetchClusterRebalanceJobStatus(ctx context.Context, node clusterNodeState, jobID string) (clusterRebalanceJobResponse, error) {
	if srv.Cluster != nil && node.ID == srv.Cluster.LocalNodeID {
		srv.rebalanceMu.Lock()
		job, ok := srv.rebalanceJobs[jobID]
		srv.rebalanceMu.Unlock()
		if !ok {
			return clusterRebalanceJobResponse{}, errors.New("rebalance job not found")
		}
		return job.snapshot(), nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(node.HTTPAddress, "/")+"/api/v1/cluster/rebalance/"+jobID, nil)
	if err != nil {
		return clusterRebalanceJobResponse{}, err
	}
	resp, err := srv.clusterHTTPClient.Do(req)
	if err != nil {
		return clusterRebalanceJobResponse{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		payload, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		return clusterRebalanceJobResponse{}, fmt.Errorf("rebalance status from %s failed: %s", node.ID, strings.TrimSpace(string(payload)))
	}
	var status clusterRebalanceJobResponse
	if err = json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return clusterRebalanceJobResponse{}, err
	}
	return status, nil
}

func (srv *Server) waitForClusterRebalanceJob(ctx context.Context, node clusterNodeState, jobID string) (clusterRebalanceJobResponse, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		status, err := srv.fetchClusterRebalanceJobStatus(ctx, node, jobID)
		if err != nil {
			return clusterRebalanceJobResponse{}, err
		}
		switch status.Status {
		case clusterRebalanceStatusDone:
			return status, nil
		case clusterRebalanceStatusFailed:
			if status.Error != "" {
				return status, errors.New(status.Error)
			}
			return status, errors.New("rebalance job failed")
		}
		select {
		case <-ctx.Done():
			return clusterRebalanceJobResponse{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (srv *Server) handleClusterInternalApplyState(w http.ResponseWriter, r *http.Request) {
	cluster, current, ok := srv.requireClusterManager(w, r)
	if !ok {
		return
	}
	var state clusterState
	if err := decodeStrictClusterJSON(r, &state); err != nil {
		_ = render.Render(w, r, ErrInvalidRequest())
		return
	}
	if state.ClusterID == "" || state.SlotCount != current.SlotCount {
		_ = render.Render(w, r, ErrStatus(http.StatusConflict, "cluster state identity does not match local cluster"))
		return
	}
	bootstrapJoin := state.ClusterID != current.ClusterID && current.Version <= 1 && current.Epoch <= 1
	if state.ClusterID != current.ClusterID && !bootstrapJoin {
		_ = render.Render(w, r, ErrStatus(http.StatusConflict, "cluster state id does not match local cluster"))
		return
	}
	if !bootstrapJoin && state.Epoch < current.Epoch {
		_ = render.Render(w, r, ErrStatus(http.StatusConflict, "stale cluster state epoch"))
		return
	}
	sourceNodeID := strings.TrimSpace(r.Header.Get(clusterTransferHeaderSourceNodeID))
	if sourceNodeID == "" {
		_ = render.Render(w, r, ErrStatus(http.StatusBadRequest, "cluster state source node header is required"))
		return
	}
	if _, found := state.NodeByID(sourceNodeID); !found {
		_ = render.Render(w, r, ErrStatus(http.StatusForbidden, "cluster state source node is unknown"))
		return
	}
	if err := cluster.ReplaceState(&state); err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusBadRequest, err.Error()))
		return
	}
	render.JSON(w, r, map[string]string{"status": "ok"})
}

func (srv *Server) handleClusterInternalSlotSnapshot(w http.ResponseWriter, r *http.Request) {
	_, state, ok := srv.requireClusterManager(w, r)
	if !ok {
		return
	}
	startSlot, err := strconv.Atoi(r.URL.Query().Get("start_slot"))
	if err != nil {
		_ = render.Render(w, r, ErrInvalidRequest())
		return
	}
	endSlot, err := strconv.Atoi(r.URL.Query().Get("end_slot"))
	if err != nil {
		_ = render.Render(w, r, ErrInvalidRequest())
		return
	}
	if startSlot < 0 || endSlot < startSlot || endSlot >= state.SlotCount {
		_ = render.Render(w, r, ErrStatus(http.StatusBadRequest, "slot range is invalid"))
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	if err := streamClusterSlotRange(srv.DB, w, state.SlotCount, startSlot, endSlot, redisNowUnixMilli(time.Now())); err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusInternalServerError, err.Error()))
		return
	}
}

func (srv *Server) handleClusterInternalSlotImport(w http.ResponseWriter, r *http.Request) {
	cluster, state, ok := srv.requireClusterManager(w, r)
	if !ok {
		return
	}
	receiptContext := clusterImportReceiptContext{JobID: strings.TrimSpace(r.URL.Query().Get("job_id"))}
	if receiptContext.JobID != "" {
		chunkIndex, err := strconv.ParseUint(r.URL.Query().Get("chunk_index"), 10, 64)
		if err != nil || chunkIndex == 0 {
			_ = render.Render(w, r, ErrStatus(http.StatusBadRequest, "chunk_index must be a positive integer"))
			return
		}
		receiptContext.ChunkIndex = chunkIndex
	}
	expectedIdentity, err := expectedClusterTransferIdentity(r, cluster, state, receiptContext)
	if err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusConflict, err.Error()))
		return
	}
	stats, err := importClusterSlotStreamWithReceiptAndIdentity(srv.DB, r.Body, redisNowUnixMilli(time.Now()), receiptContext, expectedIdentity)
	if err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusBadRequest, err.Error()))
		return
	}
	render.JSON(w, r, stats)
}

func (srv *Server) handleClusterInternalSlotVerify(w http.ResponseWriter, r *http.Request) {
	cluster, state, ok := srv.requireClusterManager(w, r)
	if !ok {
		return
	}
	startSlot, err := strconv.Atoi(r.URL.Query().Get("start_slot"))
	if err != nil {
		_ = render.Render(w, r, ErrInvalidRequest())
		return
	}
	endSlot, err := strconv.Atoi(r.URL.Query().Get("end_slot"))
	if err != nil || startSlot < 0 || endSlot < startSlot || endSlot >= state.SlotCount {
		_ = render.Render(w, r, ErrStatus(http.StatusBadRequest, "slot range is invalid"))
		return
	}
	receiptContext := clusterImportReceiptContext{JobID: strings.TrimSpace(r.URL.Query().Get("job_id"))}
	receiptContext.ChunkIndex, err = strconv.ParseUint(r.URL.Query().Get("chunk_index"), 10, 64)
	if receiptContext.JobID == "" || err != nil || receiptContext.ChunkIndex == 0 {
		_ = render.Render(w, r, ErrStatus(http.StatusBadRequest, "job_id and positive chunk_index are required"))
		return
	}
	if _, err = expectedClusterTransferIdentity(r, cluster, state, receiptContext); err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusConflict, err.Error()))
		return
	}
	count, err := countClusterSlotRangeKeys(srv.DB, state.SlotCount, startSlot, endSlot, redisNowUnixMilli(time.Now()))
	if err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusInternalServerError, err.Error()))
		return
	}
	manifest, err := loadClusterImportManifestFromDB(srv.DB, receiptContext)
	if err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusInternalServerError, err.Error()))
		return
	}
	render.JSON(w, r, clusterSlotVerificationResponse{
		NodeID:            cluster.LocalNodeID,
		Epoch:             state.Epoch,
		StartSlot:         startSlot,
		EndSlot:           endSlot,
		LogicalKeyCount:   count,
		LastChunkComplete: manifest.Complete,
		LastChunkManifest: manifest,
	})
}

func (srv *Server) handleClusterInternalSlotDelete(w http.ResponseWriter, r *http.Request) {
	cluster, state, ok := srv.requireClusterManager(w, r)
	if !ok {
		return
	}
	var req clusterRebalanceDeleteRequest
	if err := decodeStrictClusterJSON(r, &req); err != nil {
		_ = render.Render(w, r, ErrInvalidRequest())
		return
	}
	if req.StartSlot < 0 || req.EndSlot < req.StartSlot || req.EndSlot >= state.SlotCount {
		_ = render.Render(w, r, ErrStatus(http.StatusBadRequest, "slot range is invalid"))
		return
	}
	cursorDB := redisDatabaseMin
	var cursorKey []byte
	for {
		stats, err := deleteClusterSlotRangeChunk(srv.DB, state.SlotCount, req.StartSlot, req.EndSlot, redisNowUnixMilli(time.Now()), cursorDB, cursorKey, clusterImportBatchEntryLimit)
		if err != nil {
			_ = render.Render(w, r, ErrStatus(http.StatusInternalServerError, err.Error()))
			return
		}
		if stats.EOF {
			break
		}
		cursorDB = stats.NextCursorDB
		cursorKey = cloneBytes(stats.NextCursorKey)
	}
	render.JSON(w, r, map[string]string{"status": "ok", "node_id": cluster.LocalNodeID})
}

func (srv *Server) handleClusterInternalImportCleanup(w http.ResponseWriter, r *http.Request) {
	cluster, state, ok := srv.requireClusterManager(w, r)
	if !ok {
		return
	}
	var request clusterImportCleanupRequest
	if err := decodeStrictClusterJSON(r, &request); err != nil || strings.TrimSpace(request.JobID) == "" || request.LastChunk == 0 {
		_ = render.Render(w, r, ErrInvalidRequest())
		return
	}
	receiptContext := clusterImportReceiptContext{JobID: strings.TrimSpace(request.JobID), ChunkIndex: request.LastChunk}
	if _, err := expectedClusterTransferIdentity(r, cluster, state, receiptContext); err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusConflict, err.Error()))
		return
	}
	if err := deleteClusterImportJobState(srv.DB, receiptContext.JobID, request.LastChunk, state.SlotCount); err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusInternalServerError, err.Error()))
		return
	}
	render.JSON(w, r, map[string]string{"status": "ok"})
}

func (srv *Server) handleClusterInternalImportReceipt(w http.ResponseWriter, r *http.Request) {
	cluster, state, ok := srv.requireClusterManager(w, r)
	if !ok {
		return
	}
	receiptContext := clusterImportReceiptContext{JobID: strings.TrimSpace(r.URL.Query().Get("job_id"))}
	chunkIndex, err := strconv.ParseUint(r.URL.Query().Get("chunk_index"), 10, 64)
	if err != nil || receiptContext.JobID == "" || chunkIndex == 0 {
		_ = render.Render(w, r, ErrInvalidRequest())
		return
	}
	receiptContext.ChunkIndex = chunkIndex
	if _, err = expectedClusterTransferIdentity(r, cluster, state, receiptContext); err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusConflict, err.Error()))
		return
	}
	manifest, err := loadClusterImportManifestFromDB(srv.DB, receiptContext)
	if err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusInternalServerError, err.Error()))
		return
	}
	render.JSON(w, r, manifest)
}
