package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/render"
)

type clusterStatusResponse struct {
	ClusterID   string              `json:"cluster_id"`
	Version     uint64              `json:"version"`
	Epoch       uint64              `json:"epoch"`
	LocalNodeID string              `json:"local_node_id"`
	SlotCount   int                 `json:"slot_count"`
	Rebalancing bool                `json:"rebalancing"`
	PendingMove *clusterPendingMove `json:"pending_move,omitempty"`
	Nodes       []clusterNodeState  `json:"nodes"`
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

type clusterRebalanceJob struct {
	mu                    sync.Mutex
	ID                    string
	Status                string
	Phase                 string
	Error                 string
	Move                  clusterPendingMove
	ChunkEntryLimit       int
	SnapshotWatermark     uint64
	LastSentChunk         uint64
	LastCommittedChunk    uint64
	LastDeltaAppliedSeq   uint64
	DeltaMutationsApplied uint64
	KeysTransferred       uint64
	BytesTransferred      uint64
	CopyCursorDB          int
	CopyCursorKey         []byte
	CleanupCursorDB       int
	CleanupCursorKey      []byte
	CleanupDeletedKeys    uint64
	StartedAt             *time.Time
	FinishedAt            *time.Time
}

type clusterRebalanceJobResponse struct {
	JobID                 string             `json:"job_id"`
	Status                string             `json:"status"`
	Phase                 string             `json:"phase"`
	Error                 string             `json:"error,omitempty"`
	Move                  clusterPendingMove `json:"move"`
	ChunkEntryLimit       int                `json:"chunk_entry_limit"`
	SnapshotWatermark     uint64             `json:"snapshot_watermark"`
	LastSentChunk         uint64             `json:"last_sent_chunk"`
	LastCommittedChunk    uint64             `json:"last_committed_chunk"`
	LastDeltaAppliedSeq   uint64             `json:"last_delta_applied_seq"`
	DeltaMutationsApplied uint64             `json:"delta_mutations_applied"`
	KeysTransferred       uint64             `json:"keys_transferred"`
	BytesTransferred      uint64             `json:"bytes_transferred"`
	CopyCursorDB          int                `json:"copy_cursor_db"`
	CopyCursorKey         []byte             `json:"copy_cursor_key,omitempty"`
	CleanupCursorDB       int                `json:"cleanup_cursor_db"`
	CleanupCursorKey      []byte             `json:"cleanup_cursor_key,omitempty"`
	CleanupDeletedKeys    uint64             `json:"cleanup_deleted_keys"`
	StartedAt             *time.Time         `json:"started_at,omitempty"`
	FinishedAt            *time.Time         `json:"finished_at,omitempty"`
}

func (job *clusterRebalanceJob) snapshot() clusterRebalanceJobResponse {
	job.mu.Lock()
	defer job.mu.Unlock()
	return clusterRebalanceJobResponse{
		JobID:                 job.ID,
		Status:                job.Status,
		Phase:                 job.Phase,
		Error:                 job.Error,
		Move:                  job.Move,
		ChunkEntryLimit:       job.ChunkEntryLimit,
		SnapshotWatermark:     job.SnapshotWatermark,
		LastSentChunk:         job.LastSentChunk,
		LastCommittedChunk:    job.LastCommittedChunk,
		LastDeltaAppliedSeq:   job.LastDeltaAppliedSeq,
		DeltaMutationsApplied: job.DeltaMutationsApplied,
		KeysTransferred:       job.KeysTransferred,
		BytesTransferred:      job.BytesTransferred,
		CopyCursorDB:          job.CopyCursorDB,
		CopyCursorKey:         cloneBytes(job.CopyCursorKey),
		CleanupCursorDB:       job.CleanupCursorDB,
		CleanupCursorKey:      cloneBytes(job.CleanupCursorKey),
		CleanupDeletedKeys:    job.CleanupDeletedKeys,
		StartedAt:             job.StartedAt,
		FinishedAt:            job.FinishedAt,
	}
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

func (srv *Server) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	cluster, state, ok := srv.requireClusterManager(w, r)
	if !ok {
		return
	}
	render.JSON(w, r, clusterStatusResponse{
		ClusterID:   state.ClusterID,
		Version:     state.Version,
		Epoch:       state.Epoch,
		LocalNodeID: cluster.LocalNodeID,
		SlotCount:   state.SlotCount,
		Rebalancing: state.Rebalancing,
		PendingMove: state.PendingMove,
		Nodes:       append([]clusterNodeState(nil), state.Nodes...),
	})
}

func (srv *Server) handleClusterTopology(w http.ResponseWriter, r *http.Request) {
	cluster, state, ok := srv.requireClusterManager(w, r)
	if !ok {
		return
	}
	render.JSON(w, r, clusterTopologyResponse{
		LocalNodeID:      cluster.LocalNodeID,
		TopologyFile:     strings.TrimSpace(srv.Config.Cluster.TopologyFile),
		Topology:         cluster.ConfiguredTopology(),
		RuntimeVersion:   state.Version,
		RuntimeEpoch:     state.Epoch,
		RuntimeSlotCount: state.SlotCount,
		RuntimeName:      state.TopologyName,
		RuntimeHash:      state.TopologyHash,
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
		if err = srv.pushClusterStateToPeers(updatedState); err != nil {
			_ = cluster.ReplaceState(originalState)
			_ = srv.pushClusterStateToPeers(originalState)
			joblessErr := err
			_ = render.Render(w, r, ErrStatus(http.StatusBadGateway, joblessErr.Error()))
			return
		}
	}
	render.JSON(w, r, clusterStatusResponse{
		ClusterID:   updatedState.ClusterID,
		Version:     updatedState.Version,
		Epoch:       updatedState.Epoch,
		LocalNodeID: cluster.LocalNodeID,
		SlotCount:   updatedState.SlotCount,
		Rebalancing: updatedState.Rebalancing,
		PendingMove: updatedState.PendingMove,
		Nodes:       append([]clusterNodeState(nil), updatedState.Nodes...),
	})
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
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
	if err = srv.pushClusterStateToPeers(updatedState); err != nil {
		_ = cluster.ReplaceState(originalState)
		_ = srv.pushClusterStateToPeers(originalState)
		_ = srv.postClusterState(node, originalState)
		_ = render.Render(w, r, ErrStatus(http.StatusBadGateway, err.Error()))
		return
	}

	render.JSON(w, r, clusterStatusResponse{
		ClusterID:   updatedState.ClusterID,
		Version:     updatedState.Version,
		Epoch:       updatedState.Epoch,
		LocalNodeID: cluster.LocalNodeID,
		SlotCount:   updatedState.SlotCount,
		Rebalancing: updatedState.Rebalancing,
		PendingMove: updatedState.PendingMove,
		Nodes:       append([]clusterNodeState(nil), updatedState.Nodes...),
	})
}

func (srv *Server) decodeClusterRebalanceRequest(r *http.Request) (clusterPendingMove, error) {
	var req clusterRebalanceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
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

	job := &clusterRebalanceJob{
		ID:              strconv.FormatInt(time.Now().UnixNano(), 10),
		Status:          clusterRebalanceStatusQueued,
		Phase:           clusterRebalancePhaseCopying,
		Move:            move,
		ChunkEntryLimit: clusterImportBatchEntryLimit,
		CopyCursorDB:    redisDatabaseMin,
		CleanupCursorDB: redisDatabaseMin,
	}
	job.Move.ID = job.ID
	srv.rebalanceMu.Lock()
	for _, existing := range srv.rebalanceJobs {
		if existing.active() {
			srv.rebalanceMu.Unlock()
			_ = render.Render(w, r, ErrStatus(http.StatusConflict, "another rebalance job is already running"))
			return
		}
	}
	srv.rebalanceJobs[job.ID] = job
	srv.rebalanceMu.Unlock()
	if err = srv.persistRebalanceJob(job); err != nil {
		srv.rebalanceMu.Lock()
		delete(srv.rebalanceJobs, job.ID)
		srv.rebalanceMu.Unlock()
		_ = render.Render(w, r, ErrStatus(http.StatusInternalServerError, err.Error()))
		return
	}

	go srv.runClusterRebalanceJob(job)

	render.Status(r, http.StatusAccepted)
	render.JSON(w, r, job.snapshot())
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
	render.JSON(w, r, job.snapshot())
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

func (srv *Server) runClusterRebalanceJob(job *clusterRebalanceJob) {
	job.markRunning()
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
	if state.Rebalancing && state.PendingMove != nil && state.PendingMove.ID == job.Move.ID {
		switch state.PendingMove.Stage {
		case clusterMoveStageCatchup:
			if job.Phase == clusterRebalancePhaseCopying {
				job.mu.Lock()
				job.Phase = clusterRebalancePhaseCatchup
				job.mu.Unlock()
				_ = srv.persistRebalanceJob(job)
			}
		case clusterMoveStageAsking:
			if job.Phase == clusterRebalancePhaseCopying || job.Phase == clusterRebalancePhaseCatchup {
				job.mu.Lock()
				job.Phase = clusterRebalancePhaseAsking
				job.mu.Unlock()
				_ = srv.persistRebalanceJob(job)
			}
		}
	}

	var workingState *clusterState
	var err error
	if job.Phase == clusterRebalancePhaseCopying {
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
			if err = srv.pushClusterStateToPeers(workingState); err != nil {
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
			stats, chunkErr := srv.postClusterSlotImportChunk(
				destinationNode,
				job.ID,
				chunkIndex,
				workingState.SlotCount,
				job.Move.StartSlot,
				job.Move.EndSlot,
				nowMs,
				job.CopyCursorDB,
				job.CopyCursorKey,
				job.ChunkEntryLimit,
			)
			if chunkErr != nil {
				job.markFailed(chunkErr)
				_ = srv.persistRebalanceJob(job)
				return
			}

			job.mu.Lock()
			job.LastCommittedChunk = chunkIndex
			job.KeysTransferred += stats.EntriesTransferred
			job.BytesTransferred += stats.BytesTransferred
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
			if err = srv.pushClusterStateToPeers(catchupState); err != nil {
				_, _ = srv.Cluster.AbortRebalance()
				_ = srv.pushClusterStateToPeers(originalState)
				job.markFailed(err)
				_ = srv.persistRebalanceJob(job)
				return
			}
		}

		destinationNode, _ := catchupState.NodeByID(job.Move.DestinationNodeID)
		for {
			batch, loadErr := loadClusterDeltaBatch(srv.DB, job.Move.ID, job.LastDeltaAppliedSeq, clusterDeltaBatchEntryLimit)
			if loadErr != nil {
				job.markFailed(loadErr)
				_ = srv.persistRebalanceJob(job)
				return
			}
			if len(batch.Mutations) == 0 {
				job.mu.Lock()
				job.Phase = clusterRebalancePhaseAsking
				job.mu.Unlock()
				if err = srv.persistRebalanceJob(job); err != nil {
					job.markFailed(err)
					_ = srv.persistRebalanceJob(job)
					return
				}
				break
			}
			if err = srv.postClusterDeltaBatch(destinationNode, batch); err != nil {
				job.markFailed(err)
				_ = srv.persistRebalanceJob(job)
				return
			}
			job.mu.Lock()
			job.LastDeltaAppliedSeq = batch.LastSeq
			job.DeltaMutationsApplied += uint64(len(batch.Mutations))
			job.mu.Unlock()
			if err = srv.persistRebalanceJob(job); err != nil {
				job.markFailed(err)
				_ = srv.persistRebalanceJob(job)
				return
			}
		}
	}

	if job.Phase == clusterRebalancePhaseAsking {
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
			if err = srv.pushClusterStateToPeers(askingState); err != nil {
				_, _ = srv.Cluster.AbortRebalance()
				_ = srv.pushClusterStateToPeers(originalState)
				job.markFailed(err)
				_ = srv.persistRebalanceJob(job)
				return
			}
		}

		finalState, finishErr := srv.Cluster.FinishRebalance(job.Move)
		if finishErr != nil {
			_, _ = srv.Cluster.AbortRebalance()
			_ = srv.pushClusterStateToPeers(originalState)
			job.markFailed(finishErr)
			_ = srv.persistRebalanceJob(job)
			return
		}
		if err = srv.pushClusterStateToPeers(finalState); err != nil {
			job.markFailed(err)
			_ = srv.persistRebalanceJob(job)
			return
		}
		job.mu.Lock()
		job.Phase = clusterRebalancePhaseCleanup
		job.mu.Unlock()
		if err = srv.persistRebalanceJob(job); err != nil {
			job.markFailed(err)
			_ = srv.persistRebalanceJob(job)
			return
		}
	}

	if job.Phase == clusterRebalancePhaseCleanup {
		finalState := srv.Cluster.Snapshot()
		if finalState == nil {
			job.markFailed(errors.New("cluster state is unavailable"))
			_ = srv.persistRebalanceJob(job)
			return
		}
		for {
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
	job.markDone()
	_ = srv.persistRebalanceJob(job)
}

func (srv *Server) pushClusterStateToPeers(state *clusterState) error {
	for _, node := range state.Nodes {
		if node.ID == srv.Cluster.LocalNodeID {
			continue
		}
		if err := srv.postClusterState(node, state); err != nil {
			return err
		}
	}
	return nil
}

func (srv *Server) postClusterState(node clusterNodeState, state *clusterState) error {
	body, err := json.Marshal(state)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, strings.TrimRight(node.HTTPAddress, "/")+"/api/v1/cluster/internal/state", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		payload, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("state sync to %s failed: %s", node.ID, strings.TrimSpace(string(payload)))
	}
	return nil
}

func (srv *Server) postClusterSlotImportChunk(node clusterNodeState, jobID string, chunkIndex uint64, slotCount int, startSlot int, endSlot int, nowMs int64, cursorDB int, cursorKey []byte, limit int) (clusterSlotTransferStats, error) {
	pr, pw := io.Pipe()
	resultCh := make(chan struct {
		stats clusterSlotTransferStats
		err   error
	}, 1)
	go func() {
		stats, err := streamClusterSlotRangeChunk(srv.DB, pw, slotCount, startSlot, endSlot, nowMs, cursorDB, cursorKey, limit)
		resultCh <- struct {
			stats clusterSlotTransferStats
			err   error
		}{stats: stats, err: err}
		_ = pw.CloseWithError(err)
	}()

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/cluster/internal/slot-import?job_id=%s&chunk_index=%d", strings.TrimRight(node.HTTPAddress, "/"), jobID, chunkIndex), pr)
	if err != nil {
		return clusterSlotTransferStats{}, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return clusterSlotTransferStats{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return clusterSlotTransferStats{}, fmt.Errorf("slot import to %s failed: %s", node.ID, strings.TrimSpace(string(body)))
	}
	var importStats clusterSlotTransferStats
	if err = json.NewDecoder(resp.Body).Decode(&importStats); err != nil {
		return clusterSlotTransferStats{}, err
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
	importStats.EOF = streamResult.stats.EOF
	importStats.NextCursorDB = streamResult.stats.NextCursorDB
	importStats.NextCursorKey = cloneBytes(streamResult.stats.NextCursorKey)
	return importStats, nil
}

func (srv *Server) postClusterDeltaBatch(node clusterNodeState, batch clusterDeltaBatch) error {
	body, err := encodeClusterDeltaBatch(batch)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/cluster/internal/delta-apply", strings.TrimRight(node.HTTPAddress, "/")), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		payload, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delta apply to %s failed: %s", node.ID, strings.TrimSpace(string(payload)))
	}
	return nil
}

func (srv *Server) handleClusterInternalApplyState(w http.ResponseWriter, r *http.Request) {
	cluster, _, ok := srv.requireClusterManager(w, r)
	if !ok {
		return
	}
	var state clusterState
	if err := json.NewDecoder(r.Body).Decode(&state); err != nil {
		_ = render.Render(w, r, ErrInvalidRequest())
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
	_, _, ok := srv.requireClusterManager(w, r)
	if !ok {
		return
	}
	stats, err := importClusterSlotStream(srv.DB, r.Body, redisNowUnixMilli(time.Now()))
	if err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusBadRequest, err.Error()))
		return
	}
	render.JSON(w, r, stats)
}

func (srv *Server) handleClusterInternalDeltaApply(w http.ResponseWriter, r *http.Request) {
	cluster, state, ok := srv.requireClusterManager(w, r)
	if !ok {
		return
	}
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		_ = render.Render(w, r, ErrInvalidRequest())
		return
	}
	batch, err := decodeClusterDeltaBatch(raw)
	if err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusBadRequest, err.Error()))
		return
	}
	if state.Rebalancing && state.PendingMove != nil && batch.MoveID != "" && state.PendingMove.ID != "" && batch.MoveID != state.PendingMove.ID {
		_ = render.Render(w, r, ErrStatus(http.StatusConflict, "delta batch move id does not match active rebalance"))
		return
	}
	if err = applyClusterDeltaBatch(srv.DB, state.SlotCount, batch, redisNowUnixMilli(time.Now())); err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusBadRequest, err.Error()))
		return
	}
	render.JSON(w, r, map[string]any{
		"status":    "ok",
		"node_id":   cluster.LocalNodeID,
		"move_id":   batch.MoveID,
		"last_seq":  batch.LastSeq,
		"mutations": len(batch.Mutations),
	})
}

func (srv *Server) handleClusterInternalSlotDelete(w http.ResponseWriter, r *http.Request) {
	cluster, state, ok := srv.requireClusterManager(w, r)
	if !ok {
		return
	}
	var req clusterRebalanceDeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
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
