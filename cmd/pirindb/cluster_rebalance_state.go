package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/timson/pirindb/storage"
)

const (
	clusterRebalanceJobsBucketNameConst = "__pirin_cluster_rebalance_jobs__"
	clusterRebalanceJobSchemaVersion    = 1

	clusterRebalanceStatusQueued  = "queued"
	clusterRebalanceStatusRunning = "running"
	clusterRebalanceStatusDone    = "done"
	clusterRebalanceStatusFailed  = "failed"

	clusterRebalancePhaseCopying = "copying"
	clusterRebalancePhaseCatchup = "catchup"
	clusterRebalancePhaseAsking  = "asking"
	clusterRebalancePhaseCleanup = "cleanup"
	clusterRebalancePhaseDone    = "done"
)

func clusterRebalanceJobsBucketName() []byte {
	return []byte(clusterRebalanceJobsBucketNameConst)
}

func (job *clusterRebalanceJob) active() bool {
	if job == nil {
		return false
	}
	job.mu.Lock()
	defer job.mu.Unlock()
	return job.Status == clusterRebalanceStatusQueued || job.Status == clusterRebalanceStatusRunning
}

func encodeClusterRebalanceJob(job *clusterRebalanceJob) ([]byte, error) {
	if err := normalizeClusterRebalanceJob(job); err != nil {
		return nil, err
	}
	return json.Marshal(job.snapshot())
}

func normalizeClusterRebalanceJob(job *clusterRebalanceJob) error {
	if job == nil {
		return errors.New("rebalance job is required")
	}
	job.mu.Lock()
	defer job.mu.Unlock()
	if job.SchemaVersion == 0 {
		job.SchemaVersion = clusterRebalanceJobSchemaVersion
	}
	if job.SchemaVersion != clusterRebalanceJobSchemaVersion {
		return fmt.Errorf("unsupported rebalance job schema version %d", job.SchemaVersion)
	}
	if job.ChunkEntryLimit < 0 {
		return errors.New("rebalance legacy chunk entry limit is invalid")
	}
	if job.ChunkTargetBytes <= 0 {
		job.ChunkTargetBytes = clusterTransferDefaultTargetChunkBytes
	}
	if job.ChunkMaxBytes <= 0 {
		job.ChunkMaxBytes = clusterTransferDefaultMaxChunkBytes
	}
	if job.ChunkMaxBytes < job.ChunkTargetBytes || job.ChunkMaxBytes > clusterTransferMaxConfiguredChunkBytes {
		return errors.New("rebalance chunk byte limits are invalid")
	}
	if job.ChunkMaxRecords <= 0 {
		job.ChunkMaxRecords = clusterTransferDefaultMaxChunkRecords
	}
	if job.ChunkMaxRecords > clusterTransferMaxConfiguredRecords {
		return errors.New("rebalance chunk record limit is invalid")
	}
	return nil
}

func decodeClusterRebalanceJob(raw []byte) (*clusterRebalanceJob, error) {
	var snapshot clusterRebalanceJobResponse
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return nil, err
	}
	job := &clusterRebalanceJob{
		SchemaVersion:             snapshot.SchemaVersion,
		ID:                        snapshot.JobID,
		Status:                    snapshot.Status,
		Phase:                     snapshot.Phase,
		Error:                     snapshot.Error,
		Move:                      snapshot.Move,
		ChunkEntryLimit:           snapshot.ChunkEntryLimit,
		ChunkTargetBytes:          snapshot.ChunkTargetBytes,
		ChunkMaxBytes:             snapshot.ChunkMaxBytes,
		ChunkMaxRecords:           snapshot.ChunkMaxRecords,
		SnapshotWatermark:         snapshot.SnapshotWatermark,
		EstimateInitialized:       snapshot.EstimateInitialized,
		EstimatedTotalKeys:        snapshot.EstimatedTotalKeys,
		LastSentChunk:             snapshot.LastSentChunk,
		LastCommittedChunk:        snapshot.LastCommittedChunk,
		LastDeltaAppliedSeq:       snapshot.LastDeltaAppliedSeq,
		DeltaMutationsApplied:     snapshot.DeltaMutationsApplied,
		KeysTransferred:           snapshot.KeysTransferred,
		BytesTransferred:          snapshot.BytesTransferred,
		RecordsTransferred:        snapshot.RecordsTransferred,
		OversizedLogicalKeyChunks: snapshot.OversizedLogicalKeyChunks,
		RetryCount:                snapshot.RetryCount,
		LastRetryReason:           snapshot.LastRetryReason,
		CopyCursorDB:              snapshot.CopyCursorDB,
		CopyCursorKey:             cloneBytes(snapshot.CopyCursorKey),
		CleanupCursorDB:           snapshot.CleanupCursorDB,
		CleanupCursorKey:          cloneBytes(snapshot.CleanupCursorKey),
		CleanupDeletedKeys:        snapshot.CleanupDeletedKeys,
		StartedAt:                 snapshot.StartedAt,
		FinishedAt:                snapshot.FinishedAt,
	}
	if job.ID == "" {
		return nil, errors.New("rebalance job id is required")
	}
	if job.Status == "" {
		job.Status = clusterRebalanceStatusQueued
	}
	if job.Phase == "" {
		job.Phase = clusterRebalancePhaseCopying
	}
	if err := normalizeClusterRebalanceJob(job); err != nil {
		return nil, err
	}
	if job.CopyCursorDB == 0 {
		job.CopyCursorDB = redisDatabaseMin
	}
	if job.CleanupCursorDB == 0 {
		job.CleanupCursorDB = redisDatabaseMin
	}
	return job, nil
}

func (srv *Server) persistRebalanceJob(job *clusterRebalanceJob) error {
	if srv == nil || srv.DB == nil || job == nil {
		return nil
	}
	encoded, err := encodeClusterRebalanceJob(job)
	if err != nil {
		return err
	}
	return srv.DB.Update(func(tx *storage.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(clusterRebalanceJobsBucketName())
		if err != nil {
			return err
		}
		return bucket.Put([]byte(job.ID), encoded)
	})
}

func (srv *Server) loadPersistedRebalanceJobs() error {
	if srv == nil || srv.DB == nil {
		return nil
	}
	jobs := make(map[string]*clusterRebalanceJob)
	err := srv.DB.View(func(tx *storage.Tx) error {
		bucket, err := tx.GetBucket(clusterRebalanceJobsBucketName())
		if errors.Is(err, storage.ErrBucketNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return bucket.ForEach(func(k, v []byte) error {
			job, err := decodeClusterRebalanceJob(v)
			if err != nil {
				return err
			}
			jobs[string(k)] = job
			return nil
		})
	})
	if err != nil {
		return err
	}
	srv.rebalanceMu.Lock()
	srv.rebalanceJobs = jobs
	srv.rebalanceMu.Unlock()
	return nil
}

func (srv *Server) resumePersistedRebalanceJobs() {
	if srv == nil || srv.Cluster == nil {
		return
	}
	srv.rebalanceMu.Lock()
	jobs := make([]*clusterRebalanceJob, 0, len(srv.rebalanceJobs))
	for _, job := range srv.rebalanceJobs {
		if job.active() && job.Move.SourceNodeID == srv.Cluster.LocalNodeID {
			jobs = append(jobs, job)
		}
	}
	srv.rebalanceMu.Unlock()
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].ID < jobs[j].ID
	})
	for _, job := range jobs {
		job := job
		if err := srv.startBackgroundJob(func(ctx context.Context) {
			srv.runClusterRebalanceJob(ctx, job)
		}); err != nil && srv.Logger != nil {
			srv.Logger.Error("failed to resume rebalance job", "job_id", job.ID, "error", err)
		}
	}
}
