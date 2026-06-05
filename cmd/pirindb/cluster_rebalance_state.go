package main

import (
	"encoding/json"
	"errors"
	"sort"

	"github.com/timson/pirindb/storage"
)

const (
	clusterRebalanceJobsBucketNameConst = "__pirin_cluster_rebalance_jobs__"

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

func (job *clusterRebalanceJob) clone() *clusterRebalanceJob {
	if job == nil {
		return nil
	}
	job.mu.Lock()
	defer job.mu.Unlock()
	cloned := *job
	return &cloned
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
	return json.Marshal(job.snapshot())
}

func decodeClusterRebalanceJob(raw []byte) (*clusterRebalanceJob, error) {
	var snapshot clusterRebalanceJobResponse
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return nil, err
	}
	job := &clusterRebalanceJob{
		ID:                    snapshot.JobID,
		Status:                snapshot.Status,
		Phase:                 snapshot.Phase,
		Error:                 snapshot.Error,
		Move:                  snapshot.Move,
		ChunkEntryLimit:       snapshot.ChunkEntryLimit,
		SnapshotWatermark:     snapshot.SnapshotWatermark,
		LastSentChunk:         snapshot.LastSentChunk,
		LastCommittedChunk:    snapshot.LastCommittedChunk,
		LastDeltaAppliedSeq:   snapshot.LastDeltaAppliedSeq,
		DeltaMutationsApplied: snapshot.DeltaMutationsApplied,
		KeysTransferred:       snapshot.KeysTransferred,
		BytesTransferred:      snapshot.BytesTransferred,
		CopyCursorDB:          snapshot.CopyCursorDB,
		CopyCursorKey:         cloneBytes(snapshot.CopyCursorKey),
		CleanupCursorDB:       snapshot.CleanupCursorDB,
		CleanupCursorKey:      cloneBytes(snapshot.CleanupCursorKey),
		CleanupDeletedKeys:    snapshot.CleanupDeletedKeys,
		StartedAt:             snapshot.StartedAt,
		FinishedAt:            snapshot.FinishedAt,
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
	if job.ChunkEntryLimit <= 0 {
		job.ChunkEntryLimit = clusterImportBatchEntryLimit
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
		go srv.runClusterRebalanceJob(job)
	}
}
