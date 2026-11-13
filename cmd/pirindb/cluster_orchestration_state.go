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
	clusterOrchestrationJobsBucketNameConst = "__pirin_cluster_orchestration_jobs__"
	clusterOrchestrationSchemaVersion       = 1
)

func clusterOrchestrationJobsBucketName() []byte {
	return []byte(clusterOrchestrationJobsBucketNameConst)
}

func encodeClusterOrchestrationJob(job *clusterAutoRebalanceJob) ([]byte, error) {
	if job == nil {
		return nil, errors.New("orchestration job is required")
	}
	return json.Marshal(job.snapshot())
}

func decodeClusterOrchestrationJob(raw []byte) (*clusterAutoRebalanceJob, error) {
	var snapshot clusterAutoRebalanceJobResponse
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return nil, err
	}
	if snapshot.JobID == "" {
		return nil, errors.New("orchestration job id is required")
	}
	if snapshot.SchemaVersion == 0 {
		snapshot.SchemaVersion = clusterOrchestrationSchemaVersion
	}
	if snapshot.SchemaVersion != clusterOrchestrationSchemaVersion {
		return nil, fmt.Errorf("unsupported orchestration job schema version %d", snapshot.SchemaVersion)
	}
	if snapshot.Status == "" {
		snapshot.Status = clusterRebalanceStatusQueued
	}
	job := &clusterAutoRebalanceJob{
		SchemaVersion:  snapshot.SchemaVersion,
		ID:             snapshot.JobID,
		IdempotencyKey: snapshot.IdempotencyKey,
		RequestedNode:  snapshot.RequestedNode,
		ClusterID:      snapshot.ClusterID,
		PlanEpoch:      snapshot.PlanEpoch,
		CurrentStep:    snapshot.CurrentStep,
		Kind:           snapshot.Kind,
		Status:         snapshot.Status,
		Error:          snapshot.Error,
		ErrorCode:      snapshot.ErrorCode,
		Plan:           snapshot.Plan,
		ScaleRequest:   snapshot.ScaleRequest,
		DrainRequest:   snapshot.DrainRequest,
		Steps:          append([]clusterAutoRebalanceStep(nil), snapshot.Steps...),
		StartedAt:      cloneTimePointer(snapshot.StartedAt),
		UpdatedAt:      cloneTimePointer(snapshot.UpdatedAt),
		FinishedAt:     cloneTimePointer(snapshot.FinishedAt),
	}
	if job.CurrentStep < 0 || job.CurrentStep > len(job.Steps) {
		return nil, errors.New("orchestration current step is invalid")
	}
	return job, nil
}

func (srv *Server) persistClusterOrchestrationJob(job *clusterAutoRebalanceJob) error {
	if srv == nil || srv.DB == nil || job == nil {
		return nil
	}
	encoded, err := encodeClusterOrchestrationJob(job)
	if err != nil {
		return err
	}
	return srv.DB.Update(func(tx *storage.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(clusterOrchestrationJobsBucketName())
		if err != nil {
			return err
		}
		return bucket.Put([]byte(job.ID), encoded)
	})
}

func (srv *Server) loadPersistedClusterOrchestrationJobs() error {
	if srv == nil || srv.DB == nil {
		return nil
	}
	jobs := make(map[string]*clusterAutoRebalanceJob)
	err := srv.DB.View(func(tx *storage.Tx) error {
		bucket, err := tx.GetBucket(clusterOrchestrationJobsBucketName())
		if errors.Is(err, storage.ErrBucketNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return bucket.ForEach(func(key, value []byte) error {
			job, err := decodeClusterOrchestrationJob(value)
			if err != nil {
				return fmt.Errorf("decode orchestration job %q: %w", string(key), err)
			}
			jobs[job.ID] = job
			return nil
		})
	})
	if err != nil {
		return err
	}
	srv.autoMu.Lock()
	srv.autoJobs = jobs
	srv.autoMu.Unlock()
	return nil
}

func (srv *Server) resumePersistedClusterOrchestrationJobs() {
	if srv == nil || srv.Cluster == nil {
		return
	}
	srv.autoMu.Lock()
	jobs := make([]*clusterAutoRebalanceJob, 0, len(srv.autoJobs))
	for _, job := range srv.autoJobs {
		if job.active() {
			jobs = append(jobs, job)
		}
	}
	srv.autoMu.Unlock()
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].ID < jobs[j].ID })
	for _, job := range jobs {
		job := job
		if err := srv.startBackgroundJob(func(ctx context.Context) {
			srv.runClusterAutoRebalanceJob(ctx, job)
		}); err != nil && srv.Logger != nil {
			srv.Logger.Error("failed to resume orchestration job", "job_id", job.ID, "error", err)
		}
	}
}

func (srv *Server) orchestrationJobByIdempotencyKey(kind, key, requestedNode string) (*clusterAutoRebalanceJob, error) {
	if key == "" {
		return nil, nil
	}
	srv.autoMu.Lock()
	defer srv.autoMu.Unlock()
	for _, job := range srv.autoJobs {
		snapshot := job.snapshot()
		if snapshot.IdempotencyKey != key {
			continue
		}
		if snapshot.Kind != kind || snapshot.RequestedNode != requestedNode {
			return nil, errors.New("idempotency key was already used for a different orchestration request")
		}
		return job, nil
	}
	return nil, nil
}

func (srv *Server) registerClusterOrchestrationJob(job *clusterAutoRebalanceJob) error {
	if job == nil {
		return errors.New("orchestration job is required")
	}
	srv.autoMu.Lock()
	defer srv.autoMu.Unlock()
	for _, existing := range srv.autoJobs {
		if existing.active() {
			return errors.New("another auto rebalance job is already running")
		}
	}
	if err := srv.persistClusterOrchestrationJob(job); err != nil {
		return err
	}
	srv.autoJobs[job.ID] = job
	return nil
}
