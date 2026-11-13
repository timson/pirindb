package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/timson/pirindb/storage"
)

const clusterMetricsMaxAge = 30 * time.Second

type clusterSlotMetric struct {
	Slot           int               `json:"slot"`
	LogicalKeys    uint64            `json:"logical_keys"`
	EstimatedBytes uint64            `json:"estimated_bytes"`
	TypeCounts     map[string]uint64 `json:"type_counts,omitempty"`
}

type clusterNodeSlotMetricsResponse struct {
	ClusterID   string              `json:"cluster_id"`
	NodeID      string              `json:"node_id"`
	Epoch       uint64              `json:"epoch"`
	SlotCount   int                 `json:"slot_count"`
	CollectedAt time.Time           `json:"collected_at"`
	Slots       []clusterSlotMetric `json:"slots"`
}

type clusterPlanningMetrics struct {
	CollectedAt    time.Time
	SlotKeyCounts  []int64
	SlotByteCounts []int64
	TypeCounts     []map[string]uint64
}

func collectLocalClusterSlotMetrics(db *storage.DB, state *clusterState, nodeID string, now time.Time) (clusterNodeSlotMetricsResponse, error) {
	if state == nil || state.SlotCount <= 0 {
		return clusterNodeSlotMetricsResponse{}, errors.New("cluster state is unavailable")
	}
	metricsBySlot := make(map[int]*clusterSlotMetric)
	for slot, owner := range state.SlotOwners {
		if owner == nodeID {
			metricsBySlot[slot] = &clusterSlotMetric{Slot: slot, TypeCounts: make(map[string]uint64)}
		}
	}
	nowMs := redisNowUnixMilli(now)
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
				slot, key, decodeErr := decodeRedisSlotIndexKey(indexKey)
				if decodeErr != nil {
					return decodeErr
				}
				metric := metricsBySlot[slot]
				if metric == nil {
					continue
				}
				keyType, typeErr := redisKeyTypeTx(tx, ns, key, nowMs)
				if typeErr != nil {
					return typeErr
				}
				if keyType == redisKeyTypeNone {
					continue
				}
				estimatedBytes, sizeErr := estimateRedisKeyLogicalBytesTx(tx, ns, key, keyType, nowMs)
				if sizeErr != nil {
					return sizeErr
				}
				metric.LogicalKeys++
				metric.EstimatedBytes += estimatedBytes
				metric.TypeCounts[keyType]++
			}
		}
		return nil
	})
	if err != nil {
		return clusterNodeSlotMetricsResponse{}, err
	}
	slots := make([]clusterSlotMetric, 0, len(metricsBySlot))
	for slot := 0; slot < state.SlotCount; slot++ {
		if metric := metricsBySlot[slot]; metric != nil {
			slots = append(slots, *metric)
		}
	}
	return clusterNodeSlotMetricsResponse{
		ClusterID:   state.ClusterID,
		NodeID:      nodeID,
		Epoch:       state.Epoch,
		SlotCount:   state.SlotCount,
		CollectedAt: now.UTC(),
		Slots:       slots,
	}, nil
}

func estimateRedisKeyLogicalBytesTx(tx *storage.Tx, ns redisNamespace, key []byte, keyType string, nowMs int64) (uint64, error) {
	total := uint64(len(key))
	addBucket := func(bucket *redisObjectBucket) error {
		cursor := bucket.Cursor()
		for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
			length, found, err := bucket.ValueLen(key)
			if err != nil {
				return err
			}
			if found {
				total += uint64(len(key) + length)
			}
		}
		return cursor.Err()
	}
	switch keyType {
	case redisKeyTypeString:
		bucket, err := tx.GetBucket(ns.stringBucket)
		if err != nil {
			return 0, err
		}
		length, found, err := bucket.ValueLen(key)
		if err != nil {
			return 0, err
		}
		if !found {
			return 0, errors.New("string disappeared while collecting cluster metrics")
		}
		total += uint64(length)
	case redisKeyTypeList:
		meta, found, err := loadRedisListMetaForReadTx(tx, ns, key, nowMs)
		if err != nil || !found {
			return 0, err
		}
		total += redisListMetaSize
		bucket, err := openRedisObjectBucketTx(tx, ns.listDataBucket, redisListBucketName(ns, meta.ID), meta.ID, meta.StorageFormat, false)
		if err != nil {
			return 0, err
		}
		if err = addBucket(bucket); err != nil {
			return 0, err
		}
	case redisKeyTypeHash:
		meta, found, err := loadRedisHashMetaForReadTx(tx, ns, key, nowMs)
		if err != nil || !found {
			return 0, err
		}
		total += redisHashMetaSize
		bucket, err := openRedisObjectBucketTx(tx, ns.hashDataBucket, redisHashBucketName(ns, meta.ID), meta.ID, meta.StorageFormat, false)
		if err != nil {
			return 0, err
		}
		if err = addBucket(bucket); err != nil {
			return 0, err
		}
	case redisKeyTypeZSet:
		members, scores, _, found, err := getRedisZSetBucketsTx(tx, ns, key, nowMs)
		if err != nil || !found {
			return 0, err
		}
		total += redisZSetMetaSize
		if err = addBucket(members); err != nil {
			return 0, err
		}
		if err = addBucket(scores); err != nil {
			return 0, err
		}
	case redisKeyTypeBloom:
		meta, found, err := loadRedisBloomMetaForReadTx(tx, ns, key, nowMs)
		if err != nil || !found {
			return 0, err
		}
		total += uint64(len(meta.serialize()))
		bucket, err := getRedisBloomDataBucketTx(tx, ns, meta)
		if err != nil {
			return 0, err
		}
		if err = addBucket(bucket); err != nil {
			return 0, err
		}
	case redisKeyTypeTopK:
		meta, found, err := loadRedisTopKMetaForReadTx(tx, ns, key, nowMs)
		if err != nil || !found {
			return 0, err
		}
		total += uint64(len(meta.serialize()))
		bucket, err := getRedisTopKDataBucketTx(tx, ns, meta)
		if err != nil {
			return 0, err
		}
		if err = addBucket(bucket); err != nil {
			return 0, err
		}
	default:
		return 0, fmt.Errorf("unsupported redis type %q while collecting cluster metrics", keyType)
	}
	return total, nil
}

func (srv *Server) collectClusterPlanningMetrics(ctx context.Context, state *clusterState) (clusterPlanningMetrics, error) {
	if state == nil || srv.Cluster == nil {
		return clusterPlanningMetrics{}, errors.New("cluster state is unavailable")
	}
	type result struct {
		response clusterNodeSlotMetricsResponse
		err      error
	}
	results := make(chan result, len(state.Nodes))
	var wg sync.WaitGroup
	for _, node := range state.Nodes {
		node := node
		wg.Add(1)
		go func() {
			defer wg.Done()
			if node.ID == srv.Cluster.LocalNodeID {
				response, err := collectLocalClusterSlotMetrics(srv.DB, state, node.ID, time.Now())
				results <- result{response: response, err: err}
				return
			}
			requestCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, node.HTTPAddress+"/api/v1/cluster/internal/slot-metrics", nil)
			if err != nil {
				results <- result{err: err}
				return
			}
			resp, err := srv.clusterHTTPClient.Do(req)
			if err != nil {
				results <- result{err: fmt.Errorf("slot metrics from %s: %w", node.ID, err)}
				return
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
				results <- result{err: fmt.Errorf("slot metrics from %s failed: %s", node.ID, body)}
				return
			}
			var response clusterNodeSlotMetricsResponse
			if err = json.NewDecoder(io.LimitReader(resp.Body, 32*1024*1024)).Decode(&response); err != nil {
				results <- result{err: fmt.Errorf("slot metrics from %s: %w", node.ID, err)}
				return
			}
			results <- result{response: response}
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	metrics := clusterPlanningMetrics{
		CollectedAt:    time.Now().UTC(),
		SlotKeyCounts:  make([]int64, state.SlotCount),
		SlotByteCounts: make([]int64, state.SlotCount),
		TypeCounts:     make([]map[string]uint64, state.SlotCount),
	}
	reported := make([]bool, state.SlotCount)
	nodeResponses := make(map[string]struct{}, len(state.Nodes))
	now := time.Now()
	for item := range results {
		if item.err != nil {
			return clusterPlanningMetrics{}, item.err
		}
		response := item.response
		if response.ClusterID != state.ClusterID || response.Epoch != state.Epoch || response.SlotCount != state.SlotCount {
			return clusterPlanningMetrics{}, fmt.Errorf("slot metrics from %s use stale cluster identity or epoch", response.NodeID)
		}
		if _, exists := nodeResponses[response.NodeID]; exists {
			return clusterPlanningMetrics{}, fmt.Errorf("duplicate slot metrics response from %s", response.NodeID)
		}
		if _, exists := state.NodeByID(response.NodeID); !exists {
			return clusterPlanningMetrics{}, fmt.Errorf("slot metrics came from unknown node %s", response.NodeID)
		}
		if response.CollectedAt.IsZero() || now.Sub(response.CollectedAt) > clusterMetricsMaxAge || response.CollectedAt.After(now.Add(5*time.Second)) {
			return clusterPlanningMetrics{}, fmt.Errorf("slot metrics from %s are stale", response.NodeID)
		}
		if response.CollectedAt.Before(metrics.CollectedAt) {
			metrics.CollectedAt = response.CollectedAt
		}
		nodeResponses[response.NodeID] = struct{}{}
		for _, metric := range response.Slots {
			if metric.Slot < 0 || metric.Slot >= state.SlotCount || state.SlotOwners[metric.Slot] != response.NodeID {
				return clusterPlanningMetrics{}, fmt.Errorf("node %s reported metrics for a slot it does not own", response.NodeID)
			}
			if reported[metric.Slot] {
				return clusterPlanningMetrics{}, fmt.Errorf("slot %d metrics were reported more than once", metric.Slot)
			}
			reported[metric.Slot] = true
			metrics.SlotKeyCounts[metric.Slot] = int64(metric.LogicalKeys)
			metrics.SlotByteCounts[metric.Slot] = int64(metric.EstimatedBytes)
			metrics.TypeCounts[metric.Slot] = metric.TypeCounts
		}
	}
	if len(nodeResponses) != len(state.Nodes) {
		return clusterPlanningMetrics{}, errors.New("cluster-wide slot metrics are incomplete")
	}
	for slot, owner := range state.SlotOwners {
		if owner != "" && !reported[slot] {
			return clusterPlanningMetrics{}, fmt.Errorf("slot %d has no metrics from owner %s", slot, owner)
		}
	}
	return metrics, nil
}

func (srv *Server) handleClusterInternalSlotMetrics(w http.ResponseWriter, r *http.Request) {
	cluster, state, ok := srv.requireClusterManager(w, r)
	if !ok {
		return
	}
	response, err := collectLocalClusterSlotMetrics(srv.DB, state, cluster.LocalNodeID, time.Now())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}
