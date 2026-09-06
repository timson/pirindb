package controllers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type clusterAPICondition struct {
	Type    string `json:"type"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

type clusterAPINodeStatus struct {
	NodeID               string `json:"node_id"`
	RedisAddress         string `json:"redis_address"`
	HTTPAddress          string `json:"http_address"`
	OwnedSlots           int32  `json:"owned_slots"`
	TargetSlots          int32  `json:"target_slots"`
	ConfiguredInTopology bool   `json:"configured_in_topology"`
	Draining             bool   `json:"draining"`
	Receiving            bool   `json:"receiving"`
}

type clusterAPIStatusResponse struct {
	ClusterID              string                 `json:"cluster_id"`
	Version                uint64                 `json:"version"`
	Epoch                  uint64                 `json:"epoch"`
	LocalNodeID            string                 `json:"local_node_id"`
	RuntimeTopologyName    string                 `json:"runtime_topology_name"`
	RuntimeTopologyHash    string                 `json:"runtime_topology_hash"`
	ConfiguredTopologyName string                 `json:"configured_topology_name"`
	ConfiguredTopologyHash string                 `json:"configured_topology_hash"`
	ConfiguredNodeIDs      []string               `json:"configured_node_ids"`
	Rebalancing            bool                   `json:"rebalancing"`
	Conditions             []clusterAPICondition  `json:"conditions"`
	NodeStatuses           []clusterAPINodeStatus `json:"node_statuses"`
}

type clusterAPIAutoJobResponse struct {
	JobID  string `json:"job_id"`
	Kind   string `json:"kind,omitempty"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type ClusterAdminClient interface {
	GetStatus(ctx context.Context, baseURL string) (*clusterAPIStatusResponse, error)
	ReconcileTopology(ctx context.Context, baseURL string) (*clusterAPIStatusResponse, error)
	StartAutoRebalance(ctx context.Context, baseURL, destinationNodeID, idempotencyKey string) (*clusterAPIAutoJobResponse, error)
	GetAutoRebalanceJob(ctx context.Context, baseURL, jobID string) (*clusterAPIAutoJobResponse, error)
	StartDrain(ctx context.Context, baseURL, nodeID, idempotencyKey string) (*clusterAPIAutoJobResponse, error)
	GetDrainJob(ctx context.Context, baseURL, jobID string) (*clusterAPIAutoJobResponse, error)
	RemoveNode(ctx context.Context, baseURL, nodeID string) error
}

type HTTPClusterAdminClient struct {
	Client *http.Client
	Token  string
}

func (c *HTTPClusterAdminClient) SetBearerToken(token string) {
	c.Token = strings.TrimSpace(token)
}

func NewHTTPClusterAdminClient() *HTTPClusterAdminClient {
	return &HTTPClusterAdminClient{
		Client: &http.Client{
			Timeout:   30 * time.Second,
			Transport: &http.Transport{},
		},
	}
}

func (c *HTTPClusterAdminClient) GetStatus(ctx context.Context, baseURL string) (*clusterAPIStatusResponse, error) {
	var response clusterAPIStatusResponse
	if err := c.doJSON(ctx, http.MethodGet, joinURL(baseURL, "/api/v1/cluster/status"), nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c *HTTPClusterAdminClient) ReconcileTopology(ctx context.Context, baseURL string) (*clusterAPIStatusResponse, error) {
	var response clusterAPIStatusResponse
	if err := c.doJSON(ctx, http.MethodPost, joinURL(baseURL, "/api/v1/cluster/reconcile-topology"), map[string]any{}, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c *HTTPClusterAdminClient) StartAutoRebalance(ctx context.Context, baseURL, destinationNodeID, idempotencyKey string) (*clusterAPIAutoJobResponse, error) {
	var response clusterAPIAutoJobResponse
	if err := c.doJSONWithHeaders(ctx, http.MethodPost, joinURL(baseURL, "/api/v1/cluster/rebalance/auto"), map[string]any{
		"destination_node_id": destinationNodeID,
	}, &response, map[string]string{"Idempotency-Key": idempotencyKey}); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c *HTTPClusterAdminClient) GetAutoRebalanceJob(ctx context.Context, baseURL, jobID string) (*clusterAPIAutoJobResponse, error) {
	var response clusterAPIAutoJobResponse
	if err := c.doJSON(ctx, http.MethodGet, joinURL(baseURL, "/api/v1/cluster/rebalance/auto/"+jobID), nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c *HTTPClusterAdminClient) StartDrain(ctx context.Context, baseURL, nodeID, idempotencyKey string) (*clusterAPIAutoJobResponse, error) {
	var response clusterAPIAutoJobResponse
	if err := c.doJSONWithHeaders(ctx, http.MethodPost, joinURL(baseURL, "/api/v1/cluster/rebalance/drain"), map[string]any{
		"node_id": nodeID,
	}, &response, map[string]string{"Idempotency-Key": idempotencyKey}); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c *HTTPClusterAdminClient) GetDrainJob(ctx context.Context, baseURL, jobID string) (*clusterAPIAutoJobResponse, error) {
	var response clusterAPIAutoJobResponse
	if err := c.doJSON(ctx, http.MethodGet, joinURL(baseURL, "/api/v1/cluster/rebalance/drain/"+jobID), nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c *HTTPClusterAdminClient) RemoveNode(ctx context.Context, baseURL, nodeID string) error {
	return c.doJSON(ctx, http.MethodPost, joinURL(baseURL, "/api/v1/cluster/nodes/remove"), map[string]any{
		"id": nodeID,
	}, nil)
}

func (c *HTTPClusterAdminClient) doJSON(ctx context.Context, method, endpoint string, requestBody any, out any) error {
	return c.doJSONWithHeaders(ctx, method, endpoint, requestBody, out, nil)
}

func (c *HTTPClusterAdminClient) doJSONWithHeaders(ctx context.Context, method, endpoint string, requestBody any, out any, headers map[string]string) error {
	var bodyReader *bytes.Reader
	if requestBody == nil {
		bodyReader = bytes.NewReader(nil)
	} else {
		raw, err := json.Marshal(requestBody)
		if err != nil {
			return err
		}
		bodyReader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, bodyReader)
	if err != nil {
		return err
	}
	if req.URL == nil || req.URL.Scheme != "http" {
		return fmt.Errorf("cluster HTTP client requires an http:// URL")
	}
	if requestBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for name, value := range headers {
		if strings.TrimSpace(value) != "" {
			req.Header.Set(name, value)
		}
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	resp, err := c.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var apiErr struct {
			StatusText string `json:"status"`
			ErrorText  string `json:"error"`
		}
		if decodeErr := json.NewDecoder(io.LimitReader(resp.Body, 1024*1024)).Decode(&apiErr); decodeErr == nil {
			if apiErr.ErrorText != "" {
				return fmt.Errorf("%s %s returned %d: %s", method, endpoint, resp.StatusCode, apiErr.ErrorText)
			}
			if apiErr.StatusText != "" {
				return fmt.Errorf("%s %s returned %d: %s", method, endpoint, resp.StatusCode, apiErr.StatusText)
			}
		}
		return fmt.Errorf("%s %s returned status %d", method, endpoint, resp.StatusCode)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 8*1024*1024)).Decode(out)
}

func joinURL(baseURL, path string) string {
	return strings.TrimRight(baseURL, "/") + path
}
