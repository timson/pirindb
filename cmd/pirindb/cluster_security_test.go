package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClusterControlPlaneAuthenticationAndPublicStatus(t *testing.T) {
	cfg := newTestClusterConfig(t, "a", filepath.Join(t.TempDir(), "secure.db"))
	cfg.Cluster.RequireAuth = true
	cfg.Cluster.PublicStatus = true
	cfg.Cluster.AdminToken = "a-long-test-control-plane-token"
	srv := openTestClusterServer(t, cfg)
	httpServer := newLocalTestHTTPServer(t, srv.buildRouter())

	statusResp, err := http.Get(httpServer.URL + "/api/v1/cluster/status")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, statusResp.StatusCode)
	_ = statusResp.Body.Close()

	requestBody, err := json.Marshal(clusterRebalanceRequest{SourceNodeID: "a", DestinationNodeID: "b", StartSlot: 0, EndSlot: 0})
	require.NoError(t, err)
	anonymousResp, err := http.Post(httpServer.URL+"/api/v1/cluster/rebalance/plan", "application/json", bytes.NewReader(requestBody))
	require.NoError(t, err)
	require.Equal(t, http.StatusUnauthorized, anonymousResp.StatusCode)
	_ = anonymousResp.Body.Close()

	request, err := http.NewRequest(http.MethodPost, httpServer.URL+"/api/v1/cluster/rebalance/plan", bytes.NewReader(requestBody))
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer wrong-token")
	wrongResp, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusUnauthorized, wrongResp.StatusCode)
	_ = wrongResp.Body.Close()

	request, err = http.NewRequest(http.MethodPost, httpServer.URL+"/api/v1/cluster/rebalance/plan", bytes.NewReader(requestBody))
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer "+cfg.Cluster.AdminToken)
	authorizedResp, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, authorizedResp.StatusCode)
	_ = authorizedResp.Body.Close()

	internalResp, err := http.Get(httpServer.URL + "/api/v1/cluster/internal/slot-metrics")
	require.NoError(t, err)
	require.Equal(t, http.StatusUnauthorized, internalResp.StatusCode)
	_ = internalResp.Body.Close()
}

func TestClusterHTTPTransportRejectsHTTPS(t *testing.T) {
	request, err := http.NewRequest(http.MethodGet, "https://node-a:4321/api/v1/cluster/status", nil)
	require.NoError(t, err)
	_, err = (&clusterAuthTransport{base: http.DefaultTransport}).RoundTrip(request)
	require.ErrorContains(t, err, "requires an http:// URL")
}

func TestClusterControlPlaneRejectsUnknownAndOversizedJSON(t *testing.T) {
	cfg := newTestClusterConfig(t, "a", filepath.Join(t.TempDir(), "limits.db"))
	cfg.Cluster.RequireAuth = true
	cfg.Cluster.AdminToken = "a-long-test-control-plane-token"
	srv := openTestClusterServer(t, cfg)
	httpServer := newLocalTestHTTPServer(t, srv.buildRouter())

	request, err := http.NewRequest(http.MethodPost, httpServer.URL+"/api/v1/cluster/nodes/remove", bytes.NewBufferString(`{"id":"b","unexpected":true}`))
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer "+cfg.Cluster.AdminToken)
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, response.StatusCode)
	_ = response.Body.Close()
	_, stillPresent := srv.Cluster.Snapshot().NodeByID("b")
	require.True(t, stillPresent)

	oversized := bytes.Repeat([]byte("x"), int(clusterMaxJSONRequestBytes)+1)
	request, err = http.NewRequest(http.MethodPost, httpServer.URL+"/api/v1/cluster/nodes/remove", bytes.NewReader(oversized))
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer "+cfg.Cluster.AdminToken)
	response, err = http.DefaultClient.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, response.StatusCode)
	_ = response.Body.Close()
	_, stillPresent = srv.Cluster.Snapshot().NodeByID("b")
	require.True(t, stillPresent)
}

func TestClusterAdminJobCreationRateLimit(t *testing.T) {
	srv := &Server{}
	for index := 0; index < clusterAdminStartsPerWindow; index++ {
		require.True(t, srv.allowClusterAdminJobStart())
	}
	require.False(t, srv.allowClusterAdminJobStart())
}
