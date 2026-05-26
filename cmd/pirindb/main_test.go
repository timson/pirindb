package main

import (
	"bytes"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/timson/pirindb/storage"
)

func setupTestServer(t *testing.T) (*Server, string, string) {
	t.Helper()
	dir := t.TempDir()
	filename := filepath.Join(dir, "test.db")

	cfg := &Config{
		Server: &ServerConfig{
			Host:     "127.0.0.1",
			Port:     0, // random port
			LogLevel: "ERROR",
		},
		DB: &DatabaseConfig{
			Filename:               filename,
			SyncPolicy:             "strict",
			CheckpointTxThreshold:  64,
			GroupCommitTxThreshold: 16,
			GroupCommitWindowMs:    1,
		},
	}

	logger := createLogger(cfg.Server.LogLevel)
	storage.SetLogger(logger)

	db, err := storage.Open(filename, cfg.DB.StorageOptions())
	if err != nil {
		t.Fatalf("failed to open DB: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	return NewServer(cfg, db, logger), filename, db.GetOptions().TxLogPath
}

func TestCRUD(t *testing.T) {
	srv, filename, txLogPath := setupTestServer(t)
	t.Cleanup(func() {
		_ = os.Remove(filename)
		_ = os.Remove(txLogPath)
	})

	router := srv.buildRouter()
	ts := httptest.NewServer(router)
	defer ts.Close()

	key := "foo"
	value := "bar"

	// PUT
	resp, err := http.Post(ts.URL+"/api/v1/kv/"+key, "text/plain", bytes.NewBufferString(value))
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	_ = resp.Body.Close()

	// GET
	resp, err = http.Get(ts.URL + "/api/v1/kv/" + key)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var getResp GetResponse
	err = json.NewDecoder(resp.Body).Decode(&getResp)
	require.NoError(t, err)
	require.Equal(t, value, getResp.Value)

	// DELETE
	req, _ := http.NewRequest("DELETE", ts.URL+"/api/v1/kv/"+key, nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	// GET again — expect 404
	resp, err = http.Get(ts.URL + "/api/v1/kv/" + key)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	// Check status, if our bucket exists, and amount of pages > 0
	resp, err = http.Get(ts.URL + "/api/v1/db/status")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var status storage.DBStat
	err = json.NewDecoder(resp.Body).Decode(&status)
	require.NoError(t, err)
	require.Contains(t, status.Buckets, "main")
	require.Greater(t, status.TotalPageNum, 0)
}

func TestHealthCheck(t *testing.T) {
	srv, filename, txLogPath := setupTestServer(t)
	t.Cleanup(func() {
		_ = os.Remove(filename)
		_ = os.Remove(txLogPath)
	})

	router := srv.buildRouter()
	ts := httptest.NewServer(router)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/health")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var healthResponse HealthResponse
	err = json.NewDecoder(resp.Body).Decode(&healthResponse)
	require.NoError(t, err)
	require.Equal(t, "ok", healthResponse.Status)
}
