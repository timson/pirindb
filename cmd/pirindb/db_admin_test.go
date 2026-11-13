package main

import (
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestDatabaseAdministrationRequiresCredentials(t *testing.T) {
	srv, _, _ := setupTestServer(t)
	router := srv.buildRouter()
	for _, path := range []string{"/api/v1/db/import", "/api/v1/db/export", "/api/v1/db/jobs/private"} {
		req := httptest.NewRequest(http.MethodPost, path, nil)
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		require.Equal(t, http.StatusUnauthorized, rr.Code)
	}
	srv.Config.DB.AdminToken = ""
	req := httptest.NewRequest(http.MethodPost, "/api/v1/db/import", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	require.Equal(t, http.StatusServiceUnavailable, rr.Code)
}
func TestDatabaseTransferConfinementAndRestoreAdmission(t *testing.T) {
	srv, _, _ := setupTestServer(t)
	srv.Config.DB.BackupDirectory = t.TempDir()
	require.Error(t, srv.validateDBTransferRequest(&dbTransferRequest{Scope: dbTransferScopeDB, Path: filepath.Join(t.TempDir(), "escape")}, true))
	req := &dbTransferRequest{Scope: dbTransferScopeDB, Path: filepath.Join(srv.Config.DB.BackupDirectory, "snapshot")}
	require.NoError(t, srv.validateDBTransferRequest(req, true))
	srv.Config.DB.RestoreEnabled = false
	require.ErrorContains(t, srv.validateDBTransferRequest(req, false), "restore requires")
	srv.Config.DB.RestoreEnabled = true
	srv.Config.Redis = &RedisConfig{Enabled: true}
	require.ErrorContains(t, srv.validateDBTransferRequest(req, false), "restore requires")
	srv.Config.Redis.Enabled = false
	srv.Config.Cluster = &ClusterConfig{Enabled: true}
	require.ErrorContains(t, srv.validateDBTransferRequest(req, false), "restore requires")
}
