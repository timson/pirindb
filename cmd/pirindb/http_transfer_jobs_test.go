package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/timson/pirindb/storage"
)

func TestHTTPExportImportDBJob(t *testing.T) {
	srv, _, _ := setupTestServer(t)
	router := srv.buildRouter()
	ts := httptest.NewServer(router)
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/api/v1/kv/foo", "text/plain", bytes.NewBufferString("bar"))
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	_ = resp.Body.Close()

	err = srv.DB.Update(func(tx *storage.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("users"))
		if err != nil {
			return err
		}
		return bucket.Put([]byte("1"), []byte("alice"))
	})
	require.NoError(t, err)

	exportPath := filepath.Join(t.TempDir(), "full.snapshot")
	exportResp := submitDBTransferJob(t, ts, "/api/v1/db/export", dbTransferRequest{
		Path:      exportPath,
		Scope:     dbTransferScopeDB,
		Overwrite: true,
	})
	require.Equal(t, dbTransferStatusQueued, exportResp.Status)
	exportResp = waitForDBTransferJob(t, ts, exportResp.JobID)
	require.Equal(t, dbTransferStatusDone, exportResp.Status)
	require.Equal(t, uint64(2), exportResp.Stats.BucketsExported)

	err = srv.DB.Update(func(tx *storage.Tx) error {
		for _, bucketName := range tx.Buckets() {
			if err := tx.DeleteBucket(bucketName); err != nil {
				return err
			}
		}
		stale, err := tx.CreateBucketIfNotExists([]byte("stale"))
		if err != nil {
			return err
		}
		return stale.Put([]byte("old"), []byte("value"))
	})
	require.NoError(t, err)

	importResp := submitDBTransferJob(t, ts, "/api/v1/db/import", dbTransferRequest{
		Path:  exportPath,
		Scope: dbTransferScopeDB,
	})
	require.Equal(t, dbTransferStatusQueued, importResp.Status)
	importResp = waitForDBTransferJob(t, ts, importResp.JobID)
	require.Equal(t, dbTransferStatusDone, importResp.Status)
	require.Equal(t, uint64(2), importResp.Stats.BucketsImported)

	resp, err = http.Get(ts.URL + "/api/v1/kv/foo")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var getResp GetResponse
	err = json.NewDecoder(resp.Body).Decode(&getResp)
	require.NoError(t, err)
	require.Equal(t, "bar", getResp.Value)

	err = srv.DB.View(func(tx *storage.Tx) error {
		bucket, err := tx.GetBucket([]byte("users"))
		require.NoError(t, err)
		value, found := bucket.Get([]byte("1"))
		require.True(t, found)
		require.Equal(t, []byte("alice"), value)
		_, err = tx.GetBucket([]byte("stale"))
		require.ErrorIs(t, err, storage.ErrBucketNotFound)
		return nil
	})
	require.NoError(t, err)
}

func TestHTTPExportImportBucketJob(t *testing.T) {
	srv, _, _ := setupTestServer(t)
	router := srv.buildRouter()
	ts := httptest.NewServer(router)
	defer ts.Close()

	err := srv.DB.Update(func(tx *storage.Tx) error {
		users, err := tx.CreateBucketIfNotExists([]byte("users"))
		if err != nil {
			return err
		}
		if err = users.Put([]byte("1"), []byte("alice")); err != nil {
			return err
		}
		if err = users.Put([]byte("2"), []byte("bob")); err != nil {
			return err
		}

		other, err := tx.CreateBucketIfNotExists([]byte("other"))
		if err != nil {
			return err
		}
		return other.Put([]byte("safe"), []byte("value"))
	})
	require.NoError(t, err)

	exportPath := filepath.Join(t.TempDir(), "users.snapshot")
	exportResp := submitDBTransferJob(t, ts, "/api/v1/db/export", dbTransferRequest{
		Path:      exportPath,
		Scope:     dbTransferScopeBucket,
		Bucket:    "users",
		Overwrite: true,
	})
	exportResp = waitForDBTransferJob(t, ts, exportResp.JobID)
	require.Equal(t, dbTransferStatusDone, exportResp.Status)

	err = srv.DB.Update(func(tx *storage.Tx) error {
		restored, err := tx.CreateBucketIfNotExists([]byte("restored"))
		if err != nil {
			return err
		}
		return restored.Put([]byte("legacy"), []byte("old"))
	})
	require.NoError(t, err)

	importResp := submitDBTransferJob(t, ts, "/api/v1/db/import", dbTransferRequest{
		Path:   exportPath,
		Scope:  dbTransferScopeBucket,
		Bucket: "restored",
	})
	importResp = waitForDBTransferJob(t, ts, importResp.JobID)
	require.Equal(t, dbTransferStatusDone, importResp.Status)
	require.Equal(t, uint64(1), importResp.Stats.BucketsImported)
	require.Equal(t, uint64(2), importResp.Stats.KeysImported)

	err = srv.DB.View(func(tx *storage.Tx) error {
		restored, err := tx.GetBucket([]byte("restored"))
		require.NoError(t, err)
		value, found := restored.Get([]byte("1"))
		require.True(t, found)
		require.Equal(t, []byte("alice"), value)
		value, found = restored.Get([]byte("2"))
		require.True(t, found)
		require.Equal(t, []byte("bob"), value)
		_, found = restored.Get([]byte("legacy"))
		require.False(t, found)

		other, err := tx.GetBucket([]byte("other"))
		require.NoError(t, err)
		value, found = other.Get([]byte("safe"))
		require.True(t, found)
		require.Equal(t, []byte("value"), value)
		return nil
	})
	require.NoError(t, err)
}

func TestHTTPExportImportJobConflict(t *testing.T) {
	srv, _, _ := setupTestServer(t)
	srv.jobMu.Lock()
	srv.importBusy = true
	srv.jobMu.Unlock()

	router := srv.buildRouter()
	ts := httptest.NewServer(router)
	defer ts.Close()

	resp, body := postDBTransferJob(t, ts, "/api/v1/db/export", dbTransferRequest{
		Path:      filepath.Join(t.TempDir(), "conflict.snapshot"),
		Scope:     dbTransferScopeDB,
		Overwrite: true,
	})
	require.Equal(t, http.StatusConflict, resp.StatusCode)
	require.Contains(t, body.Status, "Import job is already running")
}

func submitDBTransferJob(t *testing.T, ts *httptest.Server, endpoint string, payload dbTransferRequest) dbTransferJobResponse {
	t.Helper()
	resp, body := postDBTransferJob(t, ts, endpoint, payload)
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	return body
}

func postDBTransferJob(t *testing.T, ts *httptest.Server, endpoint string, payload dbTransferRequest) (*http.Response, dbTransferJobResponse) {
	t.Helper()
	body, err := json.Marshal(payload)
	require.NoError(t, err)

	resp, err := http.Post(ts.URL+endpoint, "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer func() {
		_ = resp.Body.Close()
	}()

	var response dbTransferJobResponse
	err = json.NewDecoder(resp.Body).Decode(&response)
	require.NoError(t, err)
	return resp, response
}

func waitForDBTransferJob(t *testing.T, ts *httptest.Server, jobID string) dbTransferJobResponse {
	t.Helper()

	var last dbTransferJobResponse
	require.Eventually(t, func() bool {
		resp, err := http.Get(ts.URL + "/api/v1/db/jobs/" + jobID)
		require.NoError(t, err)
		defer func() {
			_ = resp.Body.Close()
		}()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		err = json.NewDecoder(resp.Body).Decode(&last)
		require.NoError(t, err)
		return last.Status == dbTransferStatusDone || last.Status == dbTransferStatusFailed
	}, 5*time.Second, 20*time.Millisecond)

	return last
}
