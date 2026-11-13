package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestServerStopCancelsAndWaitsForBackgroundJobs(t *testing.T) {
	srv, _, _ := setupTestServer(t)
	started := make(chan struct{})
	finished := make(chan struct{})
	require.NoError(t, srv.startBackgroundJob(func(ctx context.Context) {
		close(started)
		<-ctx.Done()
		close(finished)
	}))
	<-started

	require.NoError(t, srv.Stop())
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("background job was not stopped")
	}
	require.ErrorIs(t, srv.startBackgroundJob(func(context.Context) {}), errServerStopping)
	require.NoError(t, srv.Stop())

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/health", nil)
	srv.buildRouter().ServeHTTP(recorder, request)
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
}
