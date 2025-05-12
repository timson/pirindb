package main

import (
	"errors"
	"github.com/go-chi/chi"
	"github.com/go-chi/render"
	"io"
	"net/http"
)

type GetResponse struct {
	Value  string `json:"value"`
	Status string `json:"status"`
}

type PutResponse struct {
	Key    string `json:"key"`
	Status string `json:"status"`
}

type DeleteResponse struct {
	Key    string `json:"key"`
	Status string `json:"status"`
}

type HealthResponse struct {
	Status string `json:"status"`
}

type ReshardingResponse struct {
	Status string `json:"status"`
}

type BatchGetRequest struct {
	Keys []string `json:"keys"`
}

type BatchGetResponse struct {
	Values  map[string]string `json:"values"`
	Missing []string          `json:"missing"`
	Status  string            `json:"status"`
}

func RenderHTTPError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, ErrKeyNotFound):
		_ = render.Render(w, r, RenderErrNotFound())
	case errors.Is(err, ErrServiceUnavailable):
		_ = render.Render(w, r, RenderErrServiceUnavailable())
	default:
		_ = render.Render(w, r, RenderErrInternalServerError())
	}
}

func withRequestContext(fn func(w http.ResponseWriter, r *http.Request)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			_ = render.Render(w, r, ErrRequestTimeout())
			return
		default:
			fn(w, r)
		}
	}
}

func (srv *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	render.JSON(w, r, HealthResponse{Status: "ok"})
}

func (srv *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	value, err := srv.Get(key)
	if err != nil {
		RenderHTTPError(w, r, err)
		return
	}

	render.Status(r, http.StatusOK)
	render.JSON(w, r, &GetResponse{Value: value, Status: "ok"})
}

func (srv *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	if srv.isLocal(key) {
		deleted := Delete(srv.DB, key)
		if !deleted {
			_ = render.Render(w, r, RenderErrNotFound())
			return
		}
	} else {
		err := srv.Delete(key)
		if err != nil {
			RenderHTTPError(w, r, err)
			return
		}
	}
	render.Status(r, http.StatusNoContent)
	render.JSON(w, r, &DeleteResponse{Key: key, Status: "ok"})
}

func (srv *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	body, err := io.ReadAll(r.Body)
	defer func() { _ = r.Body.Close() }()
	if err != nil {
		_ = render.Render(w, r, RenderErrInvalidRequest())
		return
	}

	value := string(body)
	clock := srv.Clock
	if r.Header.Get("resharding") == "true" {
		clock = nil
	}
	err = srv.Put(key, value, clock)
	if err != nil {
		RenderHTTPError(w, r, err)
		return
	}

	render.Status(r, http.StatusCreated)
	render.JSON(w, r, &PutResponse{Key: key, Status: "ok"})
}

func (srv *Server) handleDBStatus(w http.ResponseWriter, r *http.Request) {
	status := DBStatus(srv.DB)
	render.Status(r, http.StatusOK)
	render.JSON(w, r, status)
}

func (srv *Server) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	clusterStatus := srv.Cluster.Status()

	response := &ClusterStatusResponse{
		Shards: make(map[string]*ShardStatusResponse),
		Status: clusterStatus.Status,
	}

	for name, shard := range clusterStatus.Shards {
		response.Shards[name] = &ShardStatusResponse{
			Name:             shard.Name,
			Status:           shard.Status.String(),
			Host:             shard.Host,
			Port:             shard.Port,
			GossipPort:       shard.GossipPort,
			Scheme:           shard.Scheme,
			CurrentRingHash:  uint64PtrToHex(shard.currentHash),
			PreviousRingHash: uint64PtrToHex(shard.previousHash),
		}
	}

	render.Status(r, http.StatusOK)
	render.JSON(w, r, response)
}

func (srv *Server) handleResharding(w http.ResponseWriter, r *http.Request) {
	if !srv.IsClusterMode() {
		_ = render.Render(w, r, RenderErrInvalidRequest())
		return
	}
	srv.Cluster.RunResharding()
	response := &ReshardingResponse{Status: "ok"}
	render.Status(r, http.StatusOK)
	render.JSON(w, r, response)
}
