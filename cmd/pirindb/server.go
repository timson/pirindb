package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/timson/pirindb/storage"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
)

var (
	DBBucket    = []byte("main")
	ShardBucket = []byte("sharding")
)

type Server struct {
	DB         *storage.DB
	Logger     *slog.Logger
	Config     *Config
	Server     *http.Server
	Cluster    *Cluster
	Clock      *HLClock
	HTTPClient *http.Client
}

func NewServer(cfg *Config, db *storage.DB, logger *slog.Logger) *Server {
	return &Server{
		Config:     cfg,
		DB:         db,
		Logger:     logger,
		Clock:      NewHLClock(time.Second * 5),
		HTTPClient: newHTTPClient(cfg),
	}
}

func (srv *Server) IsClusterMode() bool {
	return srv.Cluster != nil
}

func RequestLogger(logger *slog.Logger) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			startTime := time.Now()

			next.ServeHTTP(w, r)

			logger.Info("Request completed",
				slog.String("method", r.Method),
				slog.String("url", r.URL.String()),
				slog.Duration("duration", time.Since(startTime)))
		})
	}
}

func (srv *Server) buildRouter() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(RequestLogger(srv.Logger))
	r.Use(srv.hlcMiddleware)

	r.Route("/health", func(r chi.Router) {
		r.Get("/", srv.handleHealth)
	})

	r.Route("/api/v1", func(r chi.Router) {
		r.Route("/kv", func(r chi.Router) {
			r.Get("/{key}", withRequestContext(srv.handleGet))
			r.Post("/{key}", withRequestContext(srv.handlePut))
			r.Delete("/{key}", withRequestContext(srv.handleDelete))
		})
		r.Route("/db", func(r chi.Router) {
			r.Get("/status", srv.handleDBStatus)
		})
		r.Route("/cluster", func(r chi.Router) {
			r.Get("/status", srv.handleClusterStatus)
			r.Post("/resharding", srv.handleResharding)
		})
	})

	return r
}

func (srv *Server) Start() error {
	r := srv.buildRouter()
	srv.Logger.Info("started listening", "port", srv.Config.Server.Port, "host", srv.Config.Server.Host)
	srv.Server = &http.Server{
		Addr:    fmt.Sprintf("%s:%d", srv.Config.Server.Host, srv.Config.Server.Port),
		Handler: r,
	}
	srv.Logger.Info("press Ctrl+C to exit")

	if err := srv.Server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		srv.Logger.Error("HTTP server error", slog.Any("err", err))
	}

	return nil
}

func (srv *Server) Stop() error {
	srv.Logger.Info("stopping HTTP server")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Server.Shutdown(ctx); err != nil {
		srv.Logger.Error("HTTP server shutdown error", "error", err)
		return err
	}

	srv.Logger.Info("HTTP server stopped")
	return nil
}

// Operations

func (srv *Server) Put(key string, value string, clock *HLClock) error {
	if srv.isLocal(key) {
		return srv.putLocal(key, value, clock)
	}
	shard := srv.getShardByKey(key)
	if shard == nil {
		return ErrInternalServerError
	}
	err := RemotePut(srv.HTTPClient, shard.URL(), key, value, false, srv.Cluster.clock)
	if err != nil {
		return err
	}
	return nil
}

func (srv *Server) Get(key string) (string, error) {
	if srv.isLocal(key) {
		return srv.getLocal(key)
	}

	for _, shard := range srv.getShardCandidatesByKey(key) {
		value, err := srv.getFromShard(shard, key)
		if err == nil {
			return value, nil
		}
		if errors.Is(err, ErrKeyNotFound) {
			continue
		}
		return "", err
	}
	return "", ErrKeyNotFound
}

func (srv *Server) Delete(key string) error {
	if srv.isLocal(key) {
		return srv.deleteLocal(key)
	}

	for _, shard := range srv.getShardCandidatesByKey(key) {
		err := srv.deleteFromShard(shard, key)
		fmt.Println(err)
		if err != nil {
			return err
		}
	}
	return nil
}

// Helper functions

func (srv *Server) isLocal(key string) bool {
	if !srv.IsClusterMode() {
		return true
	}
	if srv.Cluster.LocalShard().currentRing.GetShard(key) == srv.Cluster.LocalShard() {
		return true
	}
	return false
}

func (srv *Server) getShardByKey(key string) *Shard {
	if srv.Cluster.LocalShard().currentRing == nil {
		return nil
	}
	return srv.Cluster.LocalShard().currentRing.GetShard(key)
}

func (srv *Server) getShardCandidatesByKey(key string) []*Shard {
	local := srv.Cluster.LocalShard()
	if local == nil {
		return nil
	}

	var out []*Shard

	if local.currentRing != nil {
		if s := local.currentRing.GetShard(key); s != nil {
			out = append(out, s)
		}
	}

	if prev := local.previousRing; prev != nil && prev != local.currentRing {
		if s := prev.GetShard(key); s != nil && (len(out) == 0 || out[0] != s) {
			out = append(out, s)
		}
	}

	return out
}

func (srv *Server) getLocal(key string) (string, error) {
	value, found := Get(srv.DB, key)
	if !found {
		return "", ErrKeyNotFound
	}
	return value, nil
}

func (srv *Server) putLocal(key string, value string, clock *HLClock) error {
	return Put(srv.DB, key, value, clock)
}

func (srv *Server) deleteLocal(key string) error {
	ok := Delete(srv.DB, key)
	if !ok {
		return ErrKeyNotFound
	}
	return nil
}

func (srv *Server) getFromShard(shard *Shard, key string) (string, error) {
	if srv.Cluster.LocalShard() == shard {
		return srv.getLocal(key)
	}

	resp, err := RemoteGet(srv.HTTPClient, shard.URL(), key, srv.Cluster.clock)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", ErrInternalServerError
	}

	// If the remote shard returned a 404, propagate ErrKeyNotFound.
	if resp.StatusCode == http.StatusNotFound {
		return "", ErrKeyNotFound
	}

	response := &GetResponse{}
	err = json.Unmarshal(body, response)
	if err != nil {
		return "", ErrInternalServerError
	}

	return response.Value, nil
}

func (srv *Server) deleteFromShard(shard *Shard, key string) error {
	if srv.Cluster.LocalShard() == shard {
		return srv.deleteLocal(key)
	}

	resp, err := RemoteDelete(srv.HTTPClient, shard.URL(), key, srv.Cluster.clock)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	_, err = io.ReadAll(resp.Body)
	if err != nil {
		return ErrInternalServerError
	}

	// If the remote shard returned a 404, propagate ErrKeyNotFound.
	if resp.StatusCode == http.StatusNotFound {
		return ErrKeyNotFound
	}

	if resp.StatusCode != http.StatusNoContent {
		return ErrInternalServerError
	}

	return nil
}
