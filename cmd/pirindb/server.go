package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/timson/pirindb/storage"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
)

var (
	DBBucket = []byte("main")
)

type Server struct {
	DB            *storage.DB
	Logger        *slog.Logger
	Config        *Config
	Cluster       *ClusterManager
	clusterErr    error
	HTTPServer    *http.Server
	RedisServer   *RedisServer
	jobMu         sync.Mutex
	jobs          map[string]*dbTransferJob
	importBusy    bool
	rebalanceMu   sync.Mutex
	rebalanceJobs map[string]*clusterRebalanceJob
}

func NewServer(cfg *Config, db *storage.DB, logger *slog.Logger) *Server {
	cluster, clusterErr := NewClusterManager(cfg, db, logger)
	srv := &Server{
		Config:        cfg,
		DB:            db,
		Logger:        logger,
		Cluster:       cluster,
		clusterErr:    clusterErr,
		jobs:          make(map[string]*dbTransferJob),
		rebalanceJobs: make(map[string]*clusterRebalanceJob),
	}
	if clusterErr == nil && cluster != nil {
		if err := srv.loadPersistedRebalanceJobs(); err != nil {
			srv.clusterErr = err
		}
	}
	return srv
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

	r.Route("/health", func(r chi.Router) {
		r.Get("/", srv.handleHealth)
	})

	r.Route("/api/v1", func(r chi.Router) {
		r.Route("/kv", func(r chi.Router) {
			r.Get("/{key}", srv.handleGet)
			r.Post("/{key}", srv.handlePut)
			r.Delete("/{key}", srv.handleDelete)
		})
		r.Route("/db", func(r chi.Router) {
			r.Get("/status", srv.handleStatus)
			r.Post("/export", srv.handleExportJob)
			r.Post("/import", srv.handleImportJob)
			r.Get("/jobs/{jobID}", srv.handleDBJobStatus)
		})
		r.Route("/cluster", func(r chi.Router) {
			r.Get("/status", srv.handleClusterStatus)
			r.Get("/slots", srv.handleClusterSlots)
			r.Get("/topology", srv.handleClusterTopology)
			r.Post("/reconcile-topology", srv.handleClusterReconcileTopology)
			r.Post("/nodes/join", srv.handleClusterJoinNode)
			r.Post("/rebalance/plan", srv.handleClusterRebalancePlan)
			r.Post("/rebalance/execute", srv.handleClusterRebalanceExecute)
			r.Get("/rebalance/{jobID}", srv.handleClusterRebalanceJobStatus)
			r.Post("/internal/state", srv.handleClusterInternalApplyState)
			r.Get("/internal/slot-snapshot", srv.handleClusterInternalSlotSnapshot)
			r.Post("/internal/slot-import", srv.handleClusterInternalSlotImport)
			r.Post("/internal/delta-apply", srv.handleClusterInternalDeltaApply)
			r.Post("/internal/slot-delete", srv.handleClusterInternalSlotDelete)
		})
	})

	return r
}

func (srv *Server) Start() error {
	if srv.clusterErr != nil {
		return srv.clusterErr
	}
	if srv.Config.Redis != nil && srv.Config.Redis.Enabled {
		srv.RedisServer = NewRedisServer(srv.Config, srv.DB, srv.Logger)
		srv.RedisServer.Cluster = srv.Cluster
		srv.RedisServer.clusterErr = srv.clusterErr
		if err := srv.RedisServer.Start(); err != nil {
			return err
		}
	}
	srv.resumePersistedRebalanceJobs()

	r := srv.buildRouter()
	srv.Logger.Info("started listening", "protocol", "http", "port", srv.Config.Server.Port, "host", srv.Config.Server.Host)
	srv.HTTPServer = &http.Server{
		Addr:    fmt.Sprintf("%s:%d", srv.Config.Server.Host, srv.Config.Server.Port),
		Handler: r,
	}
	srv.Logger.Info("press Ctrl+C to exit")

	if err := srv.HTTPServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		if srv.RedisServer != nil {
			_ = srv.RedisServer.Stop()
		}
		srv.Logger.Error("HTTP server error", slog.Any("err", err))
		return err
	}

	return nil
}

func (srv *Server) Stop() error {
	var stopErr error

	if srv.RedisServer != nil {
		srv.Logger.Info("Stopping Redis server")
		if err := srv.RedisServer.Stop(); err != nil {
			srv.Logger.Error("Redis server shutdown error", "error", err)
			stopErr = err
		}
	}

	if srv.HTTPServer != nil {
		srv.Logger.Info("Stopping HTTP server")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := srv.HTTPServer.Shutdown(ctx); err != nil {
			srv.Logger.Error("HTTP server shutdown error", "error", err)
			if stopErr == nil {
				stopErr = err
			}
		} else {
			srv.Logger.Info("HTTP server stopped")
		}
	}

	return stopErr
}
