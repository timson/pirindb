package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/timson/pirindb/storage"
)

var (
	DBBucket = []byte("main")
)

type Server struct {
	DB                *storage.DB
	Logger            *slog.Logger
	Config            *Config
	Cluster           *ClusterManager
	clusterErr        error
	HTTPServer        *http.Server
	RedisServer       *RedisServer
	clusterHTTPClient *http.Client
	jobMu             sync.Mutex
	jobs              map[string]*dbTransferJob
	importBusy        bool
	rebalanceMu       sync.Mutex
	rebalanceJobs     map[string]*clusterRebalanceJob
	autoMu            sync.Mutex
	autoJobs          map[string]*clusterAutoRebalanceJob
	lifecycleMu       sync.Mutex
	lifecycleCtx      context.Context
	lifecycleStop     context.CancelFunc
	lifecycleWG       sync.WaitGroup
	stopping          bool
	stopOnce          sync.Once
	stopDone          chan struct{}
	stopErr           error
	adminRateMu       sync.Mutex
	adminRateWindow   time.Time
	adminRateStarts   int
}

func NewServer(cfg *Config, db *storage.DB, logger *slog.Logger) *Server {
	cluster, clusterErr := NewClusterManager(cfg, db, logger)
	lifecycleCtx, lifecycleStop := context.WithCancel(context.Background())
	clusterTransport := &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
	srv := &Server{
		Config:            cfg,
		DB:                db,
		Logger:            logger,
		Cluster:           cluster,
		clusterErr:        clusterErr,
		jobs:              make(map[string]*dbTransferJob),
		rebalanceJobs:     make(map[string]*clusterRebalanceJob),
		autoJobs:          make(map[string]*clusterAutoRebalanceJob),
		lifecycleCtx:      lifecycleCtx,
		lifecycleStop:     lifecycleStop,
		stopDone:          make(chan struct{}),
		clusterHTTPClient: &http.Client{Transport: &clusterAuthTransport{base: clusterTransport, token: configuredClusterAdminToken(cfg)}},
	}
	if clusterErr == nil && cluster != nil {
		if err := srv.loadPersistedRebalanceJobs(); err != nil {
			srv.clusterErr = err
		} else if err = srv.loadPersistedClusterOrchestrationJobs(); err != nil {
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
	r.Use(srv.rejectRequestsWhileStopping)

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
			r.Use(srv.clusterControlPlaneMiddleware)
			r.Get("/status", srv.handleClusterStatus)
			r.Get("/slots", srv.handleClusterSlots)
			r.Get("/topology", srv.handleClusterTopology)
			r.Post("/reconcile-topology", srv.handleClusterReconcileTopology)
			r.Post("/nodes/join", srv.handleClusterJoinNode)
			r.Post("/nodes/remove", srv.handleClusterRemoveNode)
			r.Post("/rebalance/plan", srv.handleClusterRebalancePlan)
			r.Post("/rebalance/plan-for-node", srv.handleClusterRebalancePlanForNode)
			r.Post("/rebalance/execute", srv.handleClusterRebalanceExecute)
			r.Post("/rebalance/auto", srv.handleClusterRebalanceAuto)
			r.Get("/rebalance/auto/{jobID}", srv.handleClusterAutoRebalanceJobStatus)
			r.Post("/rebalance/plan-drain-node", srv.handleClusterDrainNodePlan)
			r.Post("/rebalance/drain", srv.handleClusterDrainNode)
			r.Get("/rebalance/drain/{jobID}", srv.handleClusterDrainJobStatus)
			r.Get("/rebalance/{jobID}", srv.handleClusterRebalanceJobStatus)
			r.Post("/internal/state", srv.handleClusterInternalApplyState)
			r.Get("/internal/slot-snapshot", srv.handleClusterInternalSlotSnapshot)
			r.Get("/internal/slot-metrics", srv.handleClusterInternalSlotMetrics)
			r.Post("/internal/slot-import", srv.handleClusterInternalSlotImport)
			r.Get("/internal/slot-verify", srv.handleClusterInternalSlotVerify)
			r.Post("/internal/import-cleanup", srv.handleClusterInternalImportCleanup)
			r.Get("/internal/import-receipt", srv.handleClusterInternalImportReceipt)
			r.Post("/internal/slot-delete", srv.handleClusterInternalSlotDelete)
		})
	})

	return r
}

func (srv *Server) Start() error {
	listener, err := srv.startListeners()
	if err != nil {
		return err
	}
	srv.resumePersistedRebalanceJobs()
	srv.resumePersistedClusterOrchestrationJobs()
	if err := srv.HTTPServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (srv *Server) startListeners() (net.Listener, error) {
	srv.lifecycleMu.Lock()
	defer srv.lifecycleMu.Unlock()
	if srv.stopping {
		return nil, errServerStopping
	}

	if srv.clusterErr != nil {
		return nil, srv.clusterErr
	}
	if srv.Config.Redis != nil && srv.Config.Redis.Enabled {
		srv.RedisServer = NewRedisServer(srv.Config, srv.DB, srv.Logger)
		srv.RedisServer.Cluster = srv.Cluster
		srv.RedisServer.clusterErr = srv.clusterErr
		if err := srv.RedisServer.Start(); err != nil {
			return nil, err
		}
	}

	r := srv.buildRouter()
	srv.HTTPServer = &http.Server{
		Addr:              fmt.Sprintf("%s:%d", srv.Config.Server.Host, srv.Config.Server.Port),
		Handler:           r,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    64 * 1024,
	}
	srv.Logger.Info("press Ctrl+C to exit")

	listener, err := net.Listen("tcp", srv.HTTPServer.Addr)
	if err != nil {
		if srv.RedisServer != nil {
			_ = srv.RedisServer.Stop()
		}
		return nil, err
	}
	srv.Logger.Info("started listening", "protocol", "http", "address", listener.Addr().String())
	return listener, nil
}

func (srv *Server) Stop() error {
	srv.stopOnce.Do(func() {
		srv.stopErr = srv.stop()
		close(srv.stopDone)
	})
	<-srv.stopDone
	return srv.stopErr
}

func (srv *Server) stop() error {
	srv.lifecycleMu.Lock()
	srv.stopping = true
	srv.lifecycleMu.Unlock()
	srv.lifecycleStop()
	if srv.clusterHTTPClient != nil {
		srv.clusterHTTPClient.CloseIdleConnections()
	}

	var stopErr error
	if srv.HTTPServer != nil {
		srv.Logger.Info("Stopping HTTP server")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := srv.HTTPServer.Shutdown(ctx); err != nil {
			srv.Logger.Error("HTTP server shutdown error", "error", err)
			stopErr = errors.Join(stopErr, err)
		}
		cancel()
	}
	if srv.RedisServer != nil {
		srv.Logger.Info("Stopping Redis server")
		if err := srv.RedisServer.Stop(); err != nil && !errors.Is(err, net.ErrClosed) {
			srv.Logger.Error("Redis server shutdown error", "error", err)
			stopErr = errors.Join(stopErr, err)
		}
	}

	waitDone := make(chan struct{})
	go func() {
		srv.lifecycleWG.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
		srv.Logger.Info("background jobs stopped")
	case <-time.After(30 * time.Second):
		err := errors.New("timed out waiting for background jobs to stop")
		srv.Logger.Error(err.Error())
		stopErr = errors.Join(stopErr, err)
	}
	return stopErr
}
