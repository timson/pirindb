package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/timson/pirindb/storage"
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
	DB     *storage.DB
	Logger *slog.Logger
	Config *Config
	Server *http.Server
	Shard  *Shard
	RingV1 *ConsistentHash
	RingV2 *ConsistentHash
}

func NewServer(cfg *Config, db *storage.DB, logger *slog.Logger) *Server {
	return &Server{
		Config: cfg,
		DB:     db,
		Logger: logger,
	}
}

func (srv *Server) IsShard() bool {
	return srv.Shard != nil
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

func (srv *Server) IsLocal(shard *Shard) bool {
	if !srv.IsShard() {
		return true
	}
	return shard.Name == srv.Shard.Name
}
