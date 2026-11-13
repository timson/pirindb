package main

import (
	"context"
	"errors"
	"net/http"

	"github.com/go-chi/render"
)

var errServerStopping = errors.New("server is stopping")

func (srv *Server) startBackgroundJob(run func(context.Context)) error {
	if srv == nil || run == nil {
		return errors.New("background job is required")
	}
	srv.lifecycleMu.Lock()
	if srv.stopping {
		srv.lifecycleMu.Unlock()
		return errServerStopping
	}
	ctx := srv.lifecycleCtx
	srv.lifecycleWG.Add(1)
	srv.lifecycleMu.Unlock()

	go func() {
		defer srv.lifecycleWG.Done()
		run(ctx)
	}()
	return nil
}

func (srv *Server) isStopping() bool {
	if srv == nil {
		return true
	}
	srv.lifecycleMu.Lock()
	defer srv.lifecycleMu.Unlock()
	return srv.stopping
}

func (srv *Server) rejectRequestsWhileStopping(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if srv.isStopping() {
			_ = render.Render(w, r, ErrStatus(http.StatusServiceUnavailable, errServerStopping.Error()))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func contextCancelled(ctx context.Context) bool {
	return ctx != nil && ctx.Err() != nil
}
