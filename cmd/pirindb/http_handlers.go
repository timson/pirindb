package main

import (
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

func (srv *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	render.JSON(w, r, HealthResponse{Status: "ok"})
}

func (srv *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	select {
	case <-r.Context().Done():
		_ = render.Render(w, r, ErrRequestTimeout())
		return
	default:
		key := chi.URLParam(r, "key")
		value, isFound := Get(srv.DB, key)
		if isFound != true {
			_ = render.Render(w, r, ErrNotFound())
			return
		}
		render.JSON(w, r, &GetResponse{Value: value, Status: "ok"})
	}
}

func (srv *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	select {
	case <-r.Context().Done():
		_ = render.Render(w, r, ErrRequestTimeout())
		return
	default:
		key := chi.URLParam(r, "key")
		deleted := Delete(srv.DB, key)
		if !deleted {
			_ = render.Render(w, r, ErrNotFound())
			return
		}
		render.Status(r, http.StatusNoContent)
		render.JSON(w, r, &DeleteResponse{Key: key, Status: "ok"})
	}
}

func (srv *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	select {
	case <-r.Context().Done():
		_ = render.Render(w, r, ErrRequestTimeout())
		return
	default:
	}

	key := chi.URLParam(r, "key")
	body, err := io.ReadAll(r.Body)
	if err != nil {
		_ = render.Render(w, r, ErrInvalidRequest())
		return
	}
	defer func() {
		_ = r.Body.Close()
	}()

	value := string(body)
	err = Put(srv.DB, key, value)
	if err != nil {
		_ = render.Render(w, r, ErrInternalServerError())
		return
	}

	render.Status(r, http.StatusCreated)
	render.JSON(w, r, &PutResponse{Key: key, Status: "ok"})
}

func (srv *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	status := Status(srv.DB)
	render.JSON(w, r, status)
}
