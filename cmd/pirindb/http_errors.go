package main

import (
	"github.com/go-chi/render"
	"net/http"
)

type ErrResponse struct {
	HTTPStatusCode int    `json:"-"`
	Status         string `json:"status"`
}

func (e *ErrResponse) Render(w http.ResponseWriter, r *http.Request) error {
	render.Status(r, e.HTTPStatusCode)
	return nil
}

func ErrInvalidRequest() render.Renderer {
	return &ErrResponse{
		HTTPStatusCode: http.StatusBadRequest,
		Status:         "Invalid request",
	}
}

func ErrNotFound() render.Renderer {
	return &ErrResponse{
		HTTPStatusCode: http.StatusNotFound,
		Status:         "Key not found",
	}
}

func ErrRequestTimeout() render.Renderer {
	return &ErrResponse{
		HTTPStatusCode: http.StatusRequestTimeout,
		Status:         "Request timed out",
	}
}

func ErrInternalServerError() render.Renderer {
	return &ErrResponse{
		HTTPStatusCode: http.StatusInternalServerError,
		Status:         "Internal Server Error",
	}
}
