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

func RenderErrInvalidRequest() render.Renderer {
	return &ErrResponse{
		HTTPStatusCode: http.StatusBadRequest,
		Status:         "Invalid request",
	}
}

func RenderErrNotFound() render.Renderer {
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

func RenderErrInternalServerError() render.Renderer {
	return &ErrResponse{
		HTTPStatusCode: http.StatusInternalServerError,
		Status:         "Internal Server Error",
	}
}

func RenderErrServiceUnavailable() render.Renderer {
	return &ErrResponse{
		HTTPStatusCode: http.StatusServiceUnavailable,
		Status:         "Service Unavailable",
	}
}
