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

func ErrStatus(statusCode int, status string) render.Renderer {
	return &ErrResponse{
		HTTPStatusCode: statusCode,
		Status:         status,
	}
}

func ErrInvalidRequest() render.Renderer {
	return ErrStatus(http.StatusBadRequest, "Invalid request")
}

func ErrNotFound() render.Renderer {
	return ErrStatus(http.StatusNotFound, "Key not found")
}

func ErrRequestTimeout() render.Renderer {
	return ErrStatus(http.StatusRequestTimeout, "Request timed out")
}

func ErrInternalServerError() render.Renderer {
	return ErrStatus(http.StatusInternalServerError, "Internal Server Error")
}
