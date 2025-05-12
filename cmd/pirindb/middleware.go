package main

import "net/http"

const (
	HLCHeader = "X-HLC-Clock"
)

func (srv *Server) hlcMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Get(HLCHeader)
		if header != "" {
			remoteHLC, err := ParseHLC(header)
			if err == nil {
				_, _ = srv.Clock.Update(remoteHLC)
			}
		}
		next.ServeHTTP(w, r)
	})
}
