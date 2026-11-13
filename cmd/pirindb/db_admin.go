package main

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

func (srv *Server) databaseAdministrationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/v1/db/") && r.URL.Path != "/api/v1/db/status" {
			expected := ""
			if srv.Config != nil && srv.Config.DB != nil {
				expected = strings.TrimSpace(srv.Config.DB.AdminToken)
			}
			if expected == "" {
				http.Error(w, "database administration is disabled: configure db.admin_token", http.StatusServiceUnavailable)
				return
			}
			provided := strings.TrimSpace(r.Header.Get("Authorization"))
			if !strings.HasPrefix(provided, "Bearer ") || subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(provided, "Bearer ")), []byte(expected)) != 1 {
				http.Error(w, "database administrator credentials required", http.StatusUnauthorized)
				return
			}
			if r.Body != nil {
				r.Body = http.MaxBytesReader(w, r.Body, 1024*1024)
			}
		}
		if strings.HasPrefix(r.URL.Path, "/api/v1/kv/") {
			srv.jobMu.Lock()
			busy := srv.restoreBusy
			srv.jobMu.Unlock()
			if busy {
				http.Error(w, "database transfer in progress", http.StatusServiceUnavailable)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
