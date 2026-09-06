package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	clusterAdminTokenHeader      = "X-Pirin-Cluster-Token"
	clusterMaxJSONRequestBytes   = int64(1024 * 1024)
	clusterMaxImportRequestBytes = int64(8 * 1024 * 1024 * 1024)
	clusterAdminStartWindow      = time.Minute
	clusterAdminStartsPerWindow  = 30
)

type clusterAuthTransport struct {
	base  http.RoundTripper
	token string
}

func (transport *clusterAuthTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request == nil || request.URL == nil || request.URL.Scheme != "http" {
		return nil, errors.New("cluster HTTP client requires an http:// URL")
	}
	cloned := request.Clone(request.Context())
	cloned.Header = request.Header.Clone()
	if strings.TrimSpace(transport.token) != "" {
		cloned.Header.Set("Authorization", "Bearer "+transport.token)
		cloned.Header.Set(clusterAdminTokenHeader, transport.token)
	}
	return transport.base.RoundTrip(cloned)
}

func configuredClusterAdminToken(cfg *Config) string {
	if cfg == nil || cfg.Cluster == nil {
		return ""
	}
	return strings.TrimSpace(cfg.Cluster.AdminToken)
}

func (srv *Server) clusterControlPlaneMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if srv.Config != nil && srv.Config.Cluster != nil && srv.Config.Cluster.RequireAuth {
			if !(srv.Config.Cluster.PublicStatus && isPublicClusterRead(r)) {
				expected := configuredClusterAdminToken(srv.Config)
				provided := clusterRequestToken(r)
				if expected == "" {
					writeClusterAPIError(w, http.StatusServiceUnavailable, "cluster_auth_unavailable", "cluster control-plane authentication is not configured")
					return
				}
				if provided != expected {
					writeClusterAPIError(w, http.StatusUnauthorized, "cluster_unauthorized", "valid cluster control-plane credentials are required")
					return
				}
			}
		}

		if r.Body != nil {
			maxBytes := clusterMaxJSONRequestBytes
			if strings.HasSuffix(r.URL.Path, "/internal/slot-import") {
				maxBytes = clusterMaxImportRequestBytes
			}
			r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
		}
		next.ServeHTTP(w, r)
	})
}

func isPublicClusterRead(r *http.Request) bool {
	if r.Method != http.MethodGet {
		return false
	}
	switch strings.TrimSuffix(r.URL.Path, "/") {
	case "/api/v1/cluster/status", "/api/v1/cluster/slots", "/api/v1/cluster/topology":
		return true
	default:
		return false
	}
}

func clusterRequestToken(r *http.Request) string {
	authorization := strings.TrimSpace(r.Header.Get("Authorization"))
	if len(authorization) > len("Bearer ") && strings.EqualFold(authorization[:len("Bearer ")], "Bearer ") {
		return strings.TrimSpace(authorization[len("Bearer "):])
	}
	return strings.TrimSpace(r.Header.Get(clusterAdminTokenHeader))
}

func writeClusterAPIError(w http.ResponseWriter, status int, code string, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "error", "code": code, "error": message})
}

func decodeStrictClusterJSON(r *http.Request, destination any) error {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request body must contain one JSON value")
		}
		return err
	}
	return nil
}

func (srv *Server) allowClusterAdminJobStart() bool {
	now := time.Now()
	srv.adminRateMu.Lock()
	defer srv.adminRateMu.Unlock()
	if srv.adminRateWindow.IsZero() || now.Sub(srv.adminRateWindow) >= clusterAdminStartWindow {
		srv.adminRateWindow = now
		srv.adminRateStarts = 0
	}
	if srv.adminRateStarts >= clusterAdminStartsPerWindow {
		return false
	}
	srv.adminRateStarts++
	return true
}
