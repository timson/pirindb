package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/render"
	"github.com/google/uuid"
	"github.com/timson/pirindb/storage"
)

const (
	dbTransferKindExport = "export"
	dbTransferKindImport = "import"

	dbTransferScopeDB     = "db"
	dbTransferScopeBucket = "bucket"

	dbTransferStatusQueued  = "queued"
	dbTransferStatusRunning = "running"
	dbTransferStatusDone    = "done"
	dbTransferStatusFailed  = "failed"
)

var errDBImportBusy = errors.New("import job already running")

type dbTransferRequest struct {
	Path      string `json:"path"`
	Scope     string `json:"scope"`
	Bucket    string `json:"bucket,omitempty"`
	Overwrite bool   `json:"overwrite,omitempty"`
}

type dbTransferJobStats struct {
	BucketsExported uint64 `json:"buckets_exported,omitempty"`
	KeysExported    uint64 `json:"keys_exported,omitempty"`
	BytesWritten    int64  `json:"bytes_written,omitempty"`
	BucketsImported uint64 `json:"buckets_imported,omitempty"`
	KeysImported    uint64 `json:"keys_imported,omitempty"`
	BytesRead       int64  `json:"bytes_read,omitempty"`
}

type dbTransferJobResponse struct {
	JobID      string             `json:"job_id"`
	Kind       string             `json:"kind"`
	Scope      string             `json:"scope"`
	Bucket     string             `json:"bucket,omitempty"`
	Path       string             `json:"path"`
	Status     string             `json:"status"`
	StartedAt  *time.Time         `json:"started_at,omitempty"`
	FinishedAt *time.Time         `json:"finished_at,omitempty"`
	Error      string             `json:"error,omitempty"`
	Stats      dbTransferJobStats `json:"stats,omitempty"`
}

type dbTransferJob struct {
	mu         sync.Mutex
	ID         string
	Kind       string
	Scope      string
	Bucket     string
	Path       string
	Status     string
	StartedAt  *time.Time
	FinishedAt *time.Time
	Error      string
	Stats      dbTransferJobStats
}

func (job *dbTransferJob) snapshot() dbTransferJobResponse {
	job.mu.Lock()
	defer job.mu.Unlock()

	var startedAt *time.Time
	if job.StartedAt != nil {
		started := *job.StartedAt
		startedAt = &started
	}
	var finishedAt *time.Time
	if job.FinishedAt != nil {
		finished := *job.FinishedAt
		finishedAt = &finished
	}

	return dbTransferJobResponse{
		JobID:      job.ID,
		Kind:       job.Kind,
		Scope:      job.Scope,
		Bucket:     job.Bucket,
		Path:       job.Path,
		Status:     job.Status,
		StartedAt:  startedAt,
		FinishedAt: finishedAt,
		Error:      job.Error,
		Stats:      job.Stats,
	}
}

func (srv *Server) handleExportJob(w http.ResponseWriter, r *http.Request) {
	req, err := decodeDBTransferRequest(r)
	if err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusBadRequest, err.Error()))
		return
	}
	if err = srv.validateDBTransferRequest(req, true); err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusBadRequest, err.Error()))
		return
	}

	job, err := srv.startDBTransferJob(dbTransferKindExport, req, func() (dbTransferJobStats, error) {
		switch req.Scope {
		case dbTransferScopeDB:
			stats, exportErr := storage.ExportDBToPath(srv.DB, req.Path, req.Overwrite)
			return dbTransferJobStats{
				BucketsExported: stats.BucketsExported,
				KeysExported:    stats.KeysExported,
				BytesWritten:    stats.BytesWritten,
			}, exportErr
		case dbTransferScopeBucket:
			stats, exportErr := storage.ExportBucketToPath(srv.DB, []byte(req.Bucket), req.Path, req.Overwrite)
			return dbTransferJobStats{
				BucketsExported: stats.BucketsExported,
				KeysExported:    stats.KeysExported,
				BytesWritten:    stats.BytesWritten,
			}, exportErr
		default:
			return dbTransferJobStats{}, fmt.Errorf("unsupported export scope %q", req.Scope)
		}
	})
	if err != nil {
		if errors.Is(err, errDBImportBusy) {
			_ = render.Render(w, r, ErrStatus(http.StatusConflict, "Import job is already running"))
			return
		}
		_ = render.Render(w, r, ErrInternalServerError())
		return
	}

	render.Status(r, http.StatusAccepted)
	render.JSON(w, r, job.snapshot())
}

func (srv *Server) handleImportJob(w http.ResponseWriter, r *http.Request) {
	req, err := decodeDBTransferRequest(r)
	if err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusBadRequest, err.Error()))
		return
	}
	if err = srv.validateDBTransferRequest(req, false); err != nil {
		_ = render.Render(w, r, ErrStatus(http.StatusBadRequest, err.Error()))
		return
	}

	job, err := srv.startDBTransferJob(dbTransferKindImport, req, func() (dbTransferJobStats, error) {
		switch req.Scope {
		case dbTransferScopeDB:
			stats, importErr := storage.ImportDBFromPath(srv.DB, req.Path)
			return dbTransferJobStats{
				BucketsImported: stats.BucketsImported,
				KeysImported:    stats.KeysImported,
				BytesRead:       stats.BytesRead,
			}, importErr
		case dbTransferScopeBucket:
			stats, importErr := storage.ImportBucketFromPath(srv.DB, []byte(req.Bucket), req.Path)
			return dbTransferJobStats{
				BucketsImported: stats.BucketsImported,
				KeysImported:    stats.KeysImported,
				BytesRead:       stats.BytesRead,
			}, importErr
		default:
			return dbTransferJobStats{}, fmt.Errorf("unsupported import scope %q", req.Scope)
		}
	})
	if err != nil {
		if errors.Is(err, errDBImportBusy) {
			_ = render.Render(w, r, ErrStatus(http.StatusConflict, "Import job is already running"))
			return
		}
		_ = render.Render(w, r, ErrInternalServerError())
		return
	}

	render.Status(r, http.StatusAccepted)
	render.JSON(w, r, job.snapshot())
}

func (srv *Server) handleDBJobStatus(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "jobID")
	job, ok := srv.getDBTransferJob(jobID)
	if !ok {
		_ = render.Render(w, r, ErrStatus(http.StatusNotFound, "Job not found"))
		return
	}
	render.JSON(w, r, job)
}

func decodeDBTransferRequest(r *http.Request) (*dbTransferRequest, error) {
	defer func() {
		_ = r.Body.Close()
	}()

	var req dbTransferRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		return nil, errors.New("invalid JSON request body")
	}
	req.Scope = strings.ToLower(strings.TrimSpace(req.Scope))
	req.Bucket = strings.TrimSpace(req.Bucket)
	req.Path = strings.TrimSpace(req.Path)
	return &req, nil
}

func (srv *Server) validateDBTransferRequest(req *dbTransferRequest, isExport bool) error {
	if req.Path == "" {
		return errors.New("path is required")
	}
	switch req.Scope {
	case dbTransferScopeDB:
		if req.Bucket != "" {
			return errors.New("bucket must be empty for db scope")
		}
	case dbTransferScopeBucket:
		if req.Bucket == "" {
			return errors.New("bucket is required for bucket scope")
		}
	default:
		return errors.New("scope must be either 'db' or 'bucket'")
	}

	absolutePath, err := filepath.Abs(filepath.Clean(req.Path))
	if err != nil {
		return err
	}
	req.Path = absolutePath

	if isExport {
		if err = srv.validateExportPath(req.Path, req.Overwrite); err != nil {
			return err
		}
		if req.Scope == dbTransferScopeBucket {
			if err = srv.validateBucketExists(req.Bucket); err != nil {
				return err
			}
		}
		return nil
	}

	return srv.validateImportPath(req.Path)
}

func (srv *Server) validateExportPath(path string, overwrite bool) error {
	dbPath, err := filepath.Abs(filepath.Clean(srv.Config.DB.Filename))
	if err != nil {
		return err
	}
	txLogPath, err := filepath.Abs(filepath.Clean(srv.DB.GetOptions().TxLogPath))
	if err != nil {
		return err
	}
	if path == dbPath || path == txLogPath {
		return errors.New("export path must not overwrite the active database or tx log file")
	}

	info, err := os.Stat(path)
	switch {
	case err == nil && info.IsDir():
		return errors.New("export path must be a file, not a directory")
	case err == nil && !overwrite:
		return errors.New("export path already exists")
	case err != nil && !errors.Is(err, os.ErrNotExist):
		return err
	}

	parent := filepath.Dir(path)
	parentInfo, err := os.Stat(parent)
	if err != nil {
		return errors.New("export path parent directory does not exist")
	}
	if !parentInfo.IsDir() {
		return errors.New("export path parent is not a directory")
	}
	return nil
}

func (srv *Server) validateImportPath(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return errors.New("import path does not exist")
		}
		return err
	}
	if info.IsDir() {
		return errors.New("import path must be a file, not a directory")
	}
	return nil
}

func (srv *Server) validateBucketExists(bucketName string) error {
	return srv.DB.View(func(tx *storage.Tx) error {
		_, err := tx.GetBucket([]byte(bucketName))
		if errors.Is(err, storage.ErrBucketNotFound) {
			return errors.New("bucket not found")
		}
		return err
	})
}

func (srv *Server) startDBTransferJob(kind string, req *dbTransferRequest, run func() (dbTransferJobStats, error)) (*dbTransferJob, error) {
	srv.jobMu.Lock()
	defer srv.jobMu.Unlock()

	if srv.importBusy {
		return nil, errDBImportBusy
	}
	if kind == dbTransferKindImport {
		srv.importBusy = true
	}

	job := &dbTransferJob{
		ID:     uuid.NewString(),
		Kind:   kind,
		Scope:  req.Scope,
		Bucket: req.Bucket,
		Path:   req.Path,
		Status: dbTransferStatusQueued,
	}
	srv.jobs[job.ID] = job

	go srv.runDBTransferJob(job, run)
	return job, nil
}

func (srv *Server) runDBTransferJob(job *dbTransferJob, run func() (dbTransferJobStats, error)) {
	startedAt := time.Now()
	job.mu.Lock()
	job.Status = dbTransferStatusRunning
	job.StartedAt = &startedAt
	job.mu.Unlock()

	defer func() {
		if job.Kind == dbTransferKindImport {
			srv.jobMu.Lock()
			srv.importBusy = false
			srv.jobMu.Unlock()
		}
		if recovered := recover(); recovered != nil {
			finishedAt := time.Now()
			job.mu.Lock()
			job.Status = dbTransferStatusFailed
			job.Error = fmt.Sprintf("panic: %v", recovered)
			job.FinishedAt = &finishedAt
			job.mu.Unlock()
		}
	}()

	stats, err := run()
	finishedAt := time.Now()

	job.mu.Lock()
	job.FinishedAt = &finishedAt
	if err != nil {
		job.Status = dbTransferStatusFailed
		job.Error = err.Error()
	} else {
		job.Status = dbTransferStatusDone
		job.Stats = stats
	}
	job.mu.Unlock()
}

func (srv *Server) getDBTransferJob(jobID string) (dbTransferJobResponse, bool) {
	srv.jobMu.Lock()
	job, ok := srv.jobs[jobID]
	srv.jobMu.Unlock()
	if !ok {
		return dbTransferJobResponse{}, false
	}
	return job.snapshot(), true
}
