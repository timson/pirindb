package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/timson/pirindb/orm"
	"github.com/timson/pirindb/storage"
)

const (
	defaultDatasetURL  = "https://raw.githubusercontent.com/vega/vega-datasets/main/data/cars.json"
	defaultDatasetPath = "playground/orm_dataset/data/cars.json"
	defaultDBPath      = "playground/orm_dataset/data/cars.db"
)

func ensureDatasetFile(ctx context.Context, url string, path string, force bool) error {
	if !force {
		if st, err := os.Stat(path); err == nil && st.Size() > 0 {
			return nil
		}
	}

	client := &http.Client{Timeout: 30 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download failed, status=%s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	// Validate payload so we fail early if URL points to non-dataset content.
	var docs []carDatasetDoc
	if err = json.Unmarshal(body, &docs); err != nil {
		return fmt.Errorf("invalid dataset payload: %w", err)
	}
	if len(docs) == 0 {
		return fmt.Errorf("empty dataset payload")
	}

	if err = os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, body, 0o644)
}

func resetDBFiles(dbPath string) error {
	if err := os.Remove(dbPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	ext := filepath.Ext(dbPath)
	txLogPath := strings.TrimSuffix(dbPath, ext) + ".tlog"
	if err := os.Remove(txLogPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func loadCarsDataset(db *storage.DB, datasetPath string, batchSize int, targetRows int) (int, error) {
	raw, err := os.ReadFile(datasetPath)
	if err != nil {
		return 0, err
	}

	var docs []carDatasetDoc
	if err = json.Unmarshal(raw, &docs); err != nil {
		return 0, err
	}
	if batchSize <= 0 {
		batchSize = 128
	}
	if targetRows < 0 {
		return 0, fmt.Errorf("target rows must be >= 0")
	}

	o := orm.New(db)
	batch := make([]any, 0, batchSize)
	loaded := 0

	base := make([]Car, 0, len(docs))
	for i := range docs {
		car := docs[i].toCar()
		if car.Name == "" || car.Origin == "" {
			continue
		}
		base = append(base, *car)
	}
	if len(base) == 0 {
		return 0, fmt.Errorf("dataset has no valid rows")
	}
	if targetRows == 0 {
		targetRows = len(base)
	}

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if saveErr := o.Save(batch...); saveErr != nil {
			return saveErr
		}
		loaded += len(batch)
		batch = batch[:0]
		return nil
	}

	for row := 0; row < targetRows; row++ {
		baseCar := base[row%len(base)]
		replica := row / len(base)
		car := expandCar(baseCar, replica, row)
		batch = append(batch, car)
		if len(batch) >= batchSize {
			if err = flush(); err != nil {
				return loaded, err
			}
			if loaded >= 100000 && loaded%100000 == 0 {
				fmt.Printf("load progress: %d/%d rows\n", loaded, targetRows)
			}
		}
	}
	if err = flush(); err != nil {
		return loaded, err
	}
	return loaded, nil
}
