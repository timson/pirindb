package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"
)

func newHTTPClient(cfg *Config) *http.Client {
	timeout := time.Duration(cfg.Server.RemoteTimeoutSeconds) * time.Second
	return &http.Client{
		Timeout: timeout,
	}
}

func applyStandardHeaders(req *http.Request, clock *HLClock, resharding bool) {
	req.Header.Set("Content-Type", "text/plain")
	if clock != nil {
		req.Header.Set(HLCHeader, clock.Now().Serialize())
	}
	req.Header.Set("resharding", fmt.Sprintf("%t", resharding))
}

// RemotePut sends a key-value pair to the given remote shard URL via HTTP POST.
func RemotePut(client *http.Client, shardURL string, key string, value string, resharding bool, clock *HLClock) error {
	url := fmt.Sprintf("%s/api/v1/kv/%s", shardURL, key)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer([]byte(value)))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	applyStandardHeaders(req, clock, resharding)

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("performing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("remote put failed: %s", string(body))
	}

	return nil
}

func RemoteGet(client *http.Client, shardURL string, key string, clock *HLClock) (*http.Response, error) {
	var reqBody io.Reader
	url := fmt.Sprintf("%s/api/v1/kv/%s", shardURL, key)
	req, err := http.NewRequest(http.MethodGet, url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	applyStandardHeaders(req, clock, false)

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("performing request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode == http.StatusNotFound {
			return nil, ErrKeyNotFound
		}
		return nil, fmt.Errorf("remote get failed: %s", string(body))
	}

	return resp, nil
}

func RemoteDelete(client *http.Client, shardURL string, key string, clock *HLClock) (*http.Response, error) {
	var reqBody io.Reader
	url := fmt.Sprintf("%s/api/v1/kv/%s", shardURL, key)
	req, err := http.NewRequest(http.MethodDelete, url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	applyStandardHeaders(req, clock, false)

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("performing request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode == http.StatusNotFound {
			return nil, ErrKeyNotFound
		}
		return nil, fmt.Errorf("remote delete failed: %s", string(body))
	}

	return resp, nil
}
