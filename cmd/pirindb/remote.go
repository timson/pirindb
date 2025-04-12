package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
)

// RemotePut sends a key-value pair to the given remote shard URL via HTTP POST.
func RemotePut(shardURL string, key string, value string) error {
	url := fmt.Sprintf("%s/api/v1/kv/%s", shardURL, key)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer([]byte(value)))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain")

	resp, err := http.DefaultClient.Do(req)
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

func RemoteGet(shardURL string, key string) (*http.Response, error) {
	var reqBody io.Reader
	url := fmt.Sprintf("%s/api/v1/kv/%s", shardURL, key)
	req, err := http.NewRequest(http.MethodGet, url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("performing request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("remote get failed: %s", string(body))
	}

	return resp, nil
}
