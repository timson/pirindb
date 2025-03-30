package main

import (
	"fmt"
	"io"
	"net/http"
	"strings"
)

func checkParamCount(params []string, expected int, commandName string) error {
	if len(params) != expected {
		return fmt.Errorf("invalid number of parameters for '%s' command", commandName)
	}
	return nil
}

func doRequest(method, url string, body string, expectedStatus int) (*http.Response, error) {
	var reqBody io.Reader
	if body != "" {
		reqBody = strings.NewReader(body)
	}

	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "text/plain")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	if resp.StatusCode != expectedStatus {
		defer func() {
			_ = resp.Body.Close()
		}()
		return nil, fmt.Errorf("unexpected status code: %s", resp.Status)
	}
	return resp, nil
}

func handleSetCommand(params []string, settings *Settings) error {
	if err := checkParamCount(params, 2, "set"); err != nil {
		return err
	}
	key, value := params[0], params[1]
	url := BuildURL(settings, fmt.Sprintf("/api/v1/kv/%s", key))
	resp, err := doRequest("POST", url, value, http.StatusCreated)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	return nil
}

func handleGetCommand(params []string, settings *Settings) error {
	if err := checkParamCount(params, 1, "get"); err != nil {
		return err
	}
	url := BuildURL(settings, fmt.Sprintf("/api/v1/kv/%s", params[0]))
	resp, err := doRequest("GET", url, "", http.StatusOK)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	PrintJSONResponse(resp)
	return nil
}

func handleDeleteCommand(params []string, settings *Settings) error {
	if err := checkParamCount(params, 1, "del"); err != nil {
		return err
	}
	url := BuildURL(settings, fmt.Sprintf("/api/v1/kv/%s", params[0]))
	resp, err := doRequest("DELETE", url, "", http.StatusNoContent)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	return nil
}

func handleStatusCommand(params []string, settings *Settings) error {
	if err := checkParamCount(params, 0, "status"); err != nil {
		return err
	}
	url := BuildURL(settings, "/api/v1/db/status")
	resp, err := doRequest("GET", url, "", http.StatusOK)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	PrintJSONResponse(resp)
	return nil
}
