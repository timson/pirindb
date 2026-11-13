package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/timson/pirindb/storage"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDBSuiteServerHelper(t *testing.T) {
	path := os.Getenv("PIRIN_DBTEST_CHILD_DB")
	if path == "" {
		t.Skip("suite-owned child only")
	}
	db, e := storage.Open(path, nil)
	if e != nil {
		panic(e)
	}
	cfg := &Config{Server: &ServerConfig{Host: "127.0.0.1", Port: 0, LogLevel: "ERROR"}, DB: &DatabaseConfig{Filename: path}}
	srv := NewServer(cfg, db, createLogger("ERROR"))
	listener, e := srv.startListeners()
	if e != nil {
		panic(e)
	}
	fmt.Printf("DBTEST_READY %s\n", listener.Addr())
	if e = srv.HTTPServer.Serve(listener); e != nil {
		panic(e)
	}
}
func TestDBSuiteServerCrash(t *testing.T) {
	if os.Getenv("PIRIN_DBTEST") == "" {
		t.Skip("run make test-db")
	}
	parent := os.Getenv("PIRIN_DBTEST_ARTIFACTS")
	root, e := os.MkdirTemp(parent, "server-case-")
	require.NoError(t, e)
	defer func() {
		if !t.Failed() {
			_ = os.RemoveAll(root)
		}
	}()
	metadata, _ := json.Marshal(struct {
		Test                               string
		Seed                               int64
		Profile, Duration, Policy, Workers string
	}{Test: t.Name(), Profile: os.Getenv("PIRIN_DBTEST_PROFILE"), Duration: os.Getenv("PIRIN_DBTEST_DURATION"), Policy: os.Getenv("PIRIN_DBTEST_POLICY"), Workers: os.Getenv("PIRIN_DBTEST_WORKERS")})
	require.NoError(t, os.WriteFile(filepath.Join(root, "scenario.json"), metadata, 0600))
	path := filepath.Join(root, "server.db")
	var owned *exec.Cmd
	var cleanupOwned func()
	t.Cleanup(func() {
		if owned != nil && owned.Process != nil {
			_ = owned.Process.Kill()
			_ = owned.Wait()
		}
		if cleanupOwned != nil {
			cleanupOwned()
		}
	})
	generation := 0
	start := func() (*exec.Cmd, string, context.CancelFunc) {
		ctx, cancel := context.WithTimeout(context.Background(), suiteChildTimeout())
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDBSuiteServerHelper$")
		cmd.Env = append(os.Environ(), "PIRIN_DBTEST_CHILD_DB="+path)
		stdout, e := cmd.StdoutPipe()
		require.NoError(t, e)
		generation++
		if generation > 8 {
			_ = os.Remove(filepath.Join(root, fmt.Sprintf("child-%d.log", generation-8)))
		}
		logs, e := os.Create(filepath.Join(root, fmt.Sprintf("child-%d.log", generation)))
		require.NoError(t, e)
		cmd.Stderr = logs
		cleanup := func() { cancel(); _ = logs.Close() }
		owned = cmd
		cleanupOwned = cleanup
		require.NoError(t, cmd.Start())
		ready := make(chan string, 1)
		go func() {
			scanner := bufio.NewScanner(stdout)
			for scanner.Scan() {
				line := scanner.Text()
				fmt.Fprintln(logs, line)
				if strings.HasPrefix(line, "DBTEST_READY ") {
					ready <- strings.TrimPrefix(line, "DBTEST_READY ")
					return
				}
			}
			ready <- ""
		}()
		select {
		case address := <-ready:
			require.NotEmpty(t, address)
			return cmd, "http://" + address, cleanup
		case <-time.After(15 * time.Second):
			t.Fatal("server startup timed out")
			return nil, "", cancel
		}
	}
	child, url, cancel := start()

	defer func() { _ = child.Process.Kill(); _ = child.Wait() }()
	client := &http.Client{Timeout: 5 * time.Second}
	deadline := time.Now()
	if profile := os.Getenv("PIRIN_DBTEST_PROFILE"); profile == "extended" || profile == "soak" {
		duration, e := time.ParseDuration(os.Getenv("PIRIN_DBTEST_DURATION"))
		require.NoError(t, e)
		deadline = time.Now().Add(duration)
	}
	cycle := 0
	for {
		for i := 0; i < 12; i++ {
			response, e := client.Post(fmt.Sprintf("%s/api/v1/kv/key-%d", url, i), "text/plain", strings.NewReader(fmt.Sprintf("%d:%d", cycle, i)))
			require.NoError(t, e)
			response.Body.Close()
			require.Equal(t, http.StatusCreated, response.StatusCode)
		}
		require.NoError(t, child.Process.Kill())
		_ = child.Wait()
		cancel()
		child, url, cancel = start()
		for i := 0; i < 12; i++ {
			response, e := client.Get(fmt.Sprintf("%s/api/v1/kv/key-%d", url, i))
			require.NoError(t, e)
			var result GetResponse
			e = json.NewDecoder(response.Body).Decode(&result)
			response.Body.Close()
			require.NoError(t, e)
			require.Equal(t, http.StatusOK, response.StatusCode)
			require.Equal(t, fmt.Sprintf("%d:%d", cycle, i), result.Value)
		}
		cycle++
		if time.Now().After(deadline) {
			break
		}
	}

}

func suiteChildTimeout() time.Duration {
	if d, e := time.ParseDuration(os.Getenv("PIRIN_DBTEST_DURATION")); e == nil && d > 0 {
		return d + time.Minute
	}
	return time.Minute
}
