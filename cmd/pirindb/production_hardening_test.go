package main

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/timson/pirindb/storage"
)

func TestRESPRejectsUnboundedLengths(t *testing.T) {
	maxInt := strconv.Itoa(int(^uint(0) >> 1))
	for name, input := range map[string]string{
		"array overflow": "*" + maxInt + "\r\n",
		"bulk overflow":  "*1\r\n$" + maxInt + "\r\n",
		"argument limit": fmt.Sprintf("*%d\r\n", redisMaxCommandArgs+1),
		"bulk limit":     fmt.Sprintf("*1\r\n$%d\r\n", redisMaxRequestBytes+1),
		"line limit":     strings.Repeat("x", redisMaxLineBytes+1),
		"header limit":   "*" + strings.Repeat("0", redisMaxLineBytes+1),
		"negative":       "*1\r\n$-1\r\n",
	} {
		t.Run(name, func(t *testing.T) {
			conn := &redisConn{reader: bufio.NewReader(strings.NewReader(input))}
			_, err := conn.readCommand()
			require.Error(t, err)
		})
	}
}

func TestRESPAggregateRequestLimit(t *testing.T) {
	// Each argument fits separately; the second header must be rejected before
	// attempting to read or allocate its payload.
	first := strings.Repeat("a", redisMaxRequestBytes/2)
	input := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n", len(first), first, len(first))
	conn := &redisConn{reader: bufio.NewReader(strings.NewReader(input))}
	_, err := conn.readCommand()
	require.ErrorContains(t, err, "invalid bulk string length")
}

func TestRESPMalformedClientDoesNotStopServer(t *testing.T) {
	srv, valid := setupTestRedisServer(t)
	bad, err := net.Dial("tcp", srv.listener.Addr().String())
	require.NoError(t, err)
	defer bad.Close()
	require.NoError(t, bad.SetDeadline(time.Now().Add(3*time.Second)))
	_, err = fmt.Fprintf(bad, "*1\r\n$%s\r\n", strconv.Itoa(int(^uint(0)>>1)))
	require.NoError(t, err)
	line, err := bufio.NewReader(bad).ReadString('\n')
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(line, "-ERR"))
	require.NoError(t, valid.SetDeadline(time.Now().Add(3*time.Second)))
	_, err = valid.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	require.NoError(t, err)
	line, err = bufio.NewReader(valid).ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "+PONG\r\n", line)
}

func TestHealthTracksStorageAndRedis(t *testing.T) {
	srv, _, _ := setupTestServer(t)
	check := func(want int) {
		t.Helper()
		response := httptest.NewRecorder()
		srv.buildRouter().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health", nil))
		require.Equal(t, want, response.Code)
	}
	check(http.StatusOK)
	srv.Config.Redis = &RedisConfig{Enabled: true, Host: "127.0.0.1", Port: 0}
	check(http.StatusServiceUnavailable)
	srv.RedisServer = NewRedisServer(srv.Config, srv.DB, srv.Logger)
	require.NoError(t, srv.RedisServer.Start())
	check(http.StatusOK)
	require.NoError(t, srv.RedisServer.Stop())
	check(http.StatusServiceUnavailable)
	srv.Config.Redis.Enabled = false
	require.NoError(t, srv.DB.Close())
	check(http.StatusServiceUnavailable)
}

func TestStartupFailureReturnsAndClosesStorage(t *testing.T) {
	for _, endpoint := range []string{"http", "redis", "cluster"} {
		t.Run(endpoint, func(t *testing.T) {
			srv, _, _ := setupTestServer(t)
			occupied, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer occupied.Close()
			port := occupied.Addr().(*net.TCPAddr).Port
			srv.Config.Redis = &RedisConfig{Enabled: true, Host: "127.0.0.1", Port: 0}
			switch endpoint {
			case "http":
				srv.Config.Server.Port = port
			case "redis":
				srv.Config.Redis.Port = port
			case "cluster":
				srv.clusterErr = errors.New("invalid topology")
			}
			result := make(chan error, 1)
			go func() { result <- serveUntilSignal(srv, make(chan os.Signal)) }()
			select {
			case err := <-result:
				require.Error(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("startup failure waited for a signal")
			}
			require.ErrorIs(t, srv.DB.Health(), storage.ErrDatabaseClosed)
			if srv.RedisServer != nil {
				require.False(t, srv.RedisServer.accepting.Load())
			}
		})
	}
}

func TestSignalDuringStartup(t *testing.T) {
	srv, _, _ := setupTestServer(t)
	signals := make(chan os.Signal, 1)
	signals <- syscall.SIGTERM
	require.NoError(t, serveUntilSignal(srv, signals))
	require.ErrorIs(t, srv.DB.Health(), storage.ErrDatabaseClosed)
}
