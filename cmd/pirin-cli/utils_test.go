package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildURLUsesPlainHTTP(t *testing.T) {
	require.Equal(t, "http://node-a:4321/api/v1/db/status", BuildURL(&Settings{
		Host: "node-a",
		Port: 4321,
	}, "/api/v1/db/status"))
}
