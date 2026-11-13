package controllers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHTTPClusterAdminClientRejectsHTTPS(t *testing.T) {
	client := NewHTTPClusterAdminClient()
	_, err := client.GetStatus(context.Background(), "https://pirindb-0:4321")
	require.ErrorContains(t, err, "requires an http:// URL")
}
