package storage

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHealthReportsFatalAndClosedDatabase(t *testing.T) {
	var absent *DB
	require.ErrorIs(t, absent.Health(), ErrDatabaseClosed)
	db, err := Open(filepath.Join(t.TempDir(), "health.db"), DefaultOptions())
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.Health())
	tx, err := db.BeginE(true)
	require.NoError(t, err)
	// A normal write must not make probes block on the transaction lock.
	require.NoError(t, db.Health())
	tx.Rollback()
	db.setFatalErr(errors.New("simulated fsync failure"))
	require.ErrorIs(t, db.Health(), ErrDatabaseFatal)
	require.NoError(t, db.Close())
	require.ErrorIs(t, db.Health(), ErrDatabaseClosed)
}
