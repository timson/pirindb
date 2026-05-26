package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/timson/pirindb/storage"
)

func TestDatabaseConfigStorageOptions(t *testing.T) {
	journalCfg := &DatabaseConfig{
		Filename:               "test.db",
		SyncPolicy:             "journal",
		CheckpointTxThreshold:  123,
		GroupCommitTxThreshold: 16,
		GroupCommitWindowMs:    1,
	}
	journalOpts := journalCfg.StorageOptions()
	require.Equal(t, storage.SyncPolicyJournal, journalOpts.SyncPolicy)
	require.Equal(t, 123, journalOpts.CheckpointTxThreshold)

	strictCfg := &DatabaseConfig{
		Filename:               "test.db",
		SyncPolicy:             "strict",
		CheckpointTxThreshold:  64,
		GroupCommitTxThreshold: 16,
		GroupCommitWindowMs:    1,
	}
	strictOpts := strictCfg.StorageOptions()
	require.Equal(t, storage.SyncPolicyStrict, strictOpts.SyncPolicy)
	require.Equal(t, 64, strictOpts.CheckpointTxThreshold)

	groupCfg := &DatabaseConfig{
		Filename:               "test.db",
		SyncPolicy:             "group",
		CheckpointTxThreshold:  64,
		GroupCommitTxThreshold: 9,
		GroupCommitWindowMs:    7,
	}
	groupOpts := groupCfg.StorageOptions()
	require.Equal(t, storage.SyncPolicyGroup, groupOpts.SyncPolicy)
	require.Equal(t, 9, groupOpts.GroupCommitTxThreshold)
	require.Equal(t, 7*time.Millisecond, groupOpts.GroupCommitWindow)
}

func TestLoadConfigParsesSyncPolicy(t *testing.T) {
	viper.Reset()
	initDefaults()
	t.Cleanup(viper.Reset)

	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "pirindb.toml")
	err := os.WriteFile(cfgPath, []byte(`
[server]
host = "127.0.0.1"
port = 4321
log_level = "INFO"

[redis]
enabled = true
host = "127.0.0.1"
port = 6379

[db]
filename = "pirin.db"
sync_policy = "journal"
checkpoint_tx_threshold = 17
group_commit_tx_threshold = 9
group_commit_window_ms = 7
`), 0600)
	require.NoError(t, err)

	cfg, err := loadConfig(cfgPath)
	require.NoError(t, err)
	require.Equal(t, "journal", cfg.DB.SyncPolicy)
	require.Equal(t, 17, cfg.DB.CheckpointTxThreshold)
	require.Equal(t, 9, cfg.DB.GroupCommitTxThreshold)
	require.Equal(t, 7, cfg.DB.GroupCommitWindowMs)
}
