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

func TestLoadConfigParsesClusterConfig(t *testing.T) {
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
sync_policy = "strict"
checkpoint_tx_threshold = 64
group_commit_tx_threshold = 16
group_commit_window_ms = 1

[cluster]
enabled = true
node_id = "node-a"
slot_count = 128

[[cluster.nodes]]
id = "node-a"
redis_address = "127.0.0.1:7000"
http_address = "http://127.0.0.1:17000"
slots = ["0-63"]

[[cluster.nodes]]
id = "node-b"
redis_address = "127.0.0.1:7001"
http_address = "http://127.0.0.1:17001"
slots = ["64-127"]
`), 0o600)
	require.NoError(t, err)

	cfg, err := loadConfig(cfgPath)
	require.NoError(t, err)
	require.NotNil(t, cfg.Cluster)
	require.True(t, cfg.Cluster.Enabled)
	require.Equal(t, "node-a", cfg.Cluster.NodeID)
	require.Equal(t, 128, cfg.Cluster.SlotCount)
	require.Len(t, cfg.Cluster.Nodes, 2)
	require.Equal(t, "node-b", cfg.Cluster.Nodes[1].ID)
	require.Equal(t, "127.0.0.1:7001", cfg.Cluster.Nodes[1].RedisAddress)
}

func TestLoadConfigParsesClusterTopologyFile(t *testing.T) {
	viper.Reset()
	initDefaults()
	t.Cleanup(viper.Reset)

	tmpDir := t.TempDir()
	topologyPath := filepath.Join(tmpDir, "topology.toml")
	err := os.WriteFile(topologyPath, []byte(`
[cluster]
name = "test-cluster"
slot_count = 128

[[cluster.nodes]]
id = "node-a"
redis_address = "127.0.0.1:7000"
http_address = "http://127.0.0.1:17000"

[[cluster.nodes]]
id = "node-b"
redis_address = "127.0.0.1:7001"
http_address = "http://127.0.0.1:17001"

[[cluster.bootstrap_slots]]
node_id = "node-a"
slots = ["0-63"]

[[cluster.bootstrap_slots]]
node_id = "node-b"
slots = ["64-127"]
`), 0o600)
	require.NoError(t, err)

	cfgPath := filepath.Join(tmpDir, "pirindb.toml")
	err = os.WriteFile(cfgPath, []byte(`
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
sync_policy = "strict"
checkpoint_tx_threshold = 64
group_commit_tx_threshold = 16
group_commit_window_ms = 1

[cluster]
enabled = true
node_id = "node-a"
topology_file = "topology.toml"
`), 0o600)
	require.NoError(t, err)

	cfg, err := loadConfig(cfgPath)
	require.NoError(t, err)
	require.NotNil(t, cfg.Cluster)
	require.Equal(t, filepath.Join(tmpDir, "topology.toml"), cfg.Cluster.TopologyFile)
	require.Equal(t, 128, cfg.Cluster.SlotCount)
	require.NotNil(t, cfg.Cluster.Topology)
	require.Equal(t, "test-cluster", cfg.Cluster.Topology.Name)
	require.Len(t, cfg.Cluster.Topology.Nodes, 2)
	require.Len(t, cfg.Cluster.Topology.BootstrapSlots, 2)
}

func TestLoadConfigFallsBackToHostnameForClusterNodeID(t *testing.T) {
	viper.Reset()
	initDefaults()
	t.Cleanup(viper.Reset)

	hostname, err := os.Hostname()
	require.NoError(t, err)
	require.NotEmpty(t, hostname)

	tmpDir := t.TempDir()
	topologyPath := filepath.Join(tmpDir, "topology.toml")
	err = os.WriteFile(topologyPath, []byte(`
[cluster]
name = "test-cluster"
slot_count = 16

[[cluster.nodes]]
id = "`+hostname+`"
redis_address = "127.0.0.1:7000"
http_address = "http://127.0.0.1:17000"

[[cluster.bootstrap_slots]]
node_id = "`+hostname+`"
slots = ["0-15"]
`), 0o600)
	require.NoError(t, err)

	cfgPath := filepath.Join(tmpDir, "pirindb.toml")
	err = os.WriteFile(cfgPath, []byte(`
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
sync_policy = "strict"
checkpoint_tx_threshold = 64
group_commit_tx_threshold = 16
group_commit_window_ms = 1

[cluster]
enabled = true
topology_file = "topology.toml"
`), 0o600)
	require.NoError(t, err)

	cfg, err := loadConfig(cfgPath)
	require.NoError(t, err)
	require.Equal(t, hostname, cfg.Cluster.NodeID)
}
