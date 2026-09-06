package main

import (
	"errors"
	"fmt"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/timson/pirindb/storage"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type ServerConfig struct {
	Host     string `mapstructure:"host" validate:"required,hostname|ip"`
	Port     int    `mapstructure:"port" validate:"required,min=1,max=65535"`
	LogLevel string `mapstructure:"log_level" validate:"required,oneof=INFO WARNING DEBUG ERROR"`
}

type RedisConfig struct {
	Enabled                       bool   `mapstructure:"enabled"`
	Host                          string `mapstructure:"host" validate:"required,hostname|ip"`
	Port                          int    `mapstructure:"port" validate:"required,min=1,max=65535"`
	ExpirySweepIntervalMs         int    `mapstructure:"expiry_sweep_interval_ms" validate:"min=0,max=60000"`
	ExpirySweepBatchSize          int    `mapstructure:"expiry_sweep_batch_size" validate:"min=0,max=100000"`
	ExpirySweepMaxBatchesPerCycle int    `mapstructure:"expiry_sweep_max_batches_per_cycle" validate:"min=0,max=10000"`
	GCSweepIntervalMs             int    `mapstructure:"gc_sweep_interval_ms" validate:"min=0,max=60000"`
	GCSweepBatchSize              int    `mapstructure:"gc_sweep_batch_size" validate:"min=0,max=100000"`
	GCSweepBatchBytes             int64  `mapstructure:"gc_sweep_batch_bytes" validate:"min=0,max=268435456"`
	GCMaxBatchesPerCycle          int    `mapstructure:"gc_max_batches_per_cycle" validate:"min=0,max=10000"`
	MigrationBatchSize            int    `mapstructure:"migration_batch_size" validate:"min=0,max=100000"`
	MigrationBatchBytes           int64  `mapstructure:"migration_batch_bytes" validate:"min=0,max=268435456"`
	PipelineMaxCommands           int    `mapstructure:"pipeline_max_commands" validate:"min=1,max=100000"`
	PipelineMaxBytes              int64  `mapstructure:"pipeline_max_bytes" validate:"min=1024,max=268435456"`
}

type ClusterNodeConfig struct {
	ID           string   `mapstructure:"id"`
	RedisAddress string   `mapstructure:"redis_address"`
	HTTPAddress  string   `mapstructure:"http_address"`
	Slots        []string `mapstructure:"slots"`
}

type ClusterBootstrapSlotsConfig struct {
	NodeID string   `mapstructure:"node_id"`
	Slots  []string `mapstructure:"slots"`
}

type ClusterTopologyConfig struct {
	Name           string                         `mapstructure:"name"`
	SlotCount      int                            `mapstructure:"slot_count"`
	Nodes          []*ClusterNodeConfig           `mapstructure:"nodes"`
	BootstrapSlots []*ClusterBootstrapSlotsConfig `mapstructure:"bootstrap_slots"`
}

type ClusterConfig struct {
	Enabled                  bool                   `mapstructure:"enabled"`
	NodeID                   string                 `mapstructure:"node_id"`
	SlotCount                int                    `mapstructure:"slot_count"`
	Nodes                    []*ClusterNodeConfig   `mapstructure:"nodes"`
	TopologyFile             string                 `mapstructure:"topology_file"`
	Topology                 *ClusterTopologyConfig `mapstructure:"-"`
	RequireAuth              bool                   `mapstructure:"require_auth"`
	PublicStatus             bool                   `mapstructure:"public_status"`
	AdminToken               string                 `mapstructure:"admin_token"`
	AdminTokenFile           string                 `mapstructure:"admin_token_file"`
	TransferChunkTargetBytes int64                  `mapstructure:"transfer_chunk_target_bytes"`
	TransferChunkMaxBytes    int64                  `mapstructure:"transfer_chunk_max_bytes"`
	TransferChunkMaxRecords  int                    `mapstructure:"transfer_chunk_max_records"`
}

func clusterTransferChunkTargetBytes(cfg *Config) int64 {
	if cfg != nil && cfg.Cluster != nil && cfg.Cluster.TransferChunkTargetBytes > 0 {
		return cfg.Cluster.TransferChunkTargetBytes
	}
	return clusterTransferDefaultTargetChunkBytes
}

func clusterTransferChunkMaxBytes(cfg *Config) int64 {
	if cfg != nil && cfg.Cluster != nil && cfg.Cluster.TransferChunkMaxBytes > 0 {
		return cfg.Cluster.TransferChunkMaxBytes
	}
	return clusterTransferDefaultMaxChunkBytes
}

func clusterTransferChunkMaxRecords(cfg *Config) int {
	if cfg != nil && cfg.Cluster != nil && cfg.Cluster.TransferChunkMaxRecords > 0 {
		return cfg.Cluster.TransferChunkMaxRecords
	}
	return clusterTransferDefaultMaxChunkRecords
}

type DatabaseConfig struct {
	Filename               string `mapstructure:"filename" validate:"required"`
	SyncPolicy             string `mapstructure:"sync_policy" validate:"required,oneof=strict journal group"`
	CheckpointTxThreshold  int    `mapstructure:"checkpoint_tx_threshold" validate:"required,min=1"`
	GroupCommitTxThreshold int    `mapstructure:"group_commit_tx_threshold" validate:"required,min=1"`
	GroupCommitWindowMs    int    `mapstructure:"group_commit_window_ms" validate:"required,min=0"`
	NodeCacheMB            int64  `mapstructure:"node_cache_mb" validate:"min=0,max=1024"`
}

type Config struct {
	Server  *ServerConfig
	Redis   *RedisConfig
	Cluster *ClusterConfig
	DB      *DatabaseConfig
}

func initDefaults() {
	viper.SetDefault("server.host", "127.0.0.1")
	viper.SetDefault("server.port", 4321)
	viper.SetDefault("db.filename", "pirin.db")
	viper.SetDefault("db.sync_policy", "strict")
	viper.SetDefault("db.checkpoint_tx_threshold", 64)
	viper.SetDefault("db.group_commit_tx_threshold", 16)
	viper.SetDefault("db.group_commit_window_ms", 1)
	viper.SetDefault("db.node_cache_mb", 32)
	viper.SetDefault("server.log_level", "INFO")
	viper.SetDefault("redis.enabled", true)
	viper.SetDefault("redis.host", "127.0.0.1")
	viper.SetDefault("redis.port", 6379)
	viper.SetDefault("redis.expiry_sweep_interval_ms", 100)
	viper.SetDefault("redis.expiry_sweep_batch_size", 128)
	viper.SetDefault("redis.expiry_sweep_max_batches_per_cycle", 32)
	viper.SetDefault("redis.gc_sweep_interval_ms", 100)
	viper.SetDefault("redis.gc_sweep_batch_size", 128)
	viper.SetDefault("redis.gc_sweep_batch_bytes", 4*1024*1024)
	viper.SetDefault("redis.gc_max_batches_per_cycle", 32)
	viper.SetDefault("redis.migration_batch_size", 128)
	viper.SetDefault("redis.migration_batch_bytes", 4*1024*1024)
	viper.SetDefault("redis.pipeline_max_commands", 256)
	viper.SetDefault("redis.pipeline_max_bytes", 8*1024*1024)
	viper.SetDefault("cluster.enabled", false)
	viper.SetDefault("cluster.slot_count", 16384)
	viper.SetDefault("cluster.require_auth", true)
	viper.SetDefault("cluster.public_status", true)
	viper.SetDefault("cluster.transfer_chunk_target_bytes", clusterTransferDefaultTargetChunkBytes)
	viper.SetDefault("cluster.transfer_chunk_max_bytes", clusterTransferDefaultMaxChunkBytes)
	viper.SetDefault("cluster.transfer_chunk_max_records", clusterTransferDefaultMaxChunkRecords)
}

func setupFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().String("config", "", "Config file (TOML)")
	cmd.PersistentFlags().String("host", "", "Server host")
	cmd.PersistentFlags().Int("port", 0, "Server port")
	cmd.PersistentFlags().String("db", "", "Database filename")
	cmd.PersistentFlags().String("sync-policy", "", "Storage sync policy: strict, journal, or group")
	cmd.PersistentFlags().Int("checkpoint-tx-threshold", 0, "Number of journal commits before forcing a main DB checkpoint")
	cmd.PersistentFlags().Int("group-commit-tx-threshold", 0, "Maximum number of write transactions to accumulate before forcing a group commit flush")
	cmd.PersistentFlags().Int("group-commit-window-ms", 0, "Maximum time window in milliseconds for collecting write transactions into one group commit flush")
	cmd.PersistentFlags().Int64("node-cache-mb", 0, "Maximum memory in MiB for the shared B-tree node cache; zero disables it")
	cmd.PersistentFlags().String("log", "", "log level")
	cmd.PersistentFlags().Bool("redis-enabled", true, "Enable Redis-compatible server")
	cmd.PersistentFlags().String("redis-host", "", "Redis server host")
	cmd.PersistentFlags().Int("redis-port", 0, "Redis server port")
	cmd.PersistentFlags().Int("redis-expiry-sweep-interval-ms", 0, "Interval between Redis expiry sweep cycles; zero disables active expiry")
	cmd.PersistentFlags().Int("redis-expiry-sweep-batch-size", 0, "Maximum expired Redis keys deleted per transaction")
	cmd.PersistentFlags().Int("redis-expiry-sweep-max-batches", 0, "Maximum Redis expiry transactions per sweep cycle")
	cmd.PersistentFlags().Int("redis-gc-sweep-interval-ms", 0, "Interval between Redis garbage-collection cycles")
	cmd.PersistentFlags().Int("redis-gc-sweep-batch-size", 0, "Maximum Redis garbage records deleted per transaction")
	cmd.PersistentFlags().Int64("redis-gc-sweep-batch-bytes", 0, "Estimated Redis garbage bytes deleted per transaction")
	cmd.PersistentFlags().Int("redis-gc-max-batches", 0, "Maximum Redis garbage-collection transactions per cycle")
	cmd.PersistentFlags().Int("redis-migration-batch-size", 0, "Maximum Redis object records copied per migration transaction")
	cmd.PersistentFlags().Int64("redis-migration-batch-bytes", 0, "Maximum estimated Redis object bytes copied per migration transaction")
	cmd.PersistentFlags().Int("redis-pipeline-max-commands", 0, "Maximum commands collected from one buffered Redis pipeline")
	cmd.PersistentFlags().Int64("redis-pipeline-max-bytes", 0, "Maximum request bytes collected from one buffered Redis pipeline")
	cmd.PersistentFlags().Bool("cluster-enabled", false, "Enable internal cluster slot routing")
	cmd.PersistentFlags().String("cluster-node-id", "", "Local cluster node ID")
	cmd.PersistentFlags().Int("cluster-slot-count", 0, "Total cluster slot count")
	cmd.PersistentFlags().String("cluster-topology-file", "", "Path to a shared cluster topology TOML file")
	cmd.PersistentFlags().String("cluster-admin-token-file", "", "Path to the cluster control-plane bearer token")
	cmd.PersistentFlags().Bool("cluster-require-auth", true, "Require authentication for cluster control-plane requests")
	cmd.PersistentFlags().Bool("cluster-public-status", true, "Allow unauthenticated read-only cluster status endpoints")
	cmd.PersistentFlags().Int64("cluster-transfer-chunk-target-bytes", 0, "Target encoded bytes per cluster transfer request")
	cmd.PersistentFlags().Int64("cluster-transfer-chunk-max-bytes", 0, "Maximum encoded bytes per cluster transfer request, except one oversized logical key")
	cmd.PersistentFlags().Int("cluster-transfer-chunk-max-records", 0, "Maximum protocol records per cluster transfer request")

	_ = viper.BindPFlag("server.host", cmd.PersistentFlags().Lookup("host"))
	_ = viper.BindPFlag("server.port", cmd.PersistentFlags().Lookup("port"))
	_ = viper.BindPFlag("db.filename", cmd.PersistentFlags().Lookup("db"))
	_ = viper.BindPFlag("db.sync_policy", cmd.PersistentFlags().Lookup("sync-policy"))
	_ = viper.BindPFlag("db.checkpoint_tx_threshold", cmd.PersistentFlags().Lookup("checkpoint-tx-threshold"))
	_ = viper.BindPFlag("db.group_commit_tx_threshold", cmd.PersistentFlags().Lookup("group-commit-tx-threshold"))
	_ = viper.BindPFlag("db.group_commit_window_ms", cmd.PersistentFlags().Lookup("group-commit-window-ms"))
	_ = viper.BindPFlag("db.node_cache_mb", cmd.PersistentFlags().Lookup("node-cache-mb"))
	_ = viper.BindPFlag("server.log_level", cmd.PersistentFlags().Lookup("log"))
	_ = viper.BindPFlag("redis.enabled", cmd.PersistentFlags().Lookup("redis-enabled"))
	_ = viper.BindPFlag("redis.host", cmd.PersistentFlags().Lookup("redis-host"))
	_ = viper.BindPFlag("redis.port", cmd.PersistentFlags().Lookup("redis-port"))
	_ = viper.BindPFlag("redis.expiry_sweep_interval_ms", cmd.PersistentFlags().Lookup("redis-expiry-sweep-interval-ms"))
	_ = viper.BindPFlag("redis.expiry_sweep_batch_size", cmd.PersistentFlags().Lookup("redis-expiry-sweep-batch-size"))
	_ = viper.BindPFlag("redis.expiry_sweep_max_batches_per_cycle", cmd.PersistentFlags().Lookup("redis-expiry-sweep-max-batches"))
	_ = viper.BindPFlag("redis.gc_sweep_interval_ms", cmd.PersistentFlags().Lookup("redis-gc-sweep-interval-ms"))
	_ = viper.BindPFlag("redis.gc_sweep_batch_size", cmd.PersistentFlags().Lookup("redis-gc-sweep-batch-size"))
	_ = viper.BindPFlag("redis.gc_sweep_batch_bytes", cmd.PersistentFlags().Lookup("redis-gc-sweep-batch-bytes"))
	_ = viper.BindPFlag("redis.gc_max_batches_per_cycle", cmd.PersistentFlags().Lookup("redis-gc-max-batches"))
	_ = viper.BindPFlag("redis.migration_batch_size", cmd.PersistentFlags().Lookup("redis-migration-batch-size"))
	_ = viper.BindPFlag("redis.migration_batch_bytes", cmd.PersistentFlags().Lookup("redis-migration-batch-bytes"))
	_ = viper.BindPFlag("redis.pipeline_max_commands", cmd.PersistentFlags().Lookup("redis-pipeline-max-commands"))
	_ = viper.BindPFlag("redis.pipeline_max_bytes", cmd.PersistentFlags().Lookup("redis-pipeline-max-bytes"))
	_ = viper.BindPFlag("cluster.enabled", cmd.PersistentFlags().Lookup("cluster-enabled"))
	_ = viper.BindPFlag("cluster.node_id", cmd.PersistentFlags().Lookup("cluster-node-id"))
	_ = viper.BindPFlag("cluster.slot_count", cmd.PersistentFlags().Lookup("cluster-slot-count"))
	_ = viper.BindPFlag("cluster.topology_file", cmd.PersistentFlags().Lookup("cluster-topology-file"))
	_ = viper.BindPFlag("cluster.admin_token_file", cmd.PersistentFlags().Lookup("cluster-admin-token-file"))
	_ = viper.BindPFlag("cluster.require_auth", cmd.PersistentFlags().Lookup("cluster-require-auth"))
	_ = viper.BindPFlag("cluster.public_status", cmd.PersistentFlags().Lookup("cluster-public-status"))
	_ = viper.BindPFlag("cluster.transfer_chunk_target_bytes", cmd.PersistentFlags().Lookup("cluster-transfer-chunk-target-bytes"))
	_ = viper.BindPFlag("cluster.transfer_chunk_max_bytes", cmd.PersistentFlags().Lookup("cluster-transfer-chunk-max-bytes"))
	_ = viper.BindPFlag("cluster.transfer_chunk_max_records", cmd.PersistentFlags().Lookup("cluster-transfer-chunk-max-records"))
	_ = viper.BindEnv("cluster.admin_token")

	viper.SetEnvPrefix("pirindb")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()
}

func loadConfig(cfgFile string) (*Config, error) {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		viper.SetConfigName("config")
		viper.AddConfigPath(".")
		viper.SetConfigType("toml")
	}

	_ = viper.ReadInConfig()

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, err
	}
	if err := hydrateClusterConfig(&cfg, cfgFile); err != nil {
		return nil, err
	}
	validate := validator.New(validator.WithRequiredStructEnabled())
	err := validate.Struct(&cfg)
	if err != nil {
		return nil, err
	}
	if err = validateClusterConfig(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func hydrateClusterConfig(cfg *Config, cfgFile string) error {
	if cfg == nil {
		return nil
	}
	if cfg.Cluster == nil || !cfg.Cluster.Enabled {
		return nil
	}
	if cfg.Cluster.TopologyFile != "" {
		topologyPath := cfg.Cluster.TopologyFile
		if !filepath.IsAbs(topologyPath) && cfgFile != "" {
			topologyPath = filepath.Join(filepath.Dir(cfgFile), topologyPath)
		}
		topology, err := loadClusterTopologyFile(topologyPath)
		if err != nil {
			return err
		}
		cfg.Cluster.TopologyFile = topologyPath
		cfg.Cluster.Topology = topology
		if topology.SlotCount > 0 {
			cfg.Cluster.SlotCount = topology.SlotCount
		}
	}
	if strings.TrimSpace(cfg.Cluster.AdminTokenFile) != "" {
		tokenPath := cfg.Cluster.AdminTokenFile
		if !filepath.IsAbs(tokenPath) && cfgFile != "" {
			tokenPath = filepath.Join(filepath.Dir(cfgFile), tokenPath)
		}
		raw, err := os.ReadFile(tokenPath)
		if err != nil {
			return err
		}
		cfg.Cluster.AdminTokenFile = tokenPath
		cfg.Cluster.AdminToken = strings.TrimSpace(string(raw))
	}
	if cfg.Cluster.RequireAuth && strings.TrimSpace(cfg.Cluster.AdminToken) == "" {
		return errors.New("cluster admin token is required when cluster.require_auth is enabled")
	}
	if cfg.Cluster.TransferChunkTargetBytes < 1024*1024 || cfg.Cluster.TransferChunkTargetBytes > clusterTransferMaxConfiguredChunkBytes {
		return fmt.Errorf("cluster.transfer_chunk_target_bytes must be between %d and %d", 1024*1024, clusterTransferMaxConfiguredChunkBytes)
	}
	if cfg.Cluster.TransferChunkMaxBytes < cfg.Cluster.TransferChunkTargetBytes || cfg.Cluster.TransferChunkMaxBytes > clusterTransferMaxConfiguredChunkBytes {
		return fmt.Errorf("cluster.transfer_chunk_max_bytes must be between target bytes and %d", clusterTransferMaxConfiguredChunkBytes)
	}
	if cfg.Cluster.TransferChunkMaxRecords <= 0 || cfg.Cluster.TransferChunkMaxRecords > clusterTransferMaxConfiguredRecords {
		return fmt.Errorf("cluster.transfer_chunk_max_records must be between 1 and %d", clusterTransferMaxConfiguredRecords)
	}
	if strings.TrimSpace(cfg.Cluster.NodeID) == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return err
		}
		cfg.Cluster.NodeID = strings.TrimSpace(hostname)
	}
	return nil
}

func loadClusterTopologyFile(path string) (*ClusterTopologyConfig, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("cluster topology file path is empty")
	}
	type clusterTopologyFile struct {
		Cluster *ClusterTopologyConfig `mapstructure:"cluster"`
	}
	v := viper.New()
	v.SetConfigFile(path)
	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}
	var topoFile clusterTopologyFile
	if err := v.Unmarshal(&topoFile); err != nil {
		return nil, err
	}
	if topoFile.Cluster == nil {
		return nil, errors.New("cluster topology file does not contain [cluster]")
	}
	return topoFile.Cluster, nil
}

func (cfg *DatabaseConfig) StorageOptions() *storage.Options {
	opts := storage.DefaultOptions()
	switch cfg.SyncPolicy {
	case "journal":
		opts.WithSyncPolicy(storage.SyncPolicyJournal)
	case "group":
		opts.WithSyncPolicy(storage.SyncPolicyGroup)
	default:
		opts.WithSyncPolicy(storage.SyncPolicyStrict)
	}
	opts.WithCheckpointTxThreshold(cfg.CheckpointTxThreshold)
	opts.WithGroupCommitTxThreshold(cfg.GroupCommitTxThreshold)
	opts.WithGroupCommitWindow(time.Duration(cfg.GroupCommitWindowMs) * time.Millisecond)
	opts.WithNodeCacheBytes(cfg.NodeCacheMB * 1024 * 1024)
	return opts
}
