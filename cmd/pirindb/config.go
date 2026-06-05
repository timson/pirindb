package main

import (
	"errors"
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
	Enabled bool   `mapstructure:"enabled"`
	Host    string `mapstructure:"host" validate:"required,hostname|ip"`
	Port    int    `mapstructure:"port" validate:"required,min=1,max=65535"`
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
	Enabled      bool                   `mapstructure:"enabled"`
	NodeID       string                 `mapstructure:"node_id"`
	SlotCount    int                    `mapstructure:"slot_count"`
	Nodes        []*ClusterNodeConfig   `mapstructure:"nodes"`
	TopologyFile string                 `mapstructure:"topology_file"`
	Topology     *ClusterTopologyConfig `mapstructure:"-"`
}

type DatabaseConfig struct {
	Filename               string `mapstructure:"filename" validate:"required"`
	SyncPolicy             string `mapstructure:"sync_policy" validate:"required,oneof=strict journal group"`
	CheckpointTxThreshold  int    `mapstructure:"checkpoint_tx_threshold" validate:"required,min=1"`
	GroupCommitTxThreshold int    `mapstructure:"group_commit_tx_threshold" validate:"required,min=1"`
	GroupCommitWindowMs    int    `mapstructure:"group_commit_window_ms" validate:"required,min=0"`
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
	viper.SetDefault("server.log_level", "INFO")
	viper.SetDefault("redis.enabled", true)
	viper.SetDefault("redis.host", "127.0.0.1")
	viper.SetDefault("redis.port", 6379)
	viper.SetDefault("cluster.enabled", false)
	viper.SetDefault("cluster.slot_count", 16384)
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
	cmd.PersistentFlags().String("log", "", "log level")
	cmd.PersistentFlags().Bool("redis-enabled", true, "Enable Redis-compatible server")
	cmd.PersistentFlags().String("redis-host", "", "Redis server host")
	cmd.PersistentFlags().Int("redis-port", 0, "Redis server port")
	cmd.PersistentFlags().Bool("cluster-enabled", false, "Enable internal cluster slot routing")
	cmd.PersistentFlags().String("cluster-node-id", "", "Local cluster node ID")
	cmd.PersistentFlags().Int("cluster-slot-count", 0, "Total cluster slot count")
	cmd.PersistentFlags().String("cluster-topology-file", "", "Path to a shared cluster topology TOML file")

	_ = viper.BindPFlag("server.host", cmd.PersistentFlags().Lookup("host"))
	_ = viper.BindPFlag("server.port", cmd.PersistentFlags().Lookup("port"))
	_ = viper.BindPFlag("db.filename", cmd.PersistentFlags().Lookup("db"))
	_ = viper.BindPFlag("db.sync_policy", cmd.PersistentFlags().Lookup("sync-policy"))
	_ = viper.BindPFlag("db.checkpoint_tx_threshold", cmd.PersistentFlags().Lookup("checkpoint-tx-threshold"))
	_ = viper.BindPFlag("db.group_commit_tx_threshold", cmd.PersistentFlags().Lookup("group-commit-tx-threshold"))
	_ = viper.BindPFlag("db.group_commit_window_ms", cmd.PersistentFlags().Lookup("group-commit-window-ms"))
	_ = viper.BindPFlag("server.log_level", cmd.PersistentFlags().Lookup("log"))
	_ = viper.BindPFlag("redis.enabled", cmd.PersistentFlags().Lookup("redis-enabled"))
	_ = viper.BindPFlag("redis.host", cmd.PersistentFlags().Lookup("redis-host"))
	_ = viper.BindPFlag("redis.port", cmd.PersistentFlags().Lookup("redis-port"))
	_ = viper.BindPFlag("cluster.enabled", cmd.PersistentFlags().Lookup("cluster-enabled"))
	_ = viper.BindPFlag("cluster.node_id", cmd.PersistentFlags().Lookup("cluster-node-id"))
	_ = viper.BindPFlag("cluster.slot_count", cmd.PersistentFlags().Lookup("cluster-slot-count"))
	_ = viper.BindPFlag("cluster.topology_file", cmd.PersistentFlags().Lookup("cluster-topology-file"))

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
	if cfg == nil || cfg.Cluster == nil || !cfg.Cluster.Enabled {
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
	return opts
}
