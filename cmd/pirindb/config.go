package main

import (
	"github.com/go-playground/validator/v10"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/timson/pirindb/storage"
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

type ShardConfig struct {
	Name  string
	Index int
}

type DatabaseConfig struct {
	Filename               string `mapstructure:"filename" validate:"required"`
	SyncPolicy             string `mapstructure:"sync_policy" validate:"required,oneof=strict journal group"`
	CheckpointTxThreshold  int    `mapstructure:"checkpoint_tx_threshold" validate:"required,min=1"`
	GroupCommitTxThreshold int    `mapstructure:"group_commit_tx_threshold" validate:"required,min=1"`
	GroupCommitWindowMs    int    `mapstructure:"group_commit_window_ms" validate:"required,min=0"`
}

type Config struct {
	Server *ServerConfig
	Redis  *RedisConfig
	Shards []*ShardConfig
	DB     *DatabaseConfig
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
	validate := validator.New(validator.WithRequiredStructEnabled())
	err := validate.Struct(&cfg)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
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
