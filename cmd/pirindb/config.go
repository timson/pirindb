package main

import (
	"fmt"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"strings"
	"time"
)

type ServerConfig struct {
	Host                 string `mapstructure:"host" validate:"required,hostname|ip"`
	Port                 int    `mapstructure:"port" validate:"required,min=1,max=65535"`
	LogLevel             string `mapstructure:"log_level" validate:"required,oneof=INFO WARNING DEBUG ERROR"`
	ShardName            string `mapstructure:"shard_name"`
	RemoteTimeoutSeconds int    `mapstructure:"remote_timeout_seconds" validate:"required,min=1,max=300"`
}

type ShardConfig struct {
	Name       string
	Host       string `mapstructure:"host" validate:"required,hostname|ip"`
	Port       int    `mapstructure:"port" validate:"required,min=1,max=65535"`
	GossipPort int    `mapstructure:"gossipport" validate:"required,,min=1,max=65535"`
	Scheme     string `mapstructure:"scheme" validate:"omitempty,oneof=http https"`
	Skip       bool   `mapstructure:"skip" validate:"omitempty"`
}

type ReshardingConfig struct {
	RetryTimeout int `mapstructure:"retry_timeout" validate:"required,min=1"`
	Retries      int `mapstructure:"retries" validate:"required,min=1,max=65536"`
	BatchSize    int `mapstructure:"batch_size" validate:"required,min=1,max=65536"`
	BatchDelay   int `mapstructure:"batch_delay" validate:"required,min=1,max=65536"`
}

func (rc *ReshardingConfig) GetRetryTimeout() time.Duration {
	return time.Duration(rc.RetryTimeout) * time.Second
}

func (rc *ReshardingConfig) GetBatchDelay() time.Duration {
	return time.Duration(rc.BatchDelay) * time.Millisecond
}

func (s *ShardConfig) setDefaults() {
	if s.Scheme == "" {
		s.Scheme = "http"
	}
}

type DatabaseConfig struct {
	Filename string `mapstructure:"filename" validate:"required,filepath"`
}

type Config struct {
	Server     *ServerConfig
	Shards     []*ShardConfig
	DB         *DatabaseConfig
	Resharding *ReshardingConfig
}

func initDefaults() {
	viper.SetDefault("server.host", "127.0.0.1")
	viper.SetDefault("server.port", 4321)
	viper.SetDefault("db.filename", "pirin.db")
	viper.SetDefault("server.log_level", "INFO")
	viper.SetDefault("resharding.retry_timeout", 5)
	viper.SetDefault("resharding.retries", 10)
	viper.SetDefault("resharding.batch_size", 1000)
	viper.SetDefault("resharding.batch_delay", 100)
	viper.SetDefault("server.remote_timeout_seconds", 5)

}

func setupFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().String("config", "", "Config file (TOML)")
	cmd.PersistentFlags().String("host", "", "Server host")
	cmd.PersistentFlags().String("shard", "", "shard name")
	cmd.PersistentFlags().Int("port", 0, "Server port")
	cmd.PersistentFlags().String("db", "", "Database filename")
	cmd.PersistentFlags().String("log", "", "log level")

	_ = viper.BindPFlag("server.host", cmd.PersistentFlags().Lookup("host"))
	_ = viper.BindPFlag("server.port", cmd.PersistentFlags().Lookup("port"))
	_ = viper.BindPFlag("db.filename", cmd.PersistentFlags().Lookup("db"))
	_ = viper.BindPFlag("server.log_level", cmd.PersistentFlags().Lookup("log"))
	_ = viper.BindPFlag("server.shard_name", cmd.PersistentFlags().Lookup("shard"))

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

	if err := viper.ReadInConfig(); err != nil {
		fmt.Println("Error loading config:", err)
	}

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
