package main

import (
	"github.com/go-playground/validator/v10"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"strings"
)

type ServerConfig struct {
	Host     string `mapstructure:"host" validate:"required,hostname|ip"`
	Port     int    `mapstructure:"port" validate:"required,min=1,max=65535"`
	LogLevel string `mapstructure:"log_level" validate:"required,oneof=INFO WARNING DEBUG ERROR"`
}

type ShardConfig struct {
	Name  string
	Index int
}

type DatabaseConfig struct {
	Filename string `mapstructure:"filename" validate:"required"`
}

type Config struct {
	Server *ServerConfig
	Shards []*ShardConfig
	DB     *DatabaseConfig
}

func initDefaults() {
	viper.SetDefault("server.host", "127.0.0.1")
	viper.SetDefault("server.port", 4321)
	viper.SetDefault("db.filename", "pirin.db")
	viper.SetDefault("server.log_level", "INFO")
}

func setupFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().String("config", "", "Config file (TOML)")
	cmd.PersistentFlags().String("host", "", "Server host")
	cmd.PersistentFlags().Int("port", 0, "Server port")
	cmd.PersistentFlags().String("db", "", "Database filename")
	cmd.PersistentFlags().String("log", "", "log level")

	_ = viper.BindPFlag("server.host", cmd.PersistentFlags().Lookup("host"))
	_ = viper.BindPFlag("server.port", cmd.PersistentFlags().Lookup("port"))
	_ = viper.BindPFlag("db.filename", cmd.PersistentFlags().Lookup("db"))
	_ = viper.BindPFlag("server.log_level", cmd.PersistentFlags().Lookup("log"))

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
