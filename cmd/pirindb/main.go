package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/timson/pirindb/storage"
	"os"
	"os/signal"
	"slices"
	"syscall"
)

func runServer(cfgFile string) {
	printLogo(version)
	printSystemInfo()
	cfgFile = "test_shards.toml"

	config, err := loadConfig(cfgFile)
	if err != nil {
		fmt.Println("Error loading config:", err)
		os.Exit(1)
	}

	logger := createLogger(config.Server.LogLevel)
	storage.SetLogger(logger)
	db, DBErr := storage.Open(config.DB.Filename, 0600)
	if DBErr != nil {
		fmt.Printf("Error opening database:\n  %v\n", DBErr)
		os.Exit(1)
	}

	server := NewServer(config, db, logger)
	if config.Server.ShardName != "" {
		shardIdx := slices.IndexFunc(config.Shards, func(shardConfig *ShardConfig) bool {
			return shardConfig.Name == config.Server.ShardName
		})
		// Run sharding mode
		if shardIdx != -1 {
			runShard(config, server, shardIdx)
		} else {
			logger.Error("shard not found in config", "name", config.Server.ShardName)
		}
	} else {
		logger.Info("running in single mode")
	}

	go func() {
		if err = server.Start(); err != nil {
			logger.Error("failed to start HTTP server", "error", err)
		}
	}()

	// graceful shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	logger.Info("shutting down...")

	if err = server.Stop(); err != nil {
		logger.Error("failed to stop HTTP server", "error", err)
	}

	logger.Info("shutdown complete")
}

func runShard(config *Config, server *Server, shardIdx int) {
	shardConfig := config.Shards[shardIdx]
	server.Shard = NewShard(shardConfig.Name, shardConfig.Host, shardConfig.Port)
	ch := NewConsistentHash()
	for _, sc := range config.Shards {
		shard := NewShard(sc.Name, sc.Host, sc.Port)
		ch.AddShard(shard)
	}
	server.RingV1 = ch
	server.Logger.Info("running as shard:", "name", shardConfig.Name, "host", shardConfig.Host, "port", shardConfig.Port)
	ch.Sync(server.DB)
}

func main() {
	rootCmd := &cobra.Command{
		Use:   "pirindb",
		Short: "PirinDB - KV database",
		Run: func(cmd *cobra.Command, args []string) {
			cfgFile, _ := cmd.Flags().GetString("config")
			runServer(cfgFile)
		},
	}

	initDefaults()
	setupFlags(rootCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println("command execution failed:", err)
		os.Exit(1)
	}
}
