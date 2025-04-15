package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/timson/pirindb/storage"
	"os"
	"os/signal"
	"syscall"
)

func runServer(cfgFile string) {
	printLogo(version)
	printSystemInfo()

	config, err := loadConfig(cfgFile)
	if err != nil {
		fmt.Println("Error loading config:", err)
		os.Exit(1)
	}

	logger := createLogger(config.Server.LogLevel)
	storage.SetLogger(logger)
	db, DBErr := storage.Open(config.DB.Filename, nil)
	if DBErr != nil {
		fmt.Printf("Error opening database:\n  %v\n", DBErr)
		os.Exit(1)
	}

	server := NewServer(config, db, logger)

	go func() {
		if err = server.Start(); err != nil {
			logger.Error("Failed to start HTTP server", "error", err)
		}
	}()

	// graceful shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	logger.Info("Shutting down...")

	if err = server.Stop(); err != nil {
		logger.Error("Failed to stop HTTP server", "error", err)
	}

	logger.Info("Shutdown complete")
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
