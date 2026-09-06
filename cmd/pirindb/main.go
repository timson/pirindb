package main

import (
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/timson/pirindb/storage"
	"os"
	"os/signal"
	"syscall"
)

func runServer(cfgFile string) error {
	printLogo(version)
	printSystemInfo()

	config, err := loadConfig(cfgFile)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger := createLogger(config.Server.LogLevel)
	storage.SetLogger(logger)
	db, DBErr := storage.Open(config.DB.Filename, config.DB.StorageOptions())
	if DBErr != nil {
		return fmt.Errorf("open database: %w", DBErr)
	}

	server := NewServer(config, db, logger)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sig)
	return serveUntilSignal(server, sig)
}

func serveUntilSignal(server *Server, sig <-chan os.Signal) error {
	started := make(chan error, 1)
	go func() { started <- server.Start() }()
	var serveErr error
	select {
	case serveErr = <-started:
	case <-sig:
		stopErr := server.Stop()
		serveErr = <-started
		if errors.Is(serveErr, errServerStopping) {
			serveErr = nil
		}
		// A failed stop can leave background work using the database. Let process
		// exit and journal recovery handle that case instead of closing under it.
		if stopErr != nil {
			return errors.Join(serveErr, stopErr)
		}
		return errors.Join(serveErr, server.DB.Close())
	}
	if stopErr := server.Stop(); stopErr != nil {
		return errors.Join(serveErr, stopErr)
	}
	return errors.Join(serveErr, server.DB.Close())
}

func main() {
	rootCmd := &cobra.Command{
		Use:   "pirindb",
		Short: "PirinDB - KV database",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfgFile, _ := cmd.Flags().GetString("config")
			return runServer(cfgFile)
		},
	}

	initDefaults()
	setupFlags(rootCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println("command execution failed:", err)
		os.Exit(1)
	}
}
