package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/timson/pirindb/storage"
)

func newRestoreCommand() *cobra.Command {
	var snapshot, directory string
	cmd := &cobra.Command{Use: "restore", Short: "Restore a verified snapshot into a new standalone directory", RunE: func(cmd *cobra.Command, args []string) error {
		stats, err := storage.RestoreDBToDirectory(snapshot, directory, nil)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(cmd.OutOrStdout(), "Restored %d keys across %d buckets; verified READY manifest in %s\n", stats.KeysImported, stats.BucketsImported, directory)
		return err
	}}
	cmd.Flags().StringVar(&snapshot, "snapshot", "", "Snapshot v2 input path")
	cmd.Flags().StringVar(&directory, "directory", "", "New restore directory (must not exist)")
	_ = cmd.MarkFlagRequired("snapshot")
	_ = cmd.MarkFlagRequired("directory")
	return cmd
}
