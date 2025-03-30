package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
)

type Settings struct {
	Host     string
	Port     int
	UseHTTPS bool
}

const (
	version = "0.0.1"
)

func buildCommandUsage(cmd Command) string {
	usage := cmd.Name
	for _, param := range cmd.Params {
		usage += fmt.Sprintf(" <%s>", param.Name)
	}
	return usage
}

func main() {
	settings := Settings{}

	rootCmd := &cobra.Command{
		Use:   "pirin",
		Short: "Pirin CLI",
		Long:  `Pirin CLI is a tool to interact with the Pirin database server.`,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				interactiveMode(&settings)
			} else {
				_ = cmd.Help()
			}
		},
	}

	rootCmd.PersistentFlags().StringVar(&settings.Host, "host", "localhost", "Hostname for the server")
	rootCmd.PersistentFlags().IntVar(&settings.Port, "port", 4321, "Port for the server")
	rootCmd.PersistentFlags().BoolVar(&settings.UseHTTPS, "https", false, "Use HTTPS protocol")

	for _, cmd := range CommandsRegistry {
		command := cmd
		rootCmd.AddCommand(&cobra.Command{
			Use:   buildCommandUsage(command),
			Short: command.Description,
			Run: func(c *cobra.Command, args []string) {
				if len(args) != len(command.Params) {
					_, _ = colorRed.Printf("Invalid number of arguments. Expected %d but got %d\n",
						len(command.Params), len(args))
					return
				}
				err := command.Handler(args, &settings)
				if err != nil {
					_, _ = colorRed.Printf("Command error: %v\n", err)
				}
			},
		})
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
