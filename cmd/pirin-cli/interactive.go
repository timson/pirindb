package main

import (
	"fmt"
	"github.com/chzyer/readline"
	"github.com/fatih/color"
	"github.com/mattn/go-isatty"
	"os"
	"path/filepath"
	"strings"
)

var (
	colorRed        = color.New(color.FgRed)
	colorGreen      = color.New(color.FgGreen)
	colorCyanBold   = color.New(color.FgCyan, color.Bold)
	colorCyan       = color.New(color.FgCyan)
	colorYellowBold = color.New(color.FgYellow, color.Bold)
	colorYellow     = color.New(color.FgYellow)
)

func getHistoryFilePath() (string, error) {
	configDir, err := os.UserConfigDir() // Get user's config directory
	if err != nil {
		return "", err
	}
	return filepath.Join(configDir, "pirin-cli", "pirin_history"), nil
}

func interactiveMode(settings *Settings) {
	if !isatty.IsTerminal(os.Stdout.Fd()) {
		fmt.Println("error: interactive mode must be run in a terminal")
		os.Exit(1)
	}

	historyPath, err := getHistoryFilePath()
	if err != nil {
		historyPath = ".pirin_history"
	} else {
		if err := os.MkdirAll(filepath.Dir(historyPath), os.ModePerm); err != nil {
			fmt.Printf("Error creating history file directory: %v\n", err)
			os.Exit(1)
		}
	}

	rl, err := setupReadline(settings, historyPath)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = rl.Close()
	}()

	handleUserInput(rl, settings)
}

func setupReadline(settings *Settings, historyPath string) (*readline.Instance, error) {
	prompt := fmt.Sprintf("%s:%d> ", settings.Host, settings.Port)

	rl, err := readline.NewEx(&readline.Config{
		Prompt:      prompt,
		HistoryFile: historyPath,
		AutoComplete: readline.NewPrefixCompleter(
			readline.PcItem("help"),
			readline.PcItem("exit"),
		),
	})
	return rl, err
}

func handleUserInput(rl *readline.Instance, settings *Settings) {
	for {
		line, err := rl.Readline()
		if err != nil {
			break
		}

		line = strings.TrimSpace(line)
		if line == "" || line == "help" {
			_, _ = colorYellowBold.Println("Available commands:")
			for _, cmd := range CommandsRegistry {
				fmt.Printf("  %s - %s\n", colorCyanBold.Sprint(cmd.Name), colorGreen.Sprint(cmd.Description))
			}
			continue
		}

		if strings.HasPrefix(line, "help ") {
			parts := strings.Fields(line)
			if len(parts) < 2 {
				fmt.Println(color.RedString("Error: 'help' command requires a command name."))
				continue
			}
			commandName := parts[1]

			found := false
			for _, cmd := range CommandsRegistry {
				if cmd.Name == commandName {
					fmt.Printf("%s %s\n", colorYellow.Sprint("Command:"), colorGreen.Sprint(cmd.Name))
					fmt.Printf("%s %s\n", colorYellow.Sprint("Description:"), colorGreen.Sprint(cmd.Description))
					if len(cmd.Params) > 0 {
						_, _ = colorYellow.Println("Parameters:")
						for _, param := range cmd.Params {
							fmt.Printf(" <%s: %s>\n", colorCyan.Sprint(param.Name), colorGreen.Sprint(param.Description))
						}
					} else {
						_, _ = colorYellow.Println("This command has no parameters.")
					}
					found = true
					break
				}
			}
			if !found {
				_, _ = colorRed.Printf("Unknown command: '%s'\n", commandName)
			}
			continue
		}

		cmd, params, err := FindCommand(line)
		if err != nil {
			_, _ = colorRed.Printf("Error: %v\n", err)
			continue
		}

		if cmd != nil {
			if err = cmd.Handler(params, settings); err != nil {
				_, _ = colorRed.Printf("Command error: %v\n", err)
			}
		}
	}
}
