package main

import (
	"errors"
	"fmt"
	"strings"
)

// Command represents a generic command with its name, parameters, and validation rules.
type Command struct {
	Name        string
	Description string
	Params      []Param
	Handler     func(params []string, settings *Settings) error
}

// Param represents a parameter for a command.
type Param struct {
	Name        string
	Type        string
	Description string
}

var CommandsRegistry = []Command{
	{
		Name:        "set",
		Description: "Set a value for a given key",
		Params: []Param{
			{Name: "key", Type: "string", Description: "The key to set"},
			{Name: "value", Type: "string", Description: "The value to set"},
		},
		Handler: handleSetCommand,
	},
	{
		Name:        "get",
		Description: "Retrieves a value for a given key",
		Params: []Param{
			{Name: "key", Type: "string", Description: "The key to retrieve"},
		},
		Handler: handleGetCommand,
	},
	{
		Name:        "del",
		Description: "Delete a given key",
		Params: []Param{
			{Name: "key", Type: "string", Description: "The key to delete"},
		},
		Handler: handleDeleteCommand,
	},
	{
		Name:        "status",
		Description: "Request a status from the server",
		Params:      []Param{},
		Handler:     handleStatusCommand,
	},
}

func FindCommand(input string) (*Command, []string, error) {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return nil, nil, errors.New("no command provided")
	}
	commandName := parts[0]
	params := parts[1:]

	for _, cmd := range CommandsRegistry {
		if cmd.Name == commandName {
			if len(params) != len(cmd.Params) {
				return nil, nil, fmt.Errorf("invalid number of parameters for command '%s'", commandName)
			}
			return &cmd, params, nil
		}
	}
	return nil, nil, fmt.Errorf("unknown command: '%s'", commandName)
}
