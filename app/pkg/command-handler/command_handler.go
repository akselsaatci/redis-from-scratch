package commandhandler

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/pkg/command"
	"github.com/codecrafters-io/redis-starter-go/app/pkg/config"
	redisparser "github.com/codecrafters-io/redis-starter-go/app/pkg/redis-parser"
	"github.com/codecrafters-io/redis-starter-go/app/pkg/response"
	"github.com/codecrafters-io/redis-starter-go/app/pkg/storage"
)

type CommandHandler struct {
	parser  *redisparser.RedisParser
	storage storage.StorageInterface
	config  config.RedisConfig
}

func NewCommandHandler(storage storage.StorageInterface, config config.RedisConfig) *CommandHandler {
	return &CommandHandler{
		parser:  redisparser.NewRedisParser(),
		storage: storage,
		config:  config,
	}
}

func (h *CommandHandler) Handle(data []byte, length int) (*response.Response, error) {
	if length <= 0 {
		return nil, fmt.Errorf("empty command")
	}

	command, err := h.parser.Parse(data[:length])
	if err != nil {
		fmt.Printf("Error parsing command: %v\n", err)
		return nil, fmt.Errorf("invalid command")
	}

	switch command.Name {
	case "PING":
		return h.createSuccessResponse(*command, "PONG"), nil

	case "ECHO":
		if err := h.validateArgsCount(command, 1, 1); err != nil {
			return h.createErrorResponse(*command, err.Error()), nil
		}
		return h.createSuccessResponse(*command, command.Args[0]), nil

	case "GET":
		if err := h.validateArgsCount(command, 1, 1); err != nil {
			return h.createErrorResponse(*command, err.Error()), nil
		}

		value, err := h.Get(command.Args[0])

		if err != nil {
			return h.createErrorResponse(*command, err.Error()), nil
		}
		return h.createSuccessResponse(*command, value), nil

	case "SET":
		if err := h.validateArgsCount(command, 2, 4); err != nil {
			return h.createErrorResponse(*command, err.Error()), nil
		}
		if len(command.Args) > 2 && strings.ToUpper(command.Args[2]) == "PX" {

			expeire, err := strconv.Atoi(command.Args[3])
			expeire64 := int64(expeire)

			if err != nil {
				return h.createErrorResponse(*command, err.Error()), nil
			}

			if err := h.Set(command.Args[0], command.Args[1], &expeire64); err != nil {
				return h.createErrorResponse(*command, err.Error()), nil
			}

		} else {
			if err := h.Set(command.Args[0], command.Args[1], nil); err != nil {
				return h.createErrorResponse(*command, err.Error()), nil
			}
		}

		return h.createSuccessResponse(*command, ""), nil

	case "CONFIG":
		if err := h.validateArgsCount(command, 2, 2); err != nil {
			return h.createErrorResponse(*command, err.Error()), nil
		}

		subCommand := strings.ToUpper(command.Args[0])
		if subCommand == "GET" {
			return h.handleConfigGet(command)
		}

		return h.createErrorResponse(*command, "unknown CONFIG subcommand"), nil

	case "KEYS":
		if err := h.validateArgsCount(command, 1, 1); err != nil {
			return h.createErrorResponse(*command, err.Error()), nil
		}

		subCommand := strings.ToUpper(command.Args[0])
		if subCommand == "*" {
			return h.handleKeysGet(command)
		}

		return h.createErrorResponse(*command, "unknown KEYS subcommand"), nil

	case "INFO":
		if err := h.validateArgsCount(command, 1, 1); err != nil {
			return h.createErrorResponse(*command, err.Error()), nil
		}

		subCommand := strings.ToUpper(command.Args[0])
		if subCommand == "REPLICATION" {
			return h.handleInfoReplication(command)
		}

		return h.createErrorResponse(*command, "unknown INFO subcommand"), nil
	default:
		return h.createErrorResponse(*command, "unknown command"), nil
	}
}

func (h *CommandHandler) handleConfigGet(command *command.Command) (*response.Response, error) {
	parameter := strings.ToLower(command.Args[1])

	switch parameter {
	case "dir":
		return h.createMultiDataResponse(*command, []string{"dir", h.config.Dir}, false), nil

	case "dbfilename":
		return h.createMultiDataResponse(*command, []string{"dbfilename", h.config.DbFileName}, false), nil
	default:
		return h.createMultiDataResponse(*command, []string{}, false), nil
	}
}

func (h *CommandHandler) Set(key, value string, experie *int64) error {
	return h.storage.Set(key, value, experie)
}

func (h *CommandHandler) Get(key string) (string, error) {
	return h.storage.Get(key)
}
func (h *CommandHandler) handleKeysGet(command *command.Command) (*response.Response, error) {
	keys := h.storage.GetAllKeys()
	if len(keys) == 0 {
		return h.createMultiDataResponse(*command, []string{}, false), nil
	}

	return h.createMultiDataResponse(*command, keys, false), nil
}

func (h *CommandHandler) handleInfoReplication(command *command.Command) (*response.Response, error) {

	data := []string{
		"role:" + h.config.Role,
		"master_replid:" + h.config.ReplicationId,
		"master_repl_offset:" + fmt.Sprintf("%d", h.config.ReplicationOffset),
	}
	return h.createMultiDataResponse(*command, data, true), nil
}
func (h *CommandHandler) createSuccessResponse(command command.Command, data string) *response.Response {
	return &response.Response{
		Command: command,
		Status:  "OK",
		Data:    []string{data},
	}
}

func (h *CommandHandler) createErrorResponse(command command.Command, errorMsg string) *response.Response {
	return &response.Response{
		Command: command,
		Status:  "ERR",
		Error:   errorMsg,
	}
}

func (h *CommandHandler) createMultiDataResponse(command command.Command, data []string, isBulkString bool) *response.Response {
	return &response.Response{
		Command:           command,
		Status:            "OK",
		Data:              data,
		IsMulti:           true,
		IsBulkStringArray: isBulkString,
	}
}

func (h *CommandHandler) validateArgsCount(command *command.Command, expectedMin, expectedMax int) error {
	argsCount := len(command.Args)
	if expectedMax == -1 { // for unlimited args
		if argsCount < expectedMin {
			return fmt.Errorf("wrong number of arguments for '%s' command expected at least %d", command.Name, expectedMin)
		}
	}
	if argsCount < expectedMin || argsCount > expectedMax {
		return fmt.Errorf("wrong number of arguments for '%s' command expected between %d and %d got %d", command.Name, expectedMin, expectedMax, argsCount)
	}

	return nil
}

type Command = command.Command
