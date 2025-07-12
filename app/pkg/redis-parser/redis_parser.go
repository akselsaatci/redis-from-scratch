package redisparser

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/pkg/command"
)

type RedisParser struct {
}

func NewRedisParser() *RedisParser {
	return &RedisParser{}
}

func (r *RedisParser) Parse(data []byte) (*Command, error) {
	input := string(data)
	input = strings.TrimSpace(input)

	if len(input) == 0 {
		return nil, fmt.Errorf("empty input")
	}

	if input[0] != '*' {
		return nil, fmt.Errorf("No Command found")
	}

	return r.parseRESPArray(input)
}

func (r *RedisParser) parseRESPArray(input string) (*Command, error) {
	parts := strings.Split(input, "\r\n")

	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid RESP input")
	}

	if parts[0][0] != '*' {
		return nil, fmt.Errorf("input is not RESP array")
	}
	argCount, err := strconv.Atoi(parts[0][1:])
	if err != nil {
		return nil, fmt.Errorf("invalid array length: %v", err)
	}

	command := Command{}
	for i := 0; i < argCount; i++ {
		length := parts[1+(i*2)]
		if len(length) == 0 || length[0] != '$' {
			return nil, fmt.Errorf("No string length indicator")
		}

		strLen, err := strconv.Atoi(length[1:])
		if err != nil {
			return nil, fmt.Errorf("String length indicator is not an integer: %v", err)
		}

		data := parts[2+(i*2)]
		if len(data) != strLen {
			return nil, fmt.Errorf("data length mismatch: expected %d, got %d", strLen, len(data))
		}

		if i == 0 {
			command.Name = strings.ToUpper(data)
		} else {
			command.Args = append(command.Args, data)
		}
	}

	return &command, nil
}

type Command = command.Command
