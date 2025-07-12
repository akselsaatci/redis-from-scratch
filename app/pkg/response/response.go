package response

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/pkg/command"
)

type Response struct {
	Command command.Command
	Status  string
	Data    []string
	Error   string
	IsMulti bool
	// Needed this because redises weird behavior when
	// INFO REPLICATION command is called
	IsBulkStringArray bool
}

func (r *Response) ToRedisFormat() string {
	if r.Error != "" {
		//should make a custom error type for this
		if r.Error == "this data is expeired" {
			return "$-1\r\n"
		}

		return fmt.Sprintf("-ERR %s\r\n", r.Error)
	}
	if r.IsBulkStringArray {
		if len(r.Data) == 0 {
			return "$-1\r\n"
		}
		if len(r.Data) == 1 {
			return fmt.Sprintf("$%d\r\n%s\r\n", len(r.Data[0]),
				r.Data[0])
		}
		totalLength := 0
		for _, value := range r.Data {
			// plus one is for newline
			totalLength += len(value) + 1
		}
		result := fmt.Sprintf("$%d\r\n", totalLength)
		for _, value := range r.Data {
			result += fmt.Sprintf("%s\n", value)
		}
		result += "\r\n"
		return result
	}
	if len(r.Data) > 1 || r.IsMulti {
		result := fmt.Sprintf("*%d\r\n", len(r.Data))
		for _, value := range r.Data {
			result += fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
		}
		return result
	}

	if len(r.Data) == 1 && r.Data[0] != "" {
		return fmt.Sprintf("+%s\r\n", r.Data[0])
	}

	return "+OK\r\n"
}
