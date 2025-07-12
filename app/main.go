package main

import (
	"fmt"
	"net"
	"os"

	argparser "github.com/codecrafters-io/redis-starter-go/app/pkg/arg-parser"
	commandhandler "github.com/codecrafters-io/redis-starter-go/app/pkg/command-handler"
	"github.com/codecrafters-io/redis-starter-go/app/pkg/config"
	redisfileparser "github.com/codecrafters-io/redis-starter-go/app/pkg/redis-file-parser"
	"github.com/codecrafters-io/redis-starter-go/app/pkg/storage"
)

func handleConnection(conn net.Conn, handler *commandhandler.CommandHandler) {
	defer conn.Close()
	for {
		buffer := make([]byte, 1024)
		length, err := conn.Read(buffer)
		if err != nil {
			fmt.Println("Error reading from connection:", err)
			return
		}

		if length > 0 {
			response, err := handler.Handle(buffer, length)
			if err != nil {
				fmt.Printf("Error handling command: %v\n", err)
				errorResponse := fmt.Sprintf("-ERR %s\r\n", err.Error())
				conn.Write([]byte(errorResponse))
				continue
			}

			if response != nil {
				redisResponse := response.ToRedisFormat()
				_, writeErr := conn.Write([]byte(redisResponse))
				if writeErr != nil {
					fmt.Printf("Error writing response: %v\n", writeErr)
					return
				}
			}
		}
	}
}

// Todo move them out of main
func HandleSlavePingStage(redisConfig config.RedisConfig) error {
	fmt.Println("Pinging master")

	masterAddr := fmt.Sprintf("%s:%s", redisConfig.MasterHost, redisConfig.MasterPort)
	conn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to master at %s: %v", masterAddr, err)
	}
	defer conn.Close()

	pingCommand := "*1\r\n$4\r\nPING\r\n"
	_, err = conn.Write([]byte(pingCommand))
	if err != nil {
		return fmt.Errorf("failed to send PING to master: %v", err)
	}

	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		return fmt.Errorf("failed to read PING response from master: %v", err)
	}

	response := string(buffer[:n])
	fmt.Printf("Received response from master: %s", response)

	if response == "+PONG\r\n" {
		fmt.Println("Slave pinged master successfully.")
		return nil
	}

	return fmt.Errorf("unexpected response from master: %s", response)
}

func main() {
	argParser := argparser.NewArgParser()
	err := argParser.Parse(os.Args[1:])
	if err != nil {
		fmt.Println("Error parsing arguments:", err)
		os.Exit(1)
	}
	argParserConfig := argParser.ParseArgsToRedisConfig()
	var dataStorage storage.StorageInterface
	if argParserConfig.Dir != "" {
		redisFileParser := redisfileparser.NewRedisFileParser(argParserConfig.Dir + "/" + argParserConfig.DbFileName)

		if redisFileParser.DoesRedisFileExists() {
			_, data := redisFileParser.ParseFile()
			dataStorage = storage.NewPersistanceStorage(data)
		} else {
			dataStorage = storage.NewInMemoryStorage()
		}

	} else {
		dataStorage = storage.NewInMemoryStorage()
	}
	handler := commandhandler.NewCommandHandler(dataStorage, argParserConfig)

	if argParserConfig.Role == config.RoleSlave {
		err = HandleSlavePingStage(argParserConfig)
		if err != nil {
			fmt.Printf("Error during slave ping stage: %v\n", err)
			os.Exit(1)
		}
	}

	fmt.Println("Logs from your program will appear here!")
	l, err := net.Listen("tcp", "0.0.0.0:"+argParserConfig.Port)
	if err != nil {
		fmt.Println("Failed to bind to port " + argParserConfig.Port)
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn, handler)
	}
}
