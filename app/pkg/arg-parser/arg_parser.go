package argparser

import (
	"flag"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/pkg/config"
)

type ArgParser struct {
	flags      *flag.FlagSet
	dir        *string
	dbfilename *string
	port       *string
	role       *string
	replocaOf  *string
}

func NewArgParser() *ArgParser {
	parser := &ArgParser{
		flags: flag.NewFlagSet("redis", flag.ContinueOnError),
	}

	parser.dir = parser.flags.String("dir", "", "Directory for Redis files")
	parser.dbfilename = parser.flags.String("dbfilename", "", "Database filename")
	parser.port = parser.flags.String("port", "", "Port for Redis")
	parser.replocaOf = parser.flags.String("replicaof", "", "Master host and port (host port)")

	return parser
}

func (a *ArgParser) Parse(args []string) error {
	return a.flags.Parse(args)
}
func (a *ArgParser) ParseArgsToRedisConfig() config.RedisConfig {
	redisConfig := config.RedisConfig{
		Port:              "6379",
		Dir:               "",
		DbFileName:        "",
		Role:              config.RoleMaster,
		MasterHost:        "",
		MasterPort:        "",
		ReplicationId:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		ReplicationOffset: 0,
	}

	if a.dir != nil && *a.dir != "" {
		redisConfig.Dir = *a.dir
	}
	if a.dbfilename != nil && *a.dbfilename != "" {
		redisConfig.DbFileName = *a.dbfilename
	}
	if a.port != nil && *a.port != "" {
		redisConfig.Port = *a.port
	}

	if a.role != nil && *a.role != "" {
		redisConfig.Role = *a.role
	}
	if a.replocaOf != nil && *a.replocaOf != "" {
		redisConfig.Role = config.RoleSlave
		parts := strings.Split(*a.replocaOf, " ")
		if len(parts) == 2 {
			redisConfig.MasterHost = parts[0]
			redisConfig.MasterPort = parts[1]
		} else {
			redisConfig.MasterHost = *a.replocaOf
			redisConfig.MasterPort = "6379"
		}
	}

	return redisConfig
}
