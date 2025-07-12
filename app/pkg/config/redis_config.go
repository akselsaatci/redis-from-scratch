package config

const (
	RoleMaster = "master"
	RoleSlave  = "slave"
)

type RedisConfig struct {
	Port              string
	Dir               string
	DbFileName        string
	Role              string
	MasterHost        string
	MasterPort        string
	ReplicationId     string
	ReplicationOffset int64
}
