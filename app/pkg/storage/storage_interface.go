package storage

type StorageInterface interface {
	Get(key string) (string, error)
	Set(key string, value string, experie *int64) error
	GetAllKeys() []string
}
