package storage

import (
	"fmt"
	"time"
)

type Data struct {
	Value          string
	ExpeireEnabled bool
	ExpeireDate    int64
}

type InMemoryStorage struct {
	data map[string]Data
}

func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{data: make(map[string]Data)}
}

func (s *InMemoryStorage) Get(key string) (string, error) {
	data, ok := s.data[key]

	if !ok {
		return "", fmt.Errorf("this key is not setted")
	}
	if data.ExpeireEnabled && data.ExpeireDate < time.Now().UnixMilli() {
		return "", fmt.Errorf("this data is expeired")

	}

	return data.Value, nil
}

func (s *InMemoryStorage) Set(key string, value string, experie *int64) error {
	if experie != nil {
		expeireDate := time.Now().UnixMilli() + *experie
		s.data[key] = Data{Value: value, ExpeireDate: expeireDate, ExpeireEnabled: true}
		return nil
	}

	s.data[key] = Data{Value: value, ExpeireDate: 0, ExpeireEnabled: false}
	return nil
}
func (s *InMemoryStorage) GetAllKeys() []string {
	keys := make([]string, 0, len(s.data))
	for key := range s.data {
		keys = append(keys, key)
	}
	return keys
}
