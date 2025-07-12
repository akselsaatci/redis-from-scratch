package storage

import (
	"fmt"
	"time"
)

type PersistanceStorage struct {
	data map[string]Data
	// filePath string
	// file     *os.File
}

func NewPersistanceStorage(data map[string]Data) *PersistanceStorage {
	// // here i want to open a file if it exists, or create it if it doesn't
	// file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	// if err != nil {
	// 	fmt.Printf("Error opening file: %v\n", err)
	// 	os.Exit(1)
	// }
	// // latestSnapshot := make(map[string]Data)

	// fileBytes := make([]byte, 0, 1024)
	// file.Read(fileBytes)

	// if len(fileBytes) > 0 {

	// }

	return &PersistanceStorage{data: data}
}

func (s *PersistanceStorage) Get(key string) (string, error) {
	data, ok := s.data[key]

	if !ok {
		return "", fmt.Errorf("this key is not setted")
	}
	if data.ExpeireEnabled && data.ExpeireDate < time.Now().UnixMilli() {
		return "", fmt.Errorf("this data is expeired")

	}

	return data.Value, nil
}

func (s *PersistanceStorage) Set(key string, value string, experie *int64) error {
	if experie != nil {
		expeireDate := time.Now().UnixMilli() + *experie
		s.data[key] = Data{Value: value, ExpeireDate: expeireDate, ExpeireEnabled: true}
		return nil
	}

	s.data[key] = Data{Value: value, ExpeireDate: 0, ExpeireEnabled: false}
	return nil
}
func (s *PersistanceStorage) GetAllKeys() []string {
	keys := make([]string, 0, len(s.data))
	for key := range s.data {
		keys = append(keys, key)
	}
	return keys
}
