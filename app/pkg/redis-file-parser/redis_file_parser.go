package redisfileparser

import (
	"bufio"
	"fmt"
	"log"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/pkg/config"
	"github.com/codecrafters-io/redis-starter-go/app/pkg/storage"
)

type RedisFileParser struct {
	filePath      string
	doesFileExist bool
	readerWriter  *bufio.ReadWriter
}

func (r *RedisFileParser) DoesRedisFileExists() bool {
	return r.doesFileExist
}

func NewRedisFileParser(filePath string) *RedisFileParser {

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return &RedisFileParser{
			filePath:      filePath,
			readerWriter:  nil,
			doesFileExist: false,
		}
	}

	file, err := os.OpenFile(filePath, os.O_RDWR, 0666)
	if err != nil {
		log.Fatalf("Error opening file: %v\n", err)
	}
	reader := bufio.NewReader(file)
	writer := bufio.NewWriter(file)
	readerWriter := bufio.NewReadWriter(reader, writer)

	return &RedisFileParser{
		filePath:      filePath,
		readerWriter:  readerWriter,
		doesFileExist: true,
	}
}
func (r *RedisFileParser) ParseFile() (*config.FileConfig, map[string]storage.Data) {
	redisFlag := make([]byte, 5)
	_, err := r.readerWriter.Read(redisFlag)

	if err != nil {
		log.Fatalf("corrupted file: %v", err)
	}
	version := make([]byte, 4)
	_, err = r.readerWriter.Read(version)
	if err != nil {
		log.Fatalf("error reading version: %v", err)
	}

	fmt.Printf("Redis version: %s\n", string(version))
	data := make(map[string]storage.Data)

	metadata := make(map[string]string)

	databaseValue := byte(0)
	sizeOfCorrospendingHashTable := ""

	endOfFile := false

	for !endOfFile {
		nextByte, err := r.readerWriter.ReadByte()
		if err != nil {
			log.Fatalf("error reading next byte: %v", err)
		}

		switch nextByte {
		case 0xFA:
			key, err := r.readString()
			if err != nil {
				log.Fatalf("error reading metadata key: %v", err)
			}

			value, err := r.readValue()
			if err != nil {
				log.Fatalf("error reading metadata value: %v", err)
			}

			metadata[key] = value
			fmt.Printf("Metadata: %s = %s\n", key, value)
		case 0xFE:
			databaseValue, err = r.readerWriter.ReadByte()
			if err != nil {
				log.Fatalf("error reading database value: %v", err)
			}
		case 0xFB:
			bytes := make([]byte, 2)
			r.readerWriter.Read(bytes)
			sizeOfCorrospendingHashTable = string(bytes)
			fmt.Printf("Size of corresponding hash table: %s\n", sizeOfCorrospendingHashTable)
			if err != nil {
				log.Fatalf("error reading size of corresponding hash table: %v", err)
			}
		case 0xFF:
			fmt.Println("End of file reached")
			endOfFile = true

		case 0xFD:
			expeireBytes := make([]byte, 4)
			r.readerWriter.Read(expeireBytes)
			expireValue := int(expeireBytes[0]) | int(expeireBytes[1])<<8 |
				int(expeireBytes[2])<<16 | int(expeireBytes[3])<<24

			// Here i am skipping the type byte because we are just implementing the 00 and it is String Encoding
			// it is not correct
			// https://rdb.fnordig.de/file_format.html#value-type
			_, _ = r.readerWriter.ReadByte()

			key, err := r.readString()
			if err != nil {
				log.Fatalf("error reading metadata key: %v", err)
			}

			value, err := r.readString()
			if err != nil {
				log.Fatalf("error reading key and value: %v", err)
			}
			data[key] = storage.Data{
				Value:          value,
				ExpeireDate:    int64(expireValue) * 1000,
				ExpeireEnabled: true,
			}
			if err != nil {
				log.Fatalf("error reading expire bytes: %v", err)
			}

		case 0xFC:
			expeireBytes := make([]byte, 8)
			r.readerWriter.Read(expeireBytes)
			expireValue := int(expeireBytes[0]) | int(expeireBytes[1])<<8 |
				int(expeireBytes[2])<<16 | int(expeireBytes[3])<<24 |
				int(expeireBytes[4])<<32 | int(expeireBytes[5])<<40 |
				int(expeireBytes[6])<<48 | int(expeireBytes[7])<<56

			// Here i am skipping the type byte because we are just implementing the 00 and it is String Encoding
			// it is not correct
			// https://rdb.fnordig.de/file_format.html#value-type
			_, _ = r.readerWriter.ReadByte()

			key, err := r.readString()
			if err != nil {
				log.Fatalf("error reading metadata key: %v", err)
			}

			value, err := r.readString()
			if err != nil {
				log.Fatalf("error reading key and value: %v", err)
			}
			data[key] = storage.Data{
				Value:          value,
				ExpeireDate:    int64(expireValue),
				ExpeireEnabled: true,
			}
			if err != nil {
				log.Fatalf("error reading expire bytes: %v", err)
			}
		case 0x00:
			key, err := r.readString()
			if err != nil {
				log.Fatalf("error reading metadata key: %v", err)
			}

			value, err := r.readString()
			if err != nil {
				log.Fatalf("error reading key and value: %v", err)
			}
			data[key] = storage.Data{
				Value:          value,
				ExpeireDate:    0,
				ExpeireEnabled: false,
			}
		}
	}
	fileConfig := config.FileConfig{
		Version:  string(version),
		MetaData: metadata,
		// should not be string change this later
		Db: string(databaseValue),
	}
	return &fileConfig, data
}

func (r *RedisFileParser) readString() (string, error) {
	//https://rdb.fnordig.de/file_format.html#string-encoding
	length, err := r.readerWriter.ReadByte()
	if err != nil {
		return "", err
	}

	data := make([]byte, length)
	_, err = r.readerWriter.Read(data)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func (r *RedisFileParser) readValue() (string, error) {
	typeByte, err := r.readerWriter.ReadByte()
	if err != nil {
		return "", err
	}
	firtsByteOfType := typeByte & 0xF0
	switch firtsByteOfType {
	case 0x00: //The next 6 bits represent the length
		makedValue := typeByte & 0x3F
		if makedValue == 0 {
			return "", nil
		}
		data := make([]byte, makedValue)
		_, err := r.readerWriter.Read(data)
		if err != nil {
			return "", err
		}

		if intStr, ok := r.parseDataToInt(data); ok {
			return intStr, nil
		}
		return string(data), nil
	case 0x10: //Read one additional byte. The combined 14 bits represent the length
		additionalByte, err := r.readerWriter.ReadByte()
		if err != nil {
			return "", err
		}
		firstSixBytes := typeByte & 0x3F
		resultLen := (int(firstSixBytes) << 8) | int(additionalByte)

		if resultLen == 0 {
			return "", nil
		}
		data := make([]byte, resultLen)
		_, err = r.readerWriter.Read(data)
		if err != nil {
			return "", err
		}

		if intStr, ok := r.parseDataToInt(data); ok {
			return intStr, nil
		}
		return string(data), nil

	case 0x20: //Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
		lenBytes := make([]byte, 4)
		_, err := r.readerWriter.Read(lenBytes)
		if err != nil {
			return "", err
		}
		val := int(lenBytes[0]) |
			int(lenBytes[1])<<8 |
			int(lenBytes[2])<<16 |
			int(lenBytes[3])<<24
		if val == 0 {
			return "", nil
		}
		data := make([]byte, val)
		_, err = r.readerWriter.Read(data)
		if err != nil {
			return "", err
		}
		if intStr, ok := r.parseDataToInt(data); ok {
			return intStr, nil
		}
		return string(data), nil
	case 0x30: //The next 6 bits represent the length
		switch typeByte {
		case 0xC0:
			// 8-bit integer
			val, err := r.readerWriter.ReadByte()
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%d", val), nil
		case 0xC1:
			// 16-bit integer
			data := make([]byte, 2)
			_, err := r.readerWriter.Read(data)
			if err != nil {
				return "", err
			}
			val := uint16(data[0]) | uint16(data[1])<<8
			return fmt.Sprintf("%d", val), nil
		case 0xC2:
			//32-bit integer
			data := make([]byte, 4)
			_, err := r.readerWriter.Read(data)
			if err != nil {
				return "", err
			}
			val := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24
			return fmt.Sprintf("%d", val), nil
		default:
			// compressed string
			// dont support compressed strings yet
			log.Fatalf("not supported compressed strings")
		}
	}

	switch typeByte {
	case 0xC0:
		// 8-bit integer
		val, err := r.readerWriter.ReadByte()
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d", val), nil
	case 0xC1:
		data := make([]byte, 2)
		_, err := r.readerWriter.Read(data)
		if err != nil {
			return "", err
		}
		val := uint16(data[0]) | uint16(data[1])<<8
		return fmt.Sprintf("%d", val), nil
	case 0xC2:
		data := make([]byte, 4)
		_, err := r.readerWriter.Read(data)
		if err != nil {
			return "", err
		}
		val := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24
		return fmt.Sprintf("%d", val), nil
	default:
		// String with length prefix
		data := make([]byte, typeByte)
		_, err := r.readerWriter.Read(data)
		if err != nil {
			return "", err
		}
		return string(data), nil
	}
}

func (r *RedisFileParser) parseDataToInt(data []byte) (string, bool) {
	length := len(data)
	var intValue int
	switch length {
	case 1:
		intValue = int(data[0])
	case 2:
		intValue = int(data[0]) | int(data[1])<<8
	case 4:
		intValue = int(data[0]) | int(data[1])<<8 | int(data[2])<<16 | int(data[3])<<24
	case 8:
		intValue = int(data[0]) | int(data[1])<<8 | int(data[2])<<16 | int(data[3])<<24 |
			int(data[4])<<32 | int(data[5])<<40 | int(data[6])<<48 | int(data[7])<<56
	default:
		return "", false
	}
	return fmt.Sprintf("%d", intValue), true
}
