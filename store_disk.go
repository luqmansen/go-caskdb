package go_caskdb

import (
	"io"
	"os"
	"sync"
	"time"
)

type keyDirData struct {
	location int64
	// dataLength is the length of bytes data
	dataLength int64
}

type DiskStorage struct {
	currentOffset int64
	// map the key with the offset position of the value
	keyDir map[string]keyDirData

	mu   *sync.Mutex
	file *os.File
}

func NewDiskStorage(filename string) *DiskStorage {
	//todo: check if file already exists
	f, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	//todo: load existing keydir

	return &DiskStorage{
		currentOffset: io.SeekStart,
		keyDir:        make(map[string]keyDirData),
		file:          f,
		mu:            &sync.Mutex{},
	}
}
func (s DiskStorage) Set(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ts := uint32(time.Now().UnixNano())
	dataSize, data := encodeKV(ts, key, value)

	_, err := s.file.WriteAt(data, s.currentOffset)
	if err != nil {
		return err
	}
	s.keyDir[key] = keyDirData{
		location:   s.currentOffset,
		dataLength: dataSize,
	}
	s.currentOffset += dataSize
	return nil
}

func (s DiskStorage) Get(key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, found := s.keyDir[key]; !found {
		return "", nil
	}

	keyData := s.keyDir[key]
	data := make([]byte, keyData.dataLength)
	_, err := s.file.ReadAt(data, keyData.location)
	if err != nil {
		return "", err
	}

	_, _, v := decodeKV(data)

	return v, nil
}
