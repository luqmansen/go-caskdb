package go_caskdb

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	hintFilesPrefix = "keydir"
)

type keyEntry struct {
	//Timestamp in unixnano
	Timestamp uint32
	//Location is the file offset of current entry from initial position (0)
	Location int64
	// DataLength is the length of bytes of current entry only,
	// how to use this to get current entry:
	// data = currentOffset + DataLength
	DataLength int64

	// tombstone value, will mark current entry as delete, and on the next merging process
	// this entry will be gone
	isDeleted bool
}

type DiskStorage struct {
	currentOffset int64
	// map the key with the offset position of the value
	keyDir map[string]*keyEntry

	mu   *sync.Mutex
	file *os.File
}

func NewDiskStorage(filename string) *DiskStorage {
	ds := &DiskStorage{
		currentOffset: io.SeekStart,
		mu:            &sync.Mutex{},
		file:          getOrCreateFile(filename),
	}
	ds.initKeyDir()
	return ds
}

func (s *DiskStorage) Set(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ts := uint32(time.Now().UnixNano())
	dataSize, data := encodeKV(ts, key, value)

	_, err := s.file.WriteAt(data, s.currentOffset)
	if err != nil {
		return err
	}
	s.keyDir[key] = &keyEntry{
		Location:   s.currentOffset,
		DataLength: dataSize,
	}
	s.currentOffset += dataSize
	return nil
}

func (s *DiskStorage) Get(key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if val, found := s.keyDir[key]; !found || val.isDeleted {
		return "", nil
	}

	keyData := s.keyDir[key]
	data := make([]byte, keyData.DataLength)
	_, err := s.file.ReadAt(data, keyData.Location)
	if err != nil {
		return "", err
	}

	_, _, v := decodeKV(data)

	return v, nil
}

//Delete will only add "tombstone" value to entry, deletion on disk
//will be performed when there is a merging process
func (s *DiskStorage) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if v, found := s.keyDir[key]; found {
		v.isDeleted = true
	}
}

func (s *DiskStorage) initKeyDir() {
	hintFiles := fmt.Sprintf("%s_%s", s.file.Name(), hintFilesPrefix)
	b, err := ioutil.ReadFile(hintFiles)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		panic(err)
	}

	keyDir := make(map[string]*keyEntry)
	// hint files does not exist, will load key from entire db file one-by-one
	if errors.Is(err, os.ErrNotExist) {
		header := make([]byte, headerLength)

		var currOffset int64
		for {
			_, err := s.file.Read(header)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				} else {
					panic(err)
				}
			}

			ts, keySize, valueSize := decodeHeader(header)
			log.Debug(ts, keySize, valueSize)

			key := make([]byte, keySize)
			_, err = s.file.Read(key)
			if err != nil {
				panic(err)
			}

			value := make([]byte, valueSize)
			_, err = s.file.Read(value)
			if err != nil {
				panic(err)
			}

			totalSize := headerLength + keySize + valueSize
			keyDir[string(key)] = &keyEntry{
				Timestamp:  ts,
				Location:   currOffset,
				DataLength: int64(totalSize),
			}

			currOffset += int64(totalSize)
		}
	} else {
		// hint files exist, will load key that
		buff := bytes.NewBuffer(b)
		d := gob.NewDecoder(buff)

		err = d.Decode(&keyDir)
		if err != nil {
			panic(err)
		}
	}
	s.keyDir = keyDir
}

func getOrCreateFile(filename string) *os.File {
	var f *os.File
	if _, err := os.Stat(filename); errors.Is(err, os.ErrNotExist) {
		f, err = os.Create(filename)
		if err != nil {
			panic(err)
		}
	} else {
		f, err = os.Open(filename)
		if err != nil {
			panic(err)
		}
	}

	return f
}

func (s DiskStorage) Close() error {
	return s.flush()
}

func (s *DiskStorage) flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// flush key dir to hint files
	filename := fmt.Sprintf("%s_%s", s.file.Name(), hintFilesPrefix)
	f := getOrCreateFile(filename)

	b := new(bytes.Buffer)
	e := gob.NewEncoder(b)
	err := e.Encode(s.keyDir)
	if err != nil {
		panic(err)
	}
	_, err = f.Write(b.Bytes())
	return err
}
