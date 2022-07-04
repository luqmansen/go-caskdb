package caskdb

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
)

const (
	hintFilesPrefix = "keydir"
)

type keyDirEntry struct {
	//Timestamp in unixnano
	Timestamp int64
	//LocationOffset is the file offset of current entry from initial position (0)
	LocationOffset int64
	// DataLength is the length of bytes of current entry only,
	// how to use this to get current entry:
	// data = currentOffset + DataLength
	DataLength int64

	// tombstone value, will mark current entry as delete, and on the next merging process
	// this entry will be gone
	isDeleted bool
}

type DiskStorage struct {
	*sync.Mutex
	file *os.File

	currentOffset int64
	// map the key with the offset position of the value
	keyDir map[string]*keyDirEntry
}

func NewDiskStorage(filename string) *DiskStorage {
	ds := &DiskStorage{
		currentOffset: io.SeekStart,
		Mutex:         &sync.Mutex{},
		file:          getOrCreateFile(filename),
	}
	ds.initKeyDir()
	return ds
}

func (s *DiskStorage) Set(key, value []byte) error {
	s.Lock()
	defer s.Unlock()

	data := newEntry(time.Now().UnixNano(), key, value)
	dataSize, databyte := data.encode()

	_, err := s.file.WriteAt(databyte, s.currentOffset)
	if err != nil {
		return err
	}
	s.keyDir[string(key)] = &keyDirEntry{
		LocationOffset: s.currentOffset,
		DataLength:     dataSize,
	}
	s.currentOffset += dataSize
	return nil
}

func (s *DiskStorage) Get(key []byte) ([]byte, error) {
	s.Lock()
	defer s.Unlock()

	if val, found := s.keyDir[string(key)]; !found || val.isDeleted {
		return nil, nil
	}

	keyData := s.keyDir[string(key)]
	data := make([]byte, keyData.DataLength)
	_, err := s.file.ReadAt(data, keyData.LocationOffset)
	if err != nil {
		return nil, err
	}

	dataEntry := decodeKV(data)

	return dataEntry.value, nil
}

//Delete will only add "tombstone" value to entry, deletion on disk
//will be performed when there is a merging process
func (s *DiskStorage) Delete(key []byte) {
	s.Lock()
	defer s.Unlock()

	if v, found := s.keyDir[string(key)]; found {
		v.isDeleted = true
	}
}

func (s *DiskStorage) initKeyDir() {
	hintFiles := fmt.Sprintf("%s_%s", s.file.Name(), hintFilesPrefix)
	b, err := ioutil.ReadFile(hintFiles)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		panic(err)
	}

	keyDir := make(map[string]*keyDirEntry)
	// hint files does not exist, will load key from entire db file one-by-one
	if errors.Is(err, os.ErrNotExist) {
		header := make([]byte, defaultHeaderLength)

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

			headerData := decodeHeader(header)

			key := make([]byte, headerData.keySize)
			_, err = s.file.Read(key)
			if err != nil {
				panic(err)
			}

			value := make([]byte, headerData.valueSize)
			_, err = s.file.Read(value)
			if err != nil {
				panic(err)
			}

			totalSize := defaultHeaderLength + headerData.keySize + headerData.valueSize
			keyDir[string(key)] = &keyDirEntry{
				Timestamp:      headerData.timestamp,
				LocationOffset: currOffset,
				DataLength:     int64(totalSize),
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
	s.Lock()
	defer s.Unlock()

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
