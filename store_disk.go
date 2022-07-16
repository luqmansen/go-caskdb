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
	hintFilesExtension = "hint"
)

var errRecordNotFound = errors.New("record not found")

type keyDirEntry struct {
	//Timestamp in unixnano
	Timestamp int64
	//LocationOffset is the file offset of current entry from initial position (0)
	LocationOffset int64
	// DataLength is the length of bytes of current entry only,
	// how to use this to get current entry:
	// data = currentOffset + DataLength
	DataLength int64
}

type DiskStorage struct {
	*sync.RWMutex

	activeFile string
	// map the key with the offset position of the value
	keyDir map[string]*keyDirEntry

	file *datafile
}

func NewDiskStorage(filename string) *DiskStorage {
	ds := &DiskStorage{
		RWMutex:    &sync.RWMutex{},
		file:       openDataFile(filename),
		keyDir:     make(map[string]*keyDirEntry),
		activeFile: filename,
	}
	ds.initKeyDir()
	return ds
}

func (s *DiskStorage) Set(key, value []byte) error {
	data := newEntry(time.Now().UnixNano(), key, value)
	dataSize, databyte := data.encode()

	_, offset, err := s.file.Write(databyte)
	if err != nil {
		return err
	}

	s.Lock()
	defer s.Unlock()
	s.keyDir[string(key)] = &keyDirEntry{
		// offset represent current offset after this data is written
		// thus, the location of current data should be subtracted by the
		// size of current data
		Timestamp:      time.Now().UnixNano(),
		LocationOffset: offset - dataSize,
		DataLength:     dataSize,
	}

	return nil
}

func (s *DiskStorage) Get(key []byte) ([]byte, error) {
	s.RLock()
	defer s.RUnlock()
	keyData, found := s.keyDir[string(key)]
	if !found {
		return nil, errRecordNotFound
	}

	data := make([]byte, keyData.DataLength)
	_, err := s.file.ReadAt(data, keyData.LocationOffset)
	if err != nil {
		return nil, err
	}

	dataEntry := decodeEntry(data)

	return dataEntry.value, nil
}

//Delete will only add "tombstone" value to entry, deletion on disk
//will be performed when there is a merging process
func (s *DiskStorage) Delete(key []byte) {
	panic("implement me!")
}

func (s *DiskStorage) initKeyDir() {
	hintFiles := fmt.Sprintf("%s.%s", s.file.Name(), hintFilesExtension)
	b, err := ioutil.ReadFile(hintFiles)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		panic(err)
	}

	// hint files does not exist, will load key from entire db file one-by-one
	if errors.Is(err, os.ErrNotExist) {
		header := make([]byte, defaultHeaderLength)
		var currOffset int64

		for {
			headerOffset, err := s.file.ReadAt(header, currOffset)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				} else {
					panic(err)
				}
			}

			headerData := decodeHeader(header)

			key := make([]byte, headerData.keySize)
			keyOffset, err := s.file.ReadAt(key, currOffset+int64(headerOffset))
			if err != nil {
				panic(err)
			}

			value := make([]byte, headerData.valueSize)
			_, err = s.file.ReadAt(value, currOffset+int64(headerOffset+keyOffset))
			if err != nil {
				panic(err)
			}

			totalSize := defaultHeaderLength + headerData.keySize + headerData.valueSize
			s.keyDir[string(key)] = &keyDirEntry{
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

		err = d.Decode(&s.keyDir)
		if err != nil {
			panic(err)
		}
	}
}

func (s DiskStorage) Close() error {
	return s.flush()
}

func (s *DiskStorage) flush() error {
	// flush key dir to hint files
	filename := fmt.Sprintf("%s.%s", s.activeFile, hintFilesExtension)
	f := openDataFile(filename)

	b := new(bytes.Buffer)
	e := gob.NewEncoder(b)
	err := e.Encode(s.keyDir)
	if err != nil {
		panic(err)
	}
	_, _, err = f.Write(b.Bytes())
	return err
}
