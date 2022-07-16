package caskdb

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	hintFilesExtension = "hint"
)

var errRecordNotFound = errors.New("record not found")

type keyDirEntry struct {
	//FileID indicate which files is this entry stored, because there
	//could be multiple files
	FileID int
	//Timestamp in unixnano
	Timestamp int64
	//LocationOffset is the files offset of current entry from initial position (0)
	LocationOffset int64
	// DataLength is the length of bytes of current entry only,
	// how to use this to get current entry:
	// data = currentOffset + DataLength
	DataLength int64
}

type DiskStorage struct {
	*sync.RWMutex

	dbFileFullPath string
	// map the key with the offset position of the value
	keyDir map[string]*keyDirEntry

	files []*datafile
}

func (s *DiskStorage) currentFiles() *datafile {
	return s.files[len(s.files)-1]
}

func NewDiskStorage(filename string) *DiskStorage {
	files := make([]*datafile, 1)
	files[0] = openDataFile(fmt.Sprintf("%s_%d", filename, 0))

	ds := &DiskStorage{
		RWMutex:        &sync.RWMutex{},
		files:          files,
		keyDir:         make(map[string]*keyDirEntry),
		dbFileFullPath: filename,
	}
	ds.initKeyDir()
	return ds
}

func (s *DiskStorage) Set(key, value []byte) error {
	data := newEntry(time.Now().UnixNano(), key, value)
	dataSize, databyte := data.encode()

	files := s.currentFiles()
	fmt.Println(files.Size())
	if files.Size() >= 1*1024*1024 { // 1 MB
		files = openDataFile(fmt.Sprintf("%s_%d", s.dbFileFullPath, len(s.files)))
		s.files = append(s.files, files)
	}
	_, offset, err := files.Write(databyte)
	if err != nil {
		return err
	}

	s.Lock()
	defer s.Unlock()
	s.keyDir[string(key)] = &keyDirEntry{
		// offset represent current offset after this data is written
		// thus, the location of current data should be subtracted by the
		// size of current data
		FileID:         len(s.files) - 1,
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
	_, err := s.files[keyData.FileID].ReadAt(data, keyData.LocationOffset)
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
	hintFiles := fmt.Sprintf("%s.%s", s.dbFileFullPath, hintFilesExtension)
	b, err := ioutil.ReadFile(hintFiles)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		panic(err)
	}

	// hint files does not exist, will load key from entire db files one-by-one
	if !errors.Is(err, os.ErrNotExist) {
		// hint files exist, will load key that
		buff := bytes.NewBuffer(b)
		d := gob.NewDecoder(buff)

		err = d.Decode(&s.keyDir)
		if err != nil {
			panic(err)
		}

	} else if errors.Is(err, os.ErrNotExist) {
		parentPath := strings.Split(s.dbFileFullPath, "/")[0]
		dirs, err := os.ReadDir(parentPath)
		if err != nil {
			panic(err)
		}
		filePath := strings.Split(s.dbFileFullPath, "/")
		dbFileList := make([]string, 0)

		for _, dir := range dirs {
			currFilePath := fmt.Sprintf("%s/%s", parentPath, dir.Name())
			if !strings.Contains(currFilePath, filePath[len(filePath)-1]) {
				continue
			}
			dbFileList = append(dbFileList, currFilePath)
		}
		sort.Slice(dbFileList, func(i, j int) bool { return dbFileList[i] > dbFileList[j] })

		for idx, dbFile := range dbFileList {
			files := openDataFile(dbFile)
			header := make([]byte, defaultHeaderLength)
			var currOffset int64

			for {
				headerOffset, err := files.ReadAt(header, currOffset)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					} else {
						panic(err)
					}
				}

				headerData := decodeHeader(header)

				key := make([]byte, headerData.keySize)
				keyOffset, err := files.ReadAt(key, currOffset+int64(headerOffset))
				if err != nil {
					panic(err)
				}

				value := make([]byte, headerData.valueSize)
				_, err = files.ReadAt(value, currOffset+int64(headerOffset+keyOffset))
				if err != nil {
					panic(err)
				}

				totalSize := defaultHeaderLength + headerData.keySize + headerData.valueSize

				if _, exists := s.keyDir[string(key)]; !exists {
					s.keyDir[string(key)] = &keyDirEntry{
						FileID:         idx,
						Timestamp:      headerData.timestamp,
						LocationOffset: currOffset,
						DataLength:     int64(totalSize),
					}
					s.files[idx] = files
				}

				currOffset += int64(totalSize)
			}

		}

	}
}

func (s DiskStorage) Close() error {
	return s.flush()
}

func (s *DiskStorage) flush() error {
	// flush key dir to hint files
	filename := fmt.Sprintf("%s.%s", s.dbFileFullPath, hintFilesExtension)
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
