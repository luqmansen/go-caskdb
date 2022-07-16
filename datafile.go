package caskdb

import (
	"io"
	"os"
	"sync"
)

type datafile struct {
	fileID string
	file   *os.File
	offset int64
	sync.Mutex
}

// openDataFile will open data files if exists, else
// it will create new one.
func openDataFile(name string) *datafile {
	rw, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0600) // write only
	if err != nil {
		panic(err)
	}

	return &datafile{
		fileID: name,
		file:   rw,
		Mutex:  sync.Mutex{},
		offset: io.SeekStart,
	}
}

func (d *datafile) Size() int64 {
	stat, _ := d.file.Stat()
	return stat.Size()
}

func (d *datafile) Write(p []byte) (n int, offset int64, err error) {
	d.Lock()
	defer d.Unlock()

	d.Size()

	n, err = d.file.Write(p)
	d.offset += int64(n)

	return n, d.offset, err
}

// implement io.ReadAt
func (d *datafile) ReadAt(p []byte, off int64) (int, error) {
	return d.file.ReadAt(p, off)
}
