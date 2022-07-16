package caskdb

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/stretchr/testify/assert"
)

func initStorageHelper(name ...string) (*DiskStorage, string, func()) {
	filename := ""
	if len(name) > 0 {
		filename = name[0]
	} else {
		filename = uuid.NewString()
	}
	filename = "testdata/" + filename

	cleanup := func() {
		os.Remove(filename)
		os.Remove(fmt.Sprintf("%s.%s", filename, hintFilesExtension))
	}
	return NewDiskStorage(filename), filename, cleanup
}

func Test_initKeyDir_useHintFiles(t *testing.T) {
	t.Parallel()

	store, filename, cleanupFunc := initStorageHelper()
	defer cleanupFunc()

	kv := make(map[string][]byte)
	for i := 0; i <= 10; i++ {
		kv[strconv.Itoa(i)] = []byte(strconv.Itoa(i))
	}
	for k, v := range kv {
		assert.Nil(t, store.Set([]byte(k), v))
	}
	assert.Nil(t, store.Close())

	store = NewDiskStorage(filename)
	for k, v := range kv {
		res, err := store.Get([]byte(k))
		assert.Nil(t, err)
		assert.Equal(t, v, res)
	}
}

func Test_initKeyDir(t *testing.T) {
	t.Parallel()

	store, filename, cleanupFunc := initStorageHelper()
	defer cleanupFunc()

	kv := make(map[string][]byte)
	for i := 0; i <= 10; i++ {
		kv[strconv.Itoa(i)] = []byte(strconv.Itoa(i))
	}
	for k, v := range kv {
		assert.Nil(t, store.Set([]byte(k), v))
	}

	store = NewDiskStorage(filename)
	for k, v := range kv {
		res, err := store.Get([]byte(k))
		assert.Nil(t, err)
		assert.Equal(t, v, res)
	}
}

func TestDiskStorage_singleKey(t *testing.T) {
	t.Parallel()

	store, _, cleanupFunc := initStorageHelper()
	defer cleanupFunc()

	assert.Nil(t, store.Set([]byte("yeet"), []byte("donjon")))
	res, err := store.Get([]byte("yeet"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("donjon"), res)
}

func TestDiskStorage_multiKey(t *testing.T) {
	t.Parallel()

	store, _, _ := initStorageHelper()
	//defer cleanupFunc()

	kv := make(map[string][]byte)
	for i := 0; i <= 10_000_000; i++ {
		kv[strconv.Itoa(i)] = []byte(strconv.Itoa(i))
	}
	for k, v := range kv {
		assert.Nil(t, store.Set([]byte(k), v))
	}
	for k, v := range kv {
		res, err := store.Get([]byte(k))
		assert.Nil(t, err)
		assert.Equal(t, v, res)
	}
}

func TestDiskStorage_concurrent(t *testing.T) {
	t.Parallel()

	store, _, cleanupFunc := initStorageHelper()
	defer cleanupFunc()

	kv := make(map[string][]byte)

	for i := 0; i <= 1000; i++ {
		kv[strconv.Itoa(i)] = []byte(strconv.Itoa(i))
	}
	var wgAdd sync.WaitGroup
	for k, v := range kv {
		wgAdd.Add(1)
		go func(k, v []byte) {
			defer wgAdd.Done()
			assert.NoError(t, store.Set(k, v))
			res, err := store.Get(k)
			assert.Nil(t, err)
			assert.Equal(t, v, res)
		}([]byte(k), v)
	}
	wgAdd.Wait()

	var wgGet sync.WaitGroup
	for k, v := range kv {
		wgGet.Add(1)
		go func(k string, v []byte) {
			defer wgGet.Done()
			res, err := store.Get([]byte(k))
			assert.Nil(t, err)
			assert.Equal(t, v, res)
		}(k, v)
	}
	wgGet.Wait()
}

func BenchmarkDiskStorage_Set(b *testing.B) {
	store, _, cleanupFunc := initStorageHelper()
	defer cleanupFunc()

	for i := 0; i < b.N; i++ {
		store.Set([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
	}
}

func BenchmarkDiskStorage_Get(b *testing.B) {
	store, _, cleanupFunc := initStorageHelper()
	defer cleanupFunc()

	for i := 0; i < 100_000; i++ {
		store.Set([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		store.Get([]byte(strconv.Itoa(i)))
	}
}

// load all key dir value from reading the database files
func BenchmarkNewDiskStorage_from_scratch(b *testing.B) {
	store, filename, cleanupFunc := initStorageHelper()
	defer cleanupFunc()

	for i := 0; i < 100_000; i++ {
		store.Set([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		NewDiskStorage(filename)
	}
}

// load all key dir value from hint files
func BenchmarkNewDiskStorage_from_hintFiles(b *testing.B) {
	store, filename, cleanupFunc := initStorageHelper()
	defer cleanupFunc()

	for i := 0; i < 100_000; i++ {
		store.Set([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
	}
	store.Close()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		NewDiskStorage(filename)
	}
}

func TestName(t *testing.T) {
	size := int64(1 * 1024 * 1024)
	fd, err := os.Create("output")
	if err != nil {
		log.Fatal("Failed to create output")
	}
	_, err = fd.Seek(size-1, 0)
	if err != nil {
		log.Fatal("Failed to seek")
	}
	_, err = fd.Write([]byte{0})
	if err != nil {
		log.Fatal("Write failed")
	}

	st, err := fd.Stat()
	if err != nil {
		panic(err)
	}
	t.Log(st.Size())
	t.Log(st.Size() >= 1*1024*1024)

	err = fd.Close()
	if err != nil {
		log.Fatal("Failed to close file")
	}

}
