package go_caskdb

import (
	"fmt"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"strconv"
	"sync"
	"testing"
)

func init() {
	log.SetLevel(log.ErrorLevel)
}

func initStorage() (*DiskStorage, string, func()) {
	filename := uuid.NewString()
	cleanup := func() {
		os.Remove(filename)
		os.Remove(fmt.Sprintf("%s_%s", filename, hintFilesPrefix))
	}
	return NewDiskStorage(filename), filename, cleanup
}

func Test_initKeyDir_useHintFiles(t *testing.T) {
	t.Parallel()

	store, filename, cleanupFunc := initStorage()
	defer cleanupFunc()

	kv := make(map[string]string)
	for i := 0; i <= 10; i++ {
		kv[strconv.Itoa(i)] = strconv.Itoa(i)
	}
	for k, v := range kv {
		assert.Nil(t, store.Set(k, v))
	}
	assert.Nil(t, store.Close())

	store = NewDiskStorage(filename)
	for k, v := range kv {
		res, err := store.Get(k)
		assert.Nil(t, err)
		assert.Equal(t, v, res)
	}
}

func Test_initKeyDir(t *testing.T) {
	t.Parallel()

	store, filename, cleanupFunc := initStorage()
	defer cleanupFunc()

	kv := make(map[string]string)
	for i := 0; i <= 10; i++ {
		kv[strconv.Itoa(i)] = strconv.Itoa(i)
	}
	for k, v := range kv {
		assert.Nil(t, store.Set(k, v))
	}

	store = NewDiskStorage(filename)
	for k, v := range kv {
		res, err := store.Get(k)
		assert.Nil(t, err)
		assert.Equal(t, v, res)
	}
}

func TestDiskStorage_singleKey(t *testing.T) {
	t.Parallel()

	store, _, cleanupFunc := initStorage()
	defer cleanupFunc()

	assert.Nil(t, store.Set("yeet", "donjon"))
	res, err := store.Get("yeet")
	assert.Nil(t, err)
	assert.Equal(t, "donjon", res)
}

func TestDiskStorage_multiKey(t *testing.T) {
	t.Parallel()

	store, _, cleanupFunc := initStorage()
	defer cleanupFunc()

	kv := make(map[string]string)
	for i := 0; i <= 1000; i++ {
		kv[strconv.Itoa(i)] = strconv.Itoa(i)
	}
	for k, v := range kv {
		assert.Nil(t, store.Set(k, v))
	}
	for k, v := range kv {
		res, err := store.Get(k)
		assert.Nil(t, err)
		assert.Equal(t, v, res)
	}
}

func TestDiskStorage_concurrent(t *testing.T) {
	t.Parallel()

	store, _, cleanupFunc := initStorage()
	defer cleanupFunc()

	kv := make(map[string]string)

	for i := 0; i <= 5_000; i++ {
		kv[strconv.Itoa(i)] = strconv.Itoa(i)
	}
	var wgAdd sync.WaitGroup
	for k, v := range kv {
		wgAdd.Add(1)
		go func(k, v string) {
			defer wgAdd.Done()

			assert.Nil(t, store.Set(k, v))
			res, err := store.Get(k)
			assert.Nil(t, err)
			assert.Equal(t, v, res)
		}(k, v)
	}
	wgAdd.Wait()

	var wgGet sync.WaitGroup
	for k, v := range kv {
		wgGet.Add(1)
		go func(k, v string) {
			defer wgGet.Done()
			res, err := store.Get(k)
			assert.Nil(t, err)
			assert.Equal(t, v, res)
		}(k, v)
	}
	wgGet.Wait()
}

func BenchmarkDiskStorage_Set(b *testing.B) {
	store, _, cleanupFunc := initStorage()
	defer cleanupFunc()

	for i := 0; i < b.N; i++ {
		store.Set(strconv.Itoa(i), strconv.Itoa(i))
	}
}

func BenchmarkDiskStorage_Get(b *testing.B) {
	store, _, cleanupFunc := initStorage()
	defer cleanupFunc()

	for i := 0; i < 100_000; i++ {
		store.Set(strconv.Itoa(i), strconv.Itoa(i))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		store.Get(strconv.Itoa(i))
	}
}

// load all key dir value from reading the database file
func BenchmarkNewDiskStorage_from_scratch(b *testing.B) {
	store, filename, cleanupFunc := initStorage()
	defer cleanupFunc()

	for i := 0; i < 100_000; i++ {
		store.Set(strconv.Itoa(i), strconv.Itoa(i))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		NewDiskStorage(filename)
	}
}

// load all key dir value from hint files
func BenchmarkNewDiskStorage_from_hintFiles(b *testing.B) {
	store, filename, cleanupFunc := initStorage()
	defer cleanupFunc()

	for i := 0; i < 100_000; i++ {
		store.Set(strconv.Itoa(i), strconv.Itoa(i))
	}
	store.Close()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		NewDiskStorage(filename)
	}
}
