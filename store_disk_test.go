package go_caskdb

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
	"testing"
)

func TestDiskStorage_singleKey(t *testing.T) {
	filename := uuid.NewString()
	defer os.Remove(filename)

	store := NewDiskStorage(filename)
	assert.Nil(t, store.Set("yeet", "donjon"))
	res, err := store.Get("yeet")
	assert.Nil(t, err)
	assert.Equal(t, "donjon", res)
}

func TestDiskStorage_multikey(t *testing.T) {
	filename := uuid.NewString()
	defer os.Remove(filename)
	store := NewDiskStorage(filename)

	kv := map[string]string{
		"sponge":  "bob",
		"patrick": "star",
		"uzumaki": "naruto",
	}

	for k, v := range kv {
		assert.Nil(t, store.Set(k, v))
		res, err := store.Get(k)
		assert.Nil(t, err)
		assert.Equal(t, v, res)
	}
}

func TestDiskStorage_concurrent(t *testing.T) {
	filename := uuid.NewString()
	defer os.Remove(filename)
	store := NewDiskStorage(filename)

	kv := make(map[string]string)

	for i := 0; i <= 100; i++ {
		kv[uuid.NewString()] = uuid.NewString()
	}
	var wg sync.WaitGroup
	for k, v := range kv {
		wg.Add(1)
		go func(k, v string) {
			defer wg.Done()

			assert.Nil(t, store.Set(k, v))
			res, err := store.Get(k)
			assert.Nil(t, err)
			assert.Equal(t, v, res)
		}(k, v)
	}
	wg.Wait()
}
