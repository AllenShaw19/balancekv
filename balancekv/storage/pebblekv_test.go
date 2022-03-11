package storage

import (
	"fmt"
	"github.com/cockroachdb/pebble"
	"log"
	"testing"
)

func TestInitPebbleKV(t *testing.T) {
	db, err := pebble.Open("demo", &pebble.Options{})
	if err != nil {
		log.Fatal(err)
	}
	key := []byte("hello")
	if err := db.Set(key, []byte("world"), pebble.Sync); err != nil {
		log.Fatal(err)
	}
	value, closer, err := db.Get(key)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s %s\n", key, value)
	if err := closer.Close(); err != nil {
		log.Fatal(err)
	}
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}
}

func TestMultiWrite(t *testing.T) {
	db, err := pebble.Open("demo", &pebble.Options{})
	if err != nil {
		log.Fatal(err)
	}
	opts := &pebble.WriteOptions{}
	batch := db.NewBatch()
	batch.Set([]byte("account1"), []byte("50"), opts)
	batch.Set([]byte("account2"), []byte("35"), opts)
	err = db.Apply(batch, opts)
	if err != nil {
		log.Fatal(err)
		return
	}

	value, closer, err := db.Get([]byte("account1"))
	defer closer.Close()
	fmt.Printf("%s\n", value)
}
