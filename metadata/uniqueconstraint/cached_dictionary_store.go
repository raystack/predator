package uniqueconstraint

import (
	"github.com/allegro/bigcache"
	"github.com/eko/gocache/cache"
	"github.com/eko/gocache/store"
	"github.com/vmihailenco/msgpack/v5"
	"log"
	"os"
	"time"
)

const cacheKey = "spreadsheet"

var logger = log.New(os.Stdout, "INFO: ", log.Lshortfile|log.LstdFlags)

//CachedDictionaryStore is unique constrain source that cached
type CachedDictionaryStore struct {
	cache cache.CacheInterface
}

//NewCachedDictionaryStore create CachedDictionaryStore
//cacheExpiration is in seconds
func NewCachedDictionaryStore(cacheExpirationSeconds int, source DictionaryStore) *CachedDictionaryStore {
	bigcacheClient, _ := bigcache.NewBigCache(bigcache.DefaultConfig(time.Duration(cacheExpirationSeconds) * time.Second))
	bigcacheStore := store.NewBigcache(bigcacheClient, &store.Options{
		Expiration: time.Duration(cacheExpirationSeconds) * time.Second,
	})

	loadFunc := func(key interface{}) (interface{}, error) {
		dict, err := source.Get()
		if err != nil {
			return nil, err
		}

		logger.Println("load unique constraint dictionary")

		return msgpack.Marshal(&dict)
	}

	cacheManager := cache.New(bigcacheStore)
	loadableCache := cache.NewLoadable(loadFunc, cacheManager)
	return &CachedDictionaryStore{
		cache: loadableCache,
	}
}

//Get to get unique constraints
func (c *CachedDictionaryStore) Get() (map[string][]string, error) {
	cacheValue, err := c.cache.Get(cacheKey)
	if err != nil {
		return nil, err
	}

	var dict map[string][]string

	err = msgpack.Unmarshal(cacheValue.([]byte), &dict)
	if err != nil {
		return nil, err
	}

	return dict, nil
}
