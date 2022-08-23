package uniqueconstraint

import (
	"github.com/odpf/predator/protocol"
)

const uniqueConstraintCacheExpirationSecond = 120

//StoreFactory to create specific UniqueConstraintStore based on url configuration
type StoreFactory struct {
	dictionaryStoreFactory *DictionaryStoreFactory
}

//NewStoreFactory is constructor of UniqueConstraintStoreFactory
func NewStoreFactory(dictionaryStoreFactory *DictionaryStoreFactory) *StoreFactory {
	return &StoreFactory{
		dictionaryStoreFactory: dictionaryStoreFactory,
	}
}

//CreateUniqueConstraintStore to create specific implementation of UniqueConstraintStore
func (u *StoreFactory) CreateUniqueConstraintStore(URL string) (protocol.ConstraintStore, error) {
	actualDictionaryStore, err := u.dictionaryStoreFactory.CreateDictionaryStore(URL)
	if err != nil {
		return nil, err
	}

	cacheDictionaryStore := NewCachedDictionaryStore(uniqueConstraintCacheExpirationSecond, actualDictionaryStore)
	return NewStore(cacheDictionaryStore), nil
}

//DictionaryStoreFactory is factory
type DictionaryStoreFactory struct {
}

//NewDictionaryStoreFactory is constructor
func NewDictionaryStoreFactory() *DictionaryStoreFactory {
	return &DictionaryStoreFactory{}
}

//CreateDictionaryStore to create dictionary store
func (u *DictionaryStoreFactory) CreateDictionaryStore(URL string) (DictionaryStore, error) {
	fileReader := &defaultFileReader{}
	return NewCSVDictionaryStore(URL, fileReader), nil
}
