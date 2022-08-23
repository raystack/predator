package mock

import (
	"github.com/odpf/predator/protocol/meta"
	"github.com/stretchr/testify/mock"
)

type MetadataStore struct {
	mock.Mock
}

func NewMetadataStore() *MetadataStore {
	return &MetadataStore{}
}

func (spy *MetadataStore) GetMetadata(tableId string) (*meta.TableSpec, error) {
	args := spy.Called(tableId)
	return args.Get(0).(*meta.TableSpec), args.Error(1)
}

func (spy *MetadataStore) GetUniqueConstraints(tableId string) ([]string, error) {
	args := spy.Called(tableId)
	return args.Get(0).([]string), args.Error(1)
}
