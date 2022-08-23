package mock

import (
	"github.com/odpf/predator/protocol"
	"github.com/stretchr/testify/mock"
)

type mockToleranceStore struct {
	mock.Mock
}

//NewToleranceStore to mock new tolerance store
func NewToleranceStore() *mockToleranceStore {
	return &mockToleranceStore{}
}

func (m *mockToleranceStore) Create(spec *protocol.ToleranceSpec) error {
	args := m.Called(spec)
	return args.Error(0)
}

func (m *mockToleranceStore) Delete(tableID string) error {
	args := m.Called(tableID)
	return args.Error(0)
}

func (m *mockToleranceStore) GetAll() ([]*protocol.ToleranceSpec, error) {
	args := m.Called()
	return args.Get(0).([]*protocol.ToleranceSpec), args.Error(1)
}

func (m *mockToleranceStore) GetByProjectID(projectID string) ([]*protocol.ToleranceSpec, error) {
	args := m.Called(projectID)
	return args.Get(0).([]*protocol.ToleranceSpec), args.Error(1)
}

func (m *mockToleranceStore) GetByTableID(tableID string) (*protocol.ToleranceSpec, error) {
	args := m.Called(tableID)
	return args.Get(0).(*protocol.ToleranceSpec), args.Error(1)
}

func (m *mockToleranceStore) GetResourceNames() ([]string, error) {
	args := m.Called()
	return args.Get(0).([]string), args.Error(1)
}

type mockToleranceStoreFactory struct {
	mock.Mock
}

func NewMockToleranceStoreFactory() *mockToleranceStoreFactory {
	return &mockToleranceStoreFactory{}
}

func (m *mockToleranceStoreFactory) Create(URL string, multiTenancyEnabled bool) (protocol.ToleranceStore, error) {
	args := m.Called(URL, multiTenancyEnabled)
	return args.Get(0).(protocol.ToleranceStore), args.Error(1)
}

func (m *mockToleranceStoreFactory) CreateWithOptions(store protocol.FileStore, pathType protocol.PathType) (protocol.ToleranceStore, error) {
	args := m.Called(store, pathType)
	return args.Get(0).(protocol.ToleranceStore), args.Error(1)
}

type mockSpecValidator struct {
	mock.Mock
}

func (m *mockSpecValidator) Validate(spec *protocol.ToleranceSpec) error {
	args := m.Called(spec)
	return args.Error(0)
}

func NewSpecValidator() *mockSpecValidator {
	return &mockSpecValidator{}
}
