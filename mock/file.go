package mock

import (
	"github.com/odpf/predator/protocol"
	"github.com/stretchr/testify/mock"
)

type mockFileStore struct {
	mock.Mock
}

func (m *mockFileStore) GetPaths() ([]string, error) {
	args := m.Called()
	return args.Get(0).([]string), args.Error(1)
}

func NewMockFileStore() *mockFileStore {
	return &mockFileStore{}
}

func (m *mockFileStore) GetAll() ([]*protocol.File, error) {
	args := m.Called()
	return args.Get(0).([]*protocol.File), args.Error(1)
}

func (m *mockFileStore) Create(file *protocol.File) error {
	args := m.Called(file)
	return args.Error(0)
}

func (m *mockFileStore) Delete(filePath string) error {
	args := m.Called(filePath)
	return args.Error(0)
}

func (m *mockFileStore) Get(filePath string) (*protocol.File, error) {
	args := m.Called(filePath)
	return args.Get(0).(*protocol.File), args.Error(1)
}
