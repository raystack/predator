package mock

import (
	"github.com/odpf/predator/protocol"
	"github.com/stretchr/testify/mock"
)

type mockUploadFactory struct {
	mock.Mock
}

func NewMockUploadFactory() *mockUploadFactory {
	return &mockUploadFactory{}
}

func (m *mockUploadFactory) Create(gitRepo *protocol.GitInfo) (protocol.Task, error) {
	args := m.Called(gitRepo)

	return args.Get(0).(protocol.Task), args.Error(1)
}

type mockUpload struct {
	mock.Mock
}

func NewMockUpload() *mockUpload {
	return &mockUpload{}
}

func (m *mockUpload) Run() (interface{}, error) {
	args := m.Called()
	return args.Get(0), args.Error(1)
}
