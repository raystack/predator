package mock

import (
	"github.com/odpf/predator/protocol"
	"github.com/stretchr/testify/mock"
)

type mockGitRepository struct {
	mock.Mock
}

func NewMockGitRepository() *mockGitRepository {
	return &mockGitRepository{}
}

func (m *mockGitRepository) Checkout(commit string) (protocol.FileStore, error) {
	args := m.Called(commit)
	return args.Get(0).(protocol.FileStore), args.Error(1)
}

type mockGitRepositoryFactory struct {
	mock.Mock
}

func (m *mockGitRepositoryFactory) Create(url string) protocol.GitRepository {
	args := m.Called(url)
	return args.Get(0).(protocol.GitRepository)
}

func (m *mockGitRepositoryFactory) CreateWithPrefix(url string, pathPrefix string) protocol.GitRepository {
	args := m.Called(url, pathPrefix)
	return args.Get(0).(protocol.GitRepository)
}

func NewMockGitRepositoryFactory() *mockGitRepositoryFactory {
	return &mockGitRepositoryFactory{}
}
