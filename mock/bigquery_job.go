package mock

import (
	"github.com/odpf/predator/protocol"
	"github.com/stretchr/testify/mock"
)

type mockBigqueryJobStore struct {
	mock.Mock
}

func (m *mockBigqueryJobStore) Store(bigqueryJob *protocol.BigqueryJob) error {
	args := m.Called(bigqueryJob)
	return args.Error(0)
}

func NewBigqueryJobStore() *mockBigqueryJobStore {
	return &mockBigqueryJobStore{}
}
