package mock

import (
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/stretchr/testify/mock"
)

type mockStatusStore struct {
	mock.Mock
}

//NewStatusStore to mock status store
func NewStatusStore() *mockStatusStore {
	return &mockStatusStore{}
}

//Store to mock storing status
func (m *mockStatusStore) Store(status *protocol.Status) error {
	args := m.Called(&protocol.Status{
		JobID:   status.JobID,
		JobType: status.JobType,
		Status:  status.Status,
		Message: status.Message,
	})
	return args.Error(0)
}

func (m *mockStatusStore) GetLatestStatusByIDandType(jobID string, jobType job.Type) (*protocol.Status, error) {
	args := m.Called(jobID, jobType)
	return args.Get(0).(*protocol.Status), args.Error(1)
}

func (m *mockStatusStore) GetStatusLogByIDandType(jobID string, jobType job.Type) ([]*protocol.Status, error) {
	args := m.Called(jobID, jobType)
	return args.Get(0).([]*protocol.Status), args.Error(1)
}
