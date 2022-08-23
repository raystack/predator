package mock

import (
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/stretchr/testify/mock"
)

type mockQueryExecutor struct {
	mock.Mock
}

//NewQueryExecutor create mock QueryExecutor
func NewQueryExecutor() *mockQueryExecutor {
	return &mockQueryExecutor{}
}

func (m *mockQueryExecutor) Run(profile *job.Profile, query string, queryType job.QueryType) ([]protocol.Row, error) {
	args := m.Called(profile, query, queryType)
	return args.Get(0).([]protocol.Row), args.Error(1)
}
