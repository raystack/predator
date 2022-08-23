package mock

import (
	"github.com/odpf/predator/protocol"
	"github.com/stretchr/testify/mock"
)

type mockStatusLogger struct {
	mock.Mock
}

//NewStatusLogger to mock status logger
func NewStatusLogger() *mockStatusLogger {
	return &mockStatusLogger{}
}

//Log to mock log status
func (m *mockStatusLogger) Log(entry protocol.Entry, message string) error {
	args := m.Called(entry, message)
	return args.Error(0)
}
