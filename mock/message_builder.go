package mock

import (
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/odpf/predator/protocol/metric"
	"github.com/stretchr/testify/mock"
)

type mockMessageProviderFactory struct {
	mock.Mock
}

func NewMessageProviderFactory() *mockMessageProviderFactory {
	return &mockMessageProviderFactory{}
}

func (m *mockMessageProviderFactory) CreateProfileMessage(profile *job.Profile, metrics []*metric.Metric) []protocol.MessageProvider {
	args := m.Called(profile, metrics)
	return args.Get(0).([]protocol.MessageProvider)
}

func (m *mockMessageProviderFactory) CreateAuditMessage(audit *job.Audit, auditResult []*protocol.AuditReport) []protocol.MessageProvider {
	args := m.Called(audit, auditResult)
	return args.Get(0).([]protocol.MessageProvider)
}

type mockMessageBuilder struct {
	mock.Mock
}

func NewMessageBuilder() *mockMessageBuilder {
	return &mockMessageBuilder{}
}

func (m *mockMessageBuilder) Get() (*protocol.Message, error) {
	args := m.Called()
	return args.Get(0).(*protocol.Message), args.Error(1)
}
