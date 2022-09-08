package auditor

import (
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/metric"
	"github.com/stretchr/testify/mock"
)

type mockRuleValidator struct {
	mock.Mock
}

//NewMockRuleValidator create mock RuleValidator
func NewMockRuleValidator() *mockRuleValidator {
	return &mockRuleValidator{}
}

func (m *mockRuleValidator) Validate(metrics []*metric.Metric, tolerances []*protocol.Tolerance) ([]*protocol.ValidatedMetric, error) {
	args := m.Called(metrics, tolerances)
	return args.Get(0).([]*protocol.ValidatedMetric), args.Error(1)
}
