package mock

import "github.com/stretchr/testify/mock"

type mockSQLExpressionFactory struct {
	mock.Mock
}

func NewSQLExpressionFactory() *mockSQLExpressionFactory {
	return &mockSQLExpressionFactory{}
}

func (m *mockSQLExpressionFactory) CreatePartitionExpression(urn string) (string, error) {
	args := m.Called(urn)
	return args.String(0), args.Error(1)
}
