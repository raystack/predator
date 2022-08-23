package mock

import (
	"context"
	"github.com/odpf/predator/protocol"
	"github.com/stretchr/testify/mock"
)

type mockPublisher struct {
	mock.Mock
}

func (p *mockPublisher) Close(ctx context.Context) error {
	args := p.Called(ctx)
	return args.Error(0)
}

func NewPublisher() *mockPublisher {
	return &mockPublisher{}
}

func (p *mockPublisher) Publish(messageProvider protocol.MessageProvider) error {
	args := p.Called(messageProvider)
	return args.Error(0)
}
