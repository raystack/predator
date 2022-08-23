package message

import (
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"
)

type mockProtoBuilder struct {
	mock.Mock
}

func (m *mockProtoBuilder) Build() (proto.Message, error) {
	args := m.Called()
	return args.Get(0).(proto.Message), args.Error(1)
}
