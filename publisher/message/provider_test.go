package message

import (
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/publisher/proto/odpf/predator/v1beta1"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestProvider(t *testing.T) {
	t.Run("Get", func(t *testing.T) {
		t.Run("should return proto.Message", func(t *testing.T) {
			value := &predator.ResultLogMessage{}
			key := &predator.ResultLogKey{}

			expectedMessage := &protocol.Message{Key: key, Value: value}

			valueBuilder := &mockProtoBuilder{}
			defer valueBuilder.AssertExpectations(t)
			valueBuilder.On("Build").Return(value, nil)

			keyBuilder := &mockProtoBuilder{}
			defer keyBuilder.AssertExpectations(t)
			keyBuilder.On("Build").Return(key, nil)

			provider := &Provider{
				KeyBuilder:   keyBuilder,
				ValueBuilder: valueBuilder,
			}

			message, err := provider.Get()

			assert.Nil(t, err)
			assert.Equal(t, expectedMessage, message)
		})
	})
}
