package publisher

import (
	"context"
	"errors"
	predatormock "github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

type mockSink struct {
	mock.Mock
}

func (m *mockSink) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *mockSink) Sink(message *protocol.Message) error {
	args := m.Called(message)
	return args.Error(0)
}

func TestPublisher(t *testing.T) {
	t.Run("Publish", func(t *testing.T) {
		t.Run("should publish message", func(t *testing.T) {
			message := &protocol.Message{}

			messageBuilder := predatormock.NewMessageBuilder()
			defer messageBuilder.AssertExpectations(t)

			sink := &mockSink{}
			defer sink.AssertExpectations(t)

			messageBuilder.On("Get").Return(message, nil)
			sink.On("Sink", message).Return(nil)

			publisher := &Publisher{kafkaSink: sink}

			err := publisher.Publish(messageBuilder)

			assert.Nil(t, err)
		})
		t.Run("should return error when build message failed", func(t *testing.T) {
			someErr := errors.New("build message failed")
			message := &protocol.Message{}

			messageBuilder := predatormock.NewMessageBuilder()
			defer messageBuilder.AssertExpectations(t)

			sink := &mockSink{}
			defer sink.AssertExpectations(t)

			messageBuilder.On("Get").Return(message, someErr)

			publisher := &Publisher{kafkaSink: sink}

			err := publisher.Publish(messageBuilder)

			assert.Error(t, err)
		})
		t.Run("should return error when sink failed", func(t *testing.T) {
			someErr := errors.New("sinkfailed")
			message := &protocol.Message{}

			messageBuilder := predatormock.NewMessageBuilder()
			defer messageBuilder.AssertExpectations(t)

			sink := &mockSink{}
			defer sink.AssertExpectations(t)

			messageBuilder.On("Get").Return(message, nil)
			sink.On("Sink", message).Return(someErr)

			publisher := &Publisher{kafkaSink: sink}

			err := publisher.Publish(messageBuilder)

			assert.Error(t, err)
		})
	})
}

func TestSinkFactory(t *testing.T) {
	t.Run("Create", func(t *testing.T) {
		t.Run("should return kafka sink", func(t *testing.T) {
			factory := &SinkFactory{}

			config := &protocol.SinkConfig{
				Type:   protocol.Kafka,
				Broker: []string{"abc"},
				Topic:  "def",
			}
			sink := factory.Create(config)
			assert.IsType(t, &KafkaSink{}, sink)
		})
		t.Run("should return console sink", func(t *testing.T) {
			factory := &SinkFactory{}

			config := &protocol.SinkConfig{
				Type: protocol.Console,
			}
			sink := factory.Create(config)
			assert.IsType(t, &ConsoleSink{}, sink)
		})
		t.Run("should return dummy sink", func(t *testing.T) {
			factory := &SinkFactory{}

			config := &protocol.SinkConfig{
				Type: protocol.Dummy,
			}
			sink := factory.Create(config)
			assert.IsType(t, &DummySink{}, sink)
		})
	})
}
