package status

import (
	"errors"
	"testing"

	"github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol"
	"github.com/stretchr/testify/assert"
)

func TestMultiLogger(t *testing.T) {
	t.Run("MultiLogger", func(t *testing.T) {
		t.Run("Log", func(t *testing.T) {
			t.Run("should call Log on all logger", func(t *testing.T) {
				entry := protocol.NewEntry()
				message := "message"

				logger1 := mock.NewStatusLogger()
				defer logger1.AssertExpectations(t)
				logger1.On("Log", entry, message).Return(nil)

				logger2 := mock.NewStatusLogger()
				defer logger2.AssertExpectations(t)
				logger2.On("Log", entry, message).Return(nil)

				allLogger := []protocol.StatusLogger{logger1, logger2}

				multiLogger := NewMultiLogger(allLogger)
				multiLogger.Log(entry, message)
			})
			t.Run("should call return error if a logger failed", func(t *testing.T) {
				entry := protocol.NewEntry()
				message := "message"
				err := errors.New("an error")

				logger1 := mock.NewStatusLogger()
				defer logger1.AssertExpectations(t)
				logger1.On("Log", entry, message).Return(err)

				logger2 := mock.NewStatusLogger()
				defer logger2.AssertExpectations(t)

				allLogger := []protocol.StatusLogger{logger1, logger2}

				multiLogger := NewMultiLogger(allLogger)
				errResult := multiLogger.Log(entry, message)
				assert.Equal(t, err, errResult)
			})
		})
	})
}
