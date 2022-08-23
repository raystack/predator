package profile

import (
	"context"
	"errors"
	"fmt"

	"testing"
	"time"

	"github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/odpf/predator/protocol/metric"
	"github.com/odpf/predator/publisher/message"
	"github.com/stretchr/testify/assert"
)

func TestProfileService(t *testing.T) {
	t.Run("Profile", func(t *testing.T) {
		t.Run("should run profile job", func(t *testing.T) {
			profile := &job.Profile{
				Status:  job.StateCreated,
				Message: "profile started",
				URN:     "a.b.c",
			}

			inProgressProfile := &job.Profile{
				Status:  job.StateInProgress,
				Message: "profile in progress",
				URN:     "a.b.c",
			}

			completedProfile := &job.Profile{
				Status:  job.StateCompleted,
				Message: "profile completed",
				URN:     "a.b.c",
			}

			label := &protocol.Label{
				Project: "a",
				Dataset: "b",
				Table:   "c",
			}

			metrics := []*metric.Metric{
				{
					GroupValue: "2019-01-01",
				},
			}

			messageProviders := []protocol.MessageProvider{
				&message.Provider{},
			}

			profileStore := mock.NewProfileStore()
			defer profileStore.AssertExpectations(t)

			metricGenerator := mock.NewMetricGenerator()
			defer metricGenerator.AssertExpectations(t)

			publisher := mock.NewPublisher()
			defer publisher.AssertExpectations(t)

			metricProviderFactory := mock.NewMessageProviderFactory()
			defer metricProviderFactory.AssertExpectations(t)

			profileStore.On("Create", profile).Return(profile, nil)
			profileStore.On("Update", inProgressProfile).Return(nil)

			metricGenerator.On("Generate", protocol.NewEntry(), inProgressProfile).Return(metrics, nil)
			metricProviderFactory.On("CreateProfileMessage", inProgressProfile, metrics).Return(messageProviders)
			publisher.On("Publish", messageProviders[0]).Return(nil)

			profileStore.On("Update", completedProfile).Return(nil)

			statsClientBuilder := mock.NewStatBuilder()
			defer statsClientBuilder.AssertExpectations(t)

			statsClient := mock.NewDummyStats()
			statsClientBuilder.On("WithURN", label).Return(statsClientBuilder)
			statsClientBuilder.On("Build").Return(statsClient, nil)

			s := NewService(profileStore, metricGenerator, publisher, metricProviderFactory, nil, statsClientBuilder)

			result, _ := s.CreateProfile(profile)

			_ = s.WaitAll(context.Background())

			assert.Equal(t, completedProfile, result)
		})
		t.Run("should failed when generate metrics return error", func(t *testing.T) {
			someError := errors.New("network error")
			profile := &job.Profile{
				Status:  job.StateCreated,
				Message: "profile started",
				URN:     "a.b.c",
			}

			inProgressProfile := &job.Profile{
				Status:  job.StateInProgress,
				Message: "profile in progress",
				URN:     "a.b.c",
			}

			endProfileState := &job.Profile{
				Status:  job.StateFailed,
				Message: fmt.Sprintf("profile failed because %s", someError.Error()),
				URN:     "a.b.c",
			}

			label := &protocol.Label{
				Project: "a",
				Dataset: "b",
				Table:   "c",
			}

			var metrics []*metric.Metric

			profileStore := mock.NewProfileStore()
			defer profileStore.AssertExpectations(t)

			metricGenerator := mock.NewMetricGenerator()
			defer metricGenerator.AssertExpectations(t)

			publisher := mock.NewPublisher()
			defer publisher.AssertExpectations(t)

			profileStore.On("Create", profile).Return(profile, nil)
			profileStore.On("Update", inProgressProfile).Return(nil)

			metricGenerator.On("Generate", protocol.NewEntry(), inProgressProfile).Return(metrics, someError)

			profileStore.On("Update", endProfileState).Return(nil)

			statsClientBuilder := mock.NewStatBuilder()
			defer statsClientBuilder.AssertExpectations(t)

			statsClient := mock.NewDummyStats()
			statsClientBuilder.On("WithURN", label).Return(statsClientBuilder)
			statsClientBuilder.On("Build").Return(statsClient, nil)

			s := NewService(profileStore, metricGenerator, publisher, nil, nil, statsClientBuilder)

			result, _ := s.CreateProfile(profile)

			_ = s.WaitAll(context.Background())

			assert.Equal(t, endProfileState, result)
		})
		t.Run("should failed when publish metrics return error", func(t *testing.T) {
			someError := errors.New("network error")
			profile := &job.Profile{
				Status:  job.StateCreated,
				Message: "profile started",
				URN:     "a.b.c",
			}

			inProgressProfile := &job.Profile{
				Status:  job.StateInProgress,
				Message: "profile in progress",
				URN:     "a.b.c",
			}

			endProfileState := &job.Profile{
				Status:  job.StateFailed,
				Message: fmt.Sprintf("profile failed because %s", someError.Error()),
				URN:     "a.b.c",
			}

			metrics := []*metric.Metric{
				{
					GroupValue: "2019-01-01",
				},
			}

			label := &protocol.Label{
				Project: "a",
				Dataset: "b",
				Table:   "c",
			}

			messageProviders := []protocol.MessageProvider{
				&message.Provider{},
			}

			messageProviderFactory := mock.NewMessageProviderFactory()
			defer messageProviderFactory.AssertExpectations(t)

			profileStore := mock.NewProfileStore()
			defer profileStore.AssertExpectations(t)

			metricGenerator := mock.NewMetricGenerator()
			defer metricGenerator.AssertExpectations(t)

			publisher := mock.NewPublisher()
			defer publisher.AssertExpectations(t)

			profileStore.On("Create", profile).Return(profile, nil)
			profileStore.On("Update", inProgressProfile).Return(nil)
			profileStore.On("Update", endProfileState).Return(nil)

			metricGenerator.On("Generate", protocol.NewEntry(), inProgressProfile).Return(metrics, nil)
			messageProviderFactory.On("CreateProfileMessage", inProgressProfile, metrics).Return(messageProviders)
			publisher.On("Publish", messageProviders[0]).Return(someError)

			statsClientBuilder := mock.NewStatBuilder()
			defer statsClientBuilder.AssertExpectations(t)

			statsClient := mock.NewDummyStats()
			statsClientBuilder.On("WithURN", label).Return(statsClientBuilder)
			statsClientBuilder.On("Build").Return(statsClient, nil)

			s := NewService(profileStore, metricGenerator, publisher, messageProviderFactory, nil, statsClientBuilder)

			result, _ := s.CreateProfile(profile)

			_ = s.WaitAll(context.Background())

			assert.Equal(t, endProfileState, result)
		})
		t.Run("should return error when create profile failed", func(t *testing.T) {
			someError := errors.New("network error")
			profile := &job.Profile{
				Status:  job.StateCreated,
				Message: "profile started",
				URN:     "a.b.c",
			}

			profileStore := mock.NewProfileStore()
			defer profileStore.AssertExpectations(t)

			metricGenerator := mock.NewMetricGenerator()
			defer metricGenerator.AssertExpectations(t)

			publisher := mock.NewPublisher()
			defer publisher.AssertExpectations(t)

			profileStore.On("Create", profile).Return(&job.Profile{}, someError)

			statsClientBuilder := mock.NewStatBuilder()
			defer statsClientBuilder.AssertExpectations(t)

			s := NewService(profileStore, metricGenerator, publisher, nil, nil, statsClientBuilder)

			_, err := s.CreateProfile(profile)

			_ = s.WaitAll(context.Background())

			assert.Error(t, err)
		})
	})
	t.Run("Get", func(t *testing.T) {
		t.Run("should call profile store", func(t *testing.T) {
			ID := "job-1234"
			profile := &job.Profile{
				ID:      ID,
				Status:  job.StateCreated,
				Message: "profile started",
			}

			profileStore := mock.NewProfileStore()
			defer profileStore.AssertExpectations(t)

			metricGenerator := mock.NewMetricGenerator()
			defer metricGenerator.AssertExpectations(t)

			publisher := mock.NewPublisher()
			defer publisher.AssertExpectations(t)

			profileStore.On("Get", ID).Return(profile, nil)

			s := NewService(profileStore, metricGenerator, publisher, nil, nil, nil)

			result, _ := s.Get(ID)

			assert.Equal(t, profile, result)
		})
		t.Run("should return error when get profile failed", func(t *testing.T) {
			someError := errors.New("DB error")
			ID := "job-1234"
			var profile *job.Profile

			profileStore := mock.NewProfileStore()
			defer profileStore.AssertExpectations(t)

			metricGenerator := mock.NewMetricGenerator()
			defer metricGenerator.AssertExpectations(t)

			publisher := mock.NewPublisher()
			defer publisher.AssertExpectations(t)

			profileStore.On("Get", ID).Return(profile, someError)

			s := NewService(profileStore, metricGenerator, publisher, nil, nil, nil)

			result, err := s.Get(ID)

			assert.Nil(t, result)
			assert.Error(t, err)
		})
	})
	t.Run("GetLog", func(t *testing.T) {
		t.Run("should return log of a profile", func(t *testing.T) {
			profileID := "profile-abcd"
			jobType := job.TypeProfile
			currentTime := time.Now().In(time.UTC)
			statusFirst := &protocol.Status{
				JobID:          profileID,
				JobType:        jobType,
				Status:         string(job.StateInProgress),
				Message:        "gathering metadata 1",
				EventTimestamp: currentTime,
			}
			statusSecond := &protocol.Status{
				JobID:          profileID,
				JobType:        jobType,
				Status:         string(job.StateInProgress),
				Message:        "gathering metadata 2",
				EventTimestamp: currentTime.Add(10 * time.Second),
			}
			statusList := []*protocol.Status{
				statusSecond,
				statusFirst,
			}

			statusStore := mock.NewStatusStore()
			statusStore.On("GetStatusLogByIDandType", profileID, jobType).Return(statusList, nil)
			defer statusStore.AssertExpectations(t)

			service := NewService(nil, nil, nil, nil, statusStore, nil)
			result, err := service.GetLog(profileID)

			assert.Nil(t, err)
			assert.Equal(t, statusList, result)
		})
	})
}
