package profile

import (
	"context"
	"errors"
	"fmt"
	"github.com/odpf/predator/stats"
	"sync"
	"time"

	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
)

//Service is profile service
type Service struct {
	wg                    sync.WaitGroup
	profileStore          protocol.ProfileStore
	metricGenerator       protocol.MetricGenerator
	publisher             protocol.Publisher
	messageBuilderFactory protocol.MessageProviderFactory
	statusStore           protocol.StatusStore
	statsClientBuilder    stats.ClientBuilder
}

//Get to get profile
func (s *Service) Get(ID string) (*job.Profile, error) {
	return s.profileStore.Get(ID)
}

//NewService to construct profile service
func NewService(profileStore protocol.ProfileStore,
	metricGenerator protocol.MetricGenerator,
	publisher protocol.Publisher,
	messageBuilderFactory protocol.MessageProviderFactory,
	statusStore protocol.StatusStore,
	statsFactory stats.ClientBuilder) *Service {
	return &Service{
		profileStore:          profileStore,
		metricGenerator:       metricGenerator,
		publisher:             publisher,
		messageBuilderFactory: messageBuilderFactory,
		statusStore:           statusStore,
		statsClientBuilder:    statsFactory,
	}
}

//CreateProfile to create profile
func (s *Service) CreateProfile(profile *job.Profile) (*job.Profile, error) {
	createdProfile, err := s.profileStore.Create(profile)
	if err != nil {
		return nil, err
	}

	label, err := protocol.ParseLabel(profile.URN)
	if err != nil {
		return nil, err
	}

	clientBuilder := s.statsClientBuilder.WithURN(label)
	statsClient, err := clientBuilder.Build()
	if err != nil {
		return nil, err
	}

	m := stats.Metric("profile.job.created.count")
	statsClient.Increment(m)

	s.wg.Add(1)
	go func() {
		var err error
		defer func() {
			if err != nil {
				createdProfile.Status = job.StateFailed
				createdProfile.Message = fmt.Sprintf("profile failed because %s", err.Error())
				err = s.profileStore.Update(createdProfile)

				m := stats.Metric("profile.job.failed.count")
				statsClient.Increment(m)
			} else {
				createdProfile.Status = job.StateCompleted
				createdProfile.Message = "profile completed"
				err = s.profileStore.Update(createdProfile)

				m := stats.Metric("profile.job.completed.count")
				statsClient.Increment(m)
			}
			s.wg.Done()
		}()

		createdProfile.Status = job.StateInProgress
		createdProfile.Message = "profile in progress"
		err = s.profileStore.Update(createdProfile)
		if err != nil {
			return
		}

		m := stats.Metric("profile.job.inprogress.count")
		statsClient.Increment(m)

		metrics, err := s.metricGenerator.Generate(protocol.NewEntry(), createdProfile)
		if err != nil {
			return
		}

		messageProviders := s.messageBuilderFactory.CreateProfileMessage(createdProfile, metrics)
		for _, messageProvider := range messageProviders {
			err = s.publisher.Publish(messageProvider)
			if err != nil {
				return
			}
		}

		jobDurationStat := stats.Metric("profile.job.time")
		start := createdProfile.EventTimestamp
		end := time.Now().In(time.UTC)
		statsClient.DurationOf(jobDurationStat, start, end)
	}()

	return createdProfile, err
}

//WaitAll to wait until task finished
func (s *Service) WaitAll(ctx context.Context) error {
	waitChan := make(chan bool)
	go func() {
		s.wg.Wait()
		close(waitChan)
	}()

	select {
	case <-waitChan:
		fmt.Println("all task finished")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

//GetLog to get profile log
func (s *Service) GetLog(profileID string) ([]*protocol.Status, error) {
	status, err := s.statusStore.GetStatusLogByIDandType(profileID, job.TypeProfile)
	if err != nil {
		return nil, err
	}
	if status == nil {
		msg := fmt.Sprintf("Logs for %s not found", profileID)
		return nil, errors.New(msg)
	}
	return status, nil
}
