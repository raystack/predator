package mock

import (
	"context"
	"time"

	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/stretchr/testify/mock"
)

type mockProfileService struct {
	mock.Mock
}

//NewProfileService create mock ProfileService
func NewProfileService() *mockProfileService {
	return &mockProfileService{}
}

func (m *mockProfileService) Get(ID string) (*job.Profile, error) {
	args := m.Called(ID)
	return args.Get(0).(*job.Profile), args.Error(1)
}

func (m *mockProfileService) CreateProfile(profile *job.Profile) (*job.Profile, error) {
	profile.EventTimestamp = time.Time{}
	arguments := m.Called(profile)
	return arguments.Get(0).(*job.Profile), arguments.Error(1)
}

func (m *mockProfileService) WaitAll(ctx context.Context) error {
	arguments := m.Called(ctx)
	return arguments.Error(0)
}

func (m *mockProfileService) GetLog(ID string) ([]*protocol.Status, error) {
	args := m.Called(ID)
	return args.Get(0).([]*protocol.Status), args.Error(1)
}

type mockProfileStatisticGenerator struct {
	mock.Mock
}

func (m *mockProfileStatisticGenerator) Generate(profile *job.Profile) error {
	args := m.Called(profile)
	return args.Error(0)
}

func NewProfileStatisticGenerator() *mockProfileStatisticGenerator {
	return &mockProfileStatisticGenerator{}
}
