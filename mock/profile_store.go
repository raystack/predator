package mock

import (
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/stretchr/testify/mock"
)

type mockProfileStore struct {
	mock.Mock
}

//NewProfileStore create mock of profile store
func NewProfileStore() *mockProfileStore {
	return &mockProfileStore{}
}

func (m *mockProfileStore) Create(profile *job.Profile) (*job.Profile, error) {
	args := m.Called(&job.Profile{
		ID:           profile.ID,
		Status:       profile.Status,
		Detail:       profile.Detail,
		Message:      profile.Message,
		GroupName:    profile.GroupName,
		Filter:       profile.Filter,
		URN:          profile.URN,
		TotalRecords: profile.TotalRecords,
	})
	return args.Get(0).(*job.Profile), args.Error(1)
}

func (m *mockProfileStore) Update(profile *job.Profile) error {
	args := m.Called(&job.Profile{
		ID:           profile.ID,
		Status:       profile.Status,
		Detail:       profile.Detail,
		Message:      profile.Message,
		GroupName:    profile.GroupName,
		Filter:       profile.Filter,
		URN:          profile.URN,
		TotalRecords: profile.TotalRecords,
	})
	return args.Error(0)
}

func (m *mockProfileStore) Get(ID string) (*job.Profile, error) {
	args := m.Called(ID)
	return args.Get(0).(*job.Profile), args.Error(1)
}

type stubProfileStore struct {
}

func (s *stubProfileStore) Create(profile *job.Profile) (*job.Profile, error) {
	return &job.Profile{}, nil
}

func (s *stubProfileStore) Update(profile *job.Profile) error {
	return nil
}

func (s *stubProfileStore) Get(ID string) (*job.Profile, error) {
	return &job.Profile{}, nil
}

func NewProfileStoreStub() protocol.ProfileStore {
	return &stubProfileStore{}
}
