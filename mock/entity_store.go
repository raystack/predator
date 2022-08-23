package mock

import (
	"github.com/odpf/predator/protocol"
	"github.com/stretchr/testify/mock"
)

type EntityStoreMock struct {
	mock.Mock
}

func (e *EntityStoreMock) GetEntityByProjectID(gcpProjectID string) (*protocol.Entity, error) {
	args := e.Called(gcpProjectID)
	return args.Get(0).(*protocol.Entity), args.Error(1)
}

func NewEntityStore() *EntityStoreMock {
	return &EntityStoreMock{}
}

func (e *EntityStoreMock) Save(entity *protocol.Entity) (*protocol.Entity, error) {
	args := e.Called(entity)
	return args.Get(0).(*protocol.Entity), args.Error(1)
}

func (e *EntityStoreMock) GetEntityByGitURL(gitURL string) (*protocol.Entity, error) {
	args := e.Called(gitURL)
	return args.Get(0).(*protocol.Entity), args.Error(1)
}

func (e *EntityStoreMock) Create(entity *protocol.Entity) (*protocol.Entity, error) {
	args := e.Called(entity)
	return args.Get(0).(*protocol.Entity), args.Error(1)
}

func (e *EntityStoreMock) Get(ID string) (*protocol.Entity, error) {
	args := e.Called(ID)
	return args.Get(0).(*protocol.Entity), args.Error(1)
}

func (e *EntityStoreMock) Update(entity *protocol.Entity) (*protocol.Entity, error) {
	args := e.Called(entity)
	return args.Get(0).(*protocol.Entity), args.Error(1)
}

func (e *EntityStoreMock) GetAll() ([]*protocol.Entity, error) {
	args := e.Called()
	return args.Get(0).([]*protocol.Entity), args.Error(1)
}
