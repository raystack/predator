package mock

import (
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/odpf/predator/stats"
	"github.com/stretchr/testify/mock"
	"time"
)

type dummyStats struct {
	mock.Mock
}

func (m *dummyStats) WithTags(tags ...stats.KV) stats.Client {
	args := m.Called(tags)
	return args.Get(0).(stats.Client)
}

func NewDummyStats() *dummyStats {
	return &dummyStats{}
}

func (m *dummyStats) Increment(metric string) {
}

func (m *dummyStats) DurationUntilNow(metric string, start time.Time) {
}

func (m *dummyStats) IncrementBy(metric string, count int64) {
}

func (m *dummyStats) Gauge(metric string, value float64) {
}

func (m *dummyStats) Histogram(metric string, value float64) {
}

func (m *dummyStats) DurationOf(metric string, start, end time.Time) {
}

func (m *dummyStats) Close() {
}

type mockStatFactory struct {
	mock.Mock
}

func (m *mockStatFactory) Create() (stats.Client, error) {
	args := m.Called()
	return args.Get(0).(stats.Client), args.Error(1)
}

func (m *mockStatFactory) CreateFromEntity(entity *protocol.Entity) (stats.Client, error) {
	args := m.Called(entity)
	return args.Get(0).(stats.Client), args.Error(1)
}

func NewStatFactory() *mockStatFactory {
	return &mockStatFactory{}
}

func (m *mockStatFactory) CreateFromProjectID(projectID string) (stats.Client, error) {
	args := m.Called(projectID)
	return args.Get(0).(stats.Client), args.Error(1)
}

type mockStatBuilder struct {
	mock.Mock
}

func (m *mockStatBuilder) WithDeployment(deployment string) stats.ClientBuilder {
	args := m.Called(deployment)
	return args.Get(0).(stats.ClientBuilder)
}

func NewStatBuilder() *mockStatBuilder {
	return &mockStatBuilder{}
}

func (m *mockStatBuilder) WithEntity(entity *protocol.Entity) stats.ClientBuilder {
	args := m.Called(entity)
	return args.Get(0).(stats.ClientBuilder)
}

func (m *mockStatBuilder) WithURN(urn *protocol.Label) stats.ClientBuilder {
	args := m.Called(urn)
	return args.Get(0).(stats.ClientBuilder)
}

func (m *mockStatBuilder) WithEnvironment(environment string) stats.ClientBuilder {
	args := m.Called(environment)
	return args.Get(0).(stats.ClientBuilder)
}

func (m *mockStatBuilder) WithPodName(podName string) stats.ClientBuilder {
	args := m.Called(podName)
	return args.Get(0).(stats.ClientBuilder)
}

func (m *mockStatBuilder) Build() (stats.Client, error) {
	args := m.Called()
	return args.Get(0).(stats.Client), args.Error(1)
}

type mockObserver struct {
	mock.Mock
}

func NewStatObserver() *mockObserver {
	return &mockObserver{}
}

func (m *mockObserver) Update(state interface{}) {
	var p *job.Profile
	p = state.(*job.Profile)
	m.Called(p)
}
