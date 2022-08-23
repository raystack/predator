package mock

import (
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/odpf/predator/protocol/metric"
	"github.com/stretchr/testify/mock"
)

type mockMetricStore struct {
	mock.Mock
}

//NewMetricStore create metric store
func NewMetricStore() *mockMetricStore {
	return &mockMetricStore{}
}

func (m *mockMetricStore) Store(profile *job.Profile, metrics []*metric.Metric) error {
	args := m.Called(profile, metrics)
	return args.Error(0)
}

func (m *mockMetricStore) GetMetricsByProfileID(ID string) ([]*metric.Metric, error) {
	args := m.Called(ID)
	return args.Get(0).([]*metric.Metric), args.Error(1)
}

type mockMetricGenerator struct {
	mock.Mock
	protocol.MetricGenerator
}

func NewMetricGenerator() *mockMetricGenerator {
	return &mockMetricGenerator{}
}

func (m *mockMetricGenerator) Generate(entry protocol.Entry, config *job.Profile) ([]*metric.Metric, error) {
	args := m.Called(entry, config)
	return args.Get(0).([]*metric.Metric), args.Error(1)
}

type mockProfiler struct {
	mock.Mock
}

//NewProfiler create mockProfiler
func NewProfiler() *mockProfiler {
	return &mockProfiler{}
}

func (m *mockProfiler) Profile(entry protocol.Entry, profile *job.Profile, metricSpecs []*metric.Spec) ([]*metric.Metric, error) {
	arguments := m.Called(entry, profile, metricSpecs)
	return arguments.Get(0).([]*metric.Metric), arguments.Error(1)
}
