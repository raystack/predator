package protocol

import (
	"context"
	"github.com/odpf/predator/protocol/job"
	"github.com/odpf/predator/protocol/metric"
	"time"
)

//ProfileMetric is single metric that produced by publisher
type ProfileMetric struct {
	ID             string
	ProfileID      string
	TableURN       string
	Partition      string
	FieldID        string
	OwnerType      metric.Owner
	Category       metric.Category
	Condition      string
	MetricName     metric.Type
	MetricValue    float64
	EventTimestamp time.Time
}

//ProfileGroup is a type to do group by operation on ProfileMetric
type ProfileGroup []*ProfileMetric

//ByPartitionDate group by partition date
func (pg ProfileGroup) ByPartitionDate() map[string][]*ProfileMetric {
	r := make(map[string][]*ProfileMetric)
	for _, a := range []*ProfileMetric(pg) {
		if r[a.Partition] == nil {
			r[a.Partition] = []*ProfileMetric{}
		}
		r[a.Partition] = append(r[a.Partition], a)
	}
	return r
}

//MetricStore to store profile result
type MetricStore interface {
	Store(profile *job.Profile, metrics []*metric.Metric) error
	GetMetricsByProfileID(ID string) ([]*metric.Metric, error)
}

//MetricsGenerator generate metric
type MetricGenerator interface {
	//Generate metrics
	Generate(entry Entry, config *job.Profile) ([]*metric.Metric, error)
}

//ProfileService is service of profiler
type ProfileService interface {
	//CreateProfile create profile job
	CreateProfile(detail *job.Profile) (*job.Profile, error)
	Get(ID string) (*job.Profile, error)
	WaitAll(ctx context.Context) error
	GetLog(ID string) ([]*Status, error)
}

//MetricProfiler collect metrics, actually do metric calculation to obtain the value of metric
type MetricProfiler interface {
	Profile(entry Entry, profile *job.Profile, metricSpecs []*metric.Spec) ([]*metric.Metric, error)
}

//ProfileStatisticGenerator generate profile statistic
type ProfileStatisticGenerator interface {
	Generate(profile *job.Profile) error
}
