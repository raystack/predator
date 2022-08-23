package metric

import (
	"errors"
	"fmt"

	"github.com/odpf/predator/metric/common"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/odpf/predator/protocol/metric"
	"github.com/odpf/predator/protocol/query"
)

const totalRecordsAlias = "total_records"

//DefaultGenerator is default metric generator
type DefaultGenerator struct {
	specGenerator protocol.MetricSpecGenerator
	profiler      protocol.MetricProfiler
	metricStore   protocol.MetricStore
}

func NewDefaultGenerator(specGenerator protocol.MetricSpecGenerator, profiler protocol.MetricProfiler, metricStore protocol.MetricStore) *DefaultGenerator {
	return &DefaultGenerator{specGenerator: specGenerator, profiler: profiler, metricStore: metricStore}
}

//Generate get metric specification, calculate metric and store
func (m *DefaultGenerator) Generate(entry protocol.Entry, profile *job.Profile) ([]*metric.Metric, error) {
	metricSpecs, err := m.specGenerator.GenerateMetricSpec(profile.URN)
	if err != nil {
		return nil, err
	}

	metrics, err := m.profiler.Profile(entry, profile, metricSpecs)
	if err != nil {
		return nil, err
	}

	if err := m.metricStore.Store(profile, metrics); err != nil {
		return nil, err
	}

	return metrics, nil
}

type DefaultProfileStatisticGenerator struct {
	metadataStore protocol.MetadataStore
	queryExecutor protocol.QueryExecutor
	profileStore  protocol.ProfileStore
}

//NewDefaultProfileStatisticGenerator create DefaultProfileStatisticGenerator
func NewDefaultProfileStatisticGenerator(metadataStore protocol.MetadataStore, queryExecutor protocol.QueryExecutor, profileStore protocol.ProfileStore) *DefaultProfileStatisticGenerator {
	return &DefaultProfileStatisticGenerator{metadataStore: metadataStore, queryExecutor: queryExecutor, profileStore: profileStore}
}

func (d *DefaultProfileStatisticGenerator) Generate(profile *job.Profile) error {
	tableMetadata, err := d.metadataStore.GetMetadata(profile.URN)
	if err != nil {
		return err
	}

	var selectExpressions []*query.SelectExpression
	exp := &query.SelectExpression{
		Expression: "count(*)",
		Alias:      totalRecordsAlias,
	}
	selectExpressions = append(selectExpressions, exp)

	q := &query.Query{
		Expressions: selectExpressions,
		From: &query.FromClause{
			TableID: profile.URN,
		},
		Where: common.GenerateFilterExpression(profile.Filter, tableMetadata),
	}

	queryString := q.String()

	result, err := d.queryExecutor.Run(profile, queryString, job.StatisticalQuery)
	if err != nil {
		return err
	}

	if len(result) == 0 {
		return errors.New("failed to calculate profiling statistics")
	}

	firstRow := result[0]
	totalRecords, ok := firstRow[totalRecordsAlias]
	if !ok {
		return errors.New("failed to calculate profiling statistics")
	}

	profile.TotalRecords = totalRecords.(int64)
	profile.Message = fmt.Sprintf("records to be profiled: %d", profile.TotalRecords)
	return d.profileStore.Update(profile)
}

//MultistageGenerator metric generator that generate metric from multiple generators
type MultistageGenerator struct {
	generators     []protocol.MetricGenerator
	profileStatGen protocol.ProfileStatisticGenerator
}

//NewMultistageGenerator create protocol.MetricsGenerator
func NewMultistageGenerator(generators []protocol.MetricGenerator, profileStatGen protocol.ProfileStatisticGenerator) *MultistageGenerator {
	return &MultistageGenerator{generators: generators, profileStatGen: profileStatGen}
}

//Generate generate metric from multiple generator
func (m *MultistageGenerator) Generate(entry protocol.Entry, profile *job.Profile) (metrics []*metric.Metric, err error) {
	err = m.profileStatGen.Generate(profile)
	if err != nil {
		return nil, err
	}
	for _, generator := range m.generators {
		result, err := generator.Generate(entry, profile)
		if err != nil {
			return nil, err
		}
		metrics = append(metrics, result...)
	}
	return metrics, nil
}
