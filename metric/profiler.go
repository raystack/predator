package metric

import (
	"errors"
	"fmt"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/odpf/predator/protocol/metric"
	"github.com/odpf/predator/protocol/xlog"
	"github.com/odpf/predator/stats"
	"log"
	"os"
	"time"
)

var logger = log.New(os.Stdout, "INFO: ", log.Lshortfile|log.LstdFlags)

//BasicMetricProfiler to profile table and fiend metric concurrently
type BasicMetricProfiler struct {
	tableProfiler      protocol.MetricProfiler
	fieldProfiler      protocol.MetricProfiler
	profileStore       protocol.ProfileStore
	statsClientBuilder stats.ClientBuilder
}

//NewBasicMetricProfiler create BasicMetricProfiler
func NewBasicMetricProfiler(tableProfiler protocol.MetricProfiler, fieldProfiler protocol.MetricProfiler, profileStore protocol.ProfileStore, statsClientBuilder stats.ClientBuilder) *BasicMetricProfiler {
	return &BasicMetricProfiler{tableProfiler: tableProfiler, fieldProfiler: fieldProfiler, profileStore: profileStore, statsClientBuilder: statsClientBuilder}
}

//Profile to start generate basic metrics
func (m *BasicMetricProfiler) Profile(entry protocol.Entry, profile *job.Profile, metricSpecs []*metric.Spec) ([]*metric.Metric, error) {
	label, err := protocol.ParseLabel(profile.URN)
	if err != nil {
		return nil, err
	}

	statsClient, err := m.statsClientBuilder.WithURN(label).Build()
	if err != nil {
		return nil, err
	}
	startTime := time.Now().In(time.UTC)

	msg := xlog.Format("calculating basic metrics", xlog.NewValue("profile_id", profile.ID))
	logger.Println(msg)

	profile.Message = msg
	if err := m.profileStore.Update(profile); err != nil {
		return nil, fmt.Errorf("unable to write log message %w", err)
	}

	resultChan := make(chan *result, 2)

	var fieldMetricSpecs []*metric.Spec
	for _, spec := range metricSpecs {
		if spec.Owner == metric.Field {
			fieldMetricSpecs = append(fieldMetricSpecs, spec)
		}
	}

	var tableMetricSpecs []*metric.Spec
	for _, spec := range metricSpecs {
		if spec.Owner == metric.Table {
			tableMetricSpecs = append(tableMetricSpecs, spec)
		}
	}

	go func() {
		tableMetrics, err := m.tableProfiler.Profile(entry, profile, tableMetricSpecs)
		resultChan <- &result{
			Value: tableMetrics,
			Error: err,
		}
	}()

	go func() {
		fieldMetrics, err := m.fieldProfiler.Profile(entry, profile, fieldMetricSpecs)
		resultChan <- &result{
			Value: fieldMetrics,
			Error: err,
		}
	}()

	results := wait(resultChan)

	for _, res := range results {
		if err = res.Error; err != nil {
			return nil, err
		}
	}

	var metrics []*metric.Metric
	for _, r := range results {
		metrics = append(metrics, r.Value...)
	}

	msg = xlog.Format("basic metrics calculation finished", xlog.NewValue("profile_id", profile.ID))
	logger.Println(msg)

	profile.Message = msg
	if err := m.profileStore.Update(profile); err != nil {
		return nil, fmt.Errorf("unable to write log message %w", err)
	}

	statsClient.DurationUntilNow("profile.job.basic_metric.time", startTime)

	return metrics, err
}

type result struct {
	Value []*metric.Metric
	Error error
}

func wait(resultChan <-chan *result) []*result {
	var results []*result
	for i := 0; i < cap(resultChan); i++ {
		res := <-resultChan
		results = append(results, res)
	}

	return results
}

//QualityMetricProfiler to profile quality metrics
type QualityMetricProfiler struct {
	metricStore        protocol.MetricStore
	profileStore       protocol.ProfileStore
	statsClientBuilder stats.ClientBuilder
}

//NewQualityMetricProfiler create QualityMetricProfiler
func NewQualityMetricProfiler(metricStore protocol.MetricStore, profileStore protocol.ProfileStore, statsClientBuilder stats.ClientBuilder) *QualityMetricProfiler {
	return &QualityMetricProfiler{metricStore: metricStore, profileStore: profileStore, statsClientBuilder: statsClientBuilder}
}

//CreateProfile to start generate quality metrics
func (m *QualityMetricProfiler) Profile(entry protocol.Entry, profile *job.Profile, metricSpecs []*metric.Spec) ([]*metric.Metric, error) {
	label, err := protocol.ParseLabel(profile.URN)
	if err != nil {
		return nil, err
	}

	statsClient, err := m.statsClientBuilder.WithURN(label).Build()
	if err != nil {
		return nil, err
	}
	startTime := time.Now().In(time.UTC)

	msg := xlog.Format("calculating quality metric", xlog.NewValue("profile_id", profile.ID))
	logger.Println(msg)

	profile.Message = msg
	if err := m.profileStore.Update(profile); err != nil {
		return nil, fmt.Errorf("unable to write log message %w", err)
	}

	if profile.TotalRecords == 0 {
		return nil, nil
	}

	preCalculatedMetrics, err := m.metricStore.GetMetricsByProfileID(profile.ID)
	if err != nil {
		e := fmt.Errorf("profile metric for table %s, with types %v ,%w", profile.URN, metric.TypeAll, err)
		logger.Println(e)
		return nil, e
	}

	groupMetrics := make(map[string][]*metric.Metric)

	for _, m := range preCalculatedMetrics {
		groupMetrics[m.GroupValue] = append(groupMetrics[m.GroupValue], m)
	}

	var qualityMetrics []*metric.Metric

	for groupValue, metrics := range groupMetrics {
		groupMetrics, err := calculateQualityMetric(metrics, metricSpecs)
		if err != nil {
			return nil, err
		}

		for _, m := range groupMetrics {
			m.GroupValue = groupValue
			qualityMetrics = append(qualityMetrics, m)
		}
	}

	msg = xlog.Format("quality metrics calculation finished", xlog.NewValue("profile_id", profile.ID))
	logger.Println(msg)

	profile.Message = msg
	if err := m.profileStore.Update(profile); err != nil {
		return nil, fmt.Errorf("unable to write log message %w", err)
	}

	statsClient.DurationUntilNow("profile.job.quality_metric.time", startTime)
	return qualityMetrics, nil
}

func calculateQualityMetric(metrics []*metric.Metric, metricSpecs []*metric.Spec) ([]*metric.Metric, error) {
	var qualityMetrics []*metric.Metric

	var tableMetricSpecs []*metric.Spec
	for _, spec := range metricSpecs {
		if spec.Owner == metric.Table {
			tableMetricSpecs = append(tableMetricSpecs, spec)
		}
	}

	tableMetrics, err := calculateTableQualityMetric(metrics, tableMetricSpecs)
	if err != nil {
		return nil, fmt.Errorf("unable to calculate table quality score ,%w", err)
	}
	qualityMetrics = append(qualityMetrics, tableMetrics...)

	var fieldMetricSpecs []*metric.Spec
	for _, spec := range metricSpecs {
		if spec.Owner == metric.Field {
			fieldMetricSpecs = append(fieldMetricSpecs, spec)
		}
	}

	fieldMetric, err := calculateFieldQualityMetric(metrics, fieldMetricSpecs)
	if err != nil {
		return nil, fmt.Errorf("unable to calculate field quality score ,%w", err)
	}
	qualityMetrics = append(qualityMetrics, fieldMetric...)

	return qualityMetrics, nil
}

func calculateTableQualityMetric(metrics []*metric.Metric, metricSpecs []*metric.Spec) ([]*metric.Metric, error) {
	finder := metric.NewFinder(metrics)
	tableMetrics := finder.WithOwner(metric.Table).Find()

	if len(metricSpecs) == 0 {
		return nil, nil
	}

	recordCountMetric := metric.NewFinder(tableMetrics).WithType(metric.Count).FindOne()
	if recordCountMetric == nil {
		return nil, fmt.Errorf("unable to get %s", string(metric.Count))
	}

	var qualityMetrics []*metric.Metric

	for _, ms := range metricSpecs {
		switch ms.Name {
		case metric.InvalidPct:
			if ms.Name == metric.InvalidPct && ms.Owner == metric.Table {
				invalidityPct, err := calculateInvalidityMetric(ms, tableMetrics)
				if err != nil {
					return nil, fmt.Errorf("unable to calculate %v ,%w", ms, err)
				}
				qualityMetrics = append(qualityMetrics, invalidityPct)
			}
		case metric.DuplicationPct:
			recordUniqueCountMetric := metric.NewFinder(tableMetrics).WithType(metric.UniqueCount).FindOne()
			if recordUniqueCountMetric == nil {
				return nil, fmt.Errorf("unable to get %s", string(metric.UniqueCount))
			}

			duplicationMetric := calculateDuplicationMetric(recordCountMetric, recordUniqueCountMetric)
			qualityMetrics = append(qualityMetrics, duplicationMetric)
		case metric.RowCount:
			rowCountMetric := calculateRowCountMetric(recordCountMetric)
			qualityMetrics = append(qualityMetrics, rowCountMetric)
		}
	}

	return qualityMetrics, nil
}

func calculateFieldQualityMetric(metrics []*metric.Metric, metricSpecs []*metric.Spec) ([]*metric.Metric, error) {
	finder := metric.NewFinder(metrics)
	recordCountMetric := finder.WithOwner(metric.Table).WithType(metric.Count).FindOne()
	if recordCountMetric == nil {
		return nil, errors.New("unable to find table count")
	}

	var qualityMetrics []*metric.Metric

	for _, spec := range metricSpecs {
		if spec.Name == metric.NullnessPct {
			nullCountMetric := metric.NewFinder(metrics).
				WithOwner(metric.Field).
				WithFieldID(spec.FieldID).
				WithType(metric.NullCount).
				FindOne()

			if nullCountMetric == nil {
				return nil, errors.New("unable to null count metric")
			}

			nullPctMetric := calculateNullnessMetric(nullCountMetric, recordCountMetric)
			qualityMetrics = append(qualityMetrics, nullPctMetric)
		}
	}

	var invalidityMetricSpecs []*metric.Spec
	for _, ms := range metricSpecs {
		if ms.Name == metric.InvalidPct {
			invalidityMetricSpecs = append(invalidityMetricSpecs, ms)
		}
	}

	for _, ms := range invalidityMetricSpecs {
		invalidMetric, err := calculateInvalidityMetric(ms, metrics)
		if err != nil {
			return nil, fmt.Errorf("unable to calculate %v ,%w", ms, err)
		}
		qualityMetrics = append(qualityMetrics, invalidMetric)
	}

	return qualityMetrics, nil
}

func calculateNullnessMetric(nullCountMetric *metric.Metric, recordCountMetric *metric.Metric) *metric.Metric {

	var metricValue float64
	if recordCountMetric.Value != 0.0 {
		metricValue = nullCountMetric.Value / recordCountMetric.Value * 100
	}

	return &metric.Metric{
		FieldID:  nullCountMetric.FieldID,
		Type:     metric.NullnessPct,
		Category: metric.Quality,
		Owner:    metric.Field,
		Value:    metricValue,
	}
}

func calculateRowCountMetric(recordCountMetric *metric.Metric) *metric.Metric {

	return &metric.Metric{
		Type:     metric.RowCount,
		Category: metric.Quality,
		Owner:    metric.Table,
		Value:    recordCountMetric.Value,
	}
}

func calculateDuplicationMetric(recordCountMetric *metric.Metric, recordUniqueCountMetric *metric.Metric) *metric.Metric {
	var duplicationMetric float64

	if recordCountMetric.Value != 0 {
		duplicationMetric = (recordCountMetric.Value - recordUniqueCountMetric.Value) / (recordCountMetric.Value) * 100
	}

	return &metric.Metric{
		Type:     metric.DuplicationPct,
		Category: metric.Quality,
		Owner:    metric.Table,
		Metadata: recordUniqueCountMetric.Metadata,
		Value:    duplicationMetric,
	}
}

func calculateInvalidityMetric(metricSpec *metric.Spec, metrics []*metric.Metric) (*metric.Metric, error) {
	invalidityCountMetric := metric.NewFinder(metrics).
		WithFieldID(metricSpec.FieldID).
		WithCondition(metricSpec.Condition).
		FindOne()

	if invalidityCountMetric == nil {
		return nil, fmt.Errorf("unable to get %s", metric.InvalidCount)
	}

	tableMetrics := metric.NewFinder(metrics).WithOwner(metric.Table).Find()
	recordCountMetric := metric.NewFinder(tableMetrics).WithType(metric.Count).FindOne()
	if recordCountMetric == nil {
		return nil, fmt.Errorf("unable to get %s ", string(metric.Count))
	}

	var value float64
	if recordCountMetric.Value != 0.0 {
		value = invalidityCountMetric.Value / recordCountMetric.Value * 100.0
	}

	return &metric.Metric{
		FieldID:   metricSpec.FieldID,
		Type:      metricSpec.Name,
		Category:  metric.Quality,
		Owner:     metricSpec.Owner,
		Condition: invalidityCountMetric.Condition,
		Value:     value,
	}, nil
}
