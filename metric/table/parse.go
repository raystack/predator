package table

import (
	"errors"
	"github.com/odpf/predator/metric/common"
	"github.com/odpf/predator/protocol/metric"
)

var metricParserMap = map[metric.Type]common.RowParserType{
	metric.InvalidCount: getInvalidCountMetric,
	metric.Count:        getCountMetric,
	metric.UniqueCount:  getUniqueCountMetric,
}

func getCountMetric(result map[string]interface{}, alias string, metricSpec *metric.Spec) (*metric.Metric, error) {
	value, ok := result[alias]
	if !ok {
		return nil, errors.New("get count value failed")
	}

	recordCount, ok := value.(int64)
	if !ok {
		return nil, errors.New("parse row count value to int64 failed")
	}
	countMetric := &metric.Metric{
		Type:     metricSpec.Name,
		Category: metric.Basic,
		Owner:    metricSpec.Owner,
		Value:    float64(recordCount),
	}
	return countMetric, nil
}

func getUniqueCountMetric(result map[string]interface{}, alias string, metricSpec *metric.Spec) (*metric.Metric, error) {
	value, ok := result[alias]
	if !ok {
		return nil, errors.New("get unique count value failed")
	}

	recordUniqueCount, ok := value.(int64)
	if !ok {
		return nil, errors.New("parse unique count value to int64 failed")
	}

	uniqueCountMetric := &metric.Metric{
		Type:     metricSpec.Name,
		Category: metric.Basic,
		Owner:    metricSpec.Owner,
		Value:    float64(recordUniqueCount),
		Metadata: metricSpec.Metadata,
	}
	return uniqueCountMetric, nil
}

func getInvalidCountMetric(result map[string]interface{}, alias string, metricSpec *metric.Spec) (*metric.Metric, error) {
	value, ok := result[alias]
	if !ok {
		return nil, errors.New("get invalid count value failed")
	}

	invalidCount, ok := value.(int64)
	if !ok {
		return nil, errors.New("parse invalid count value to int64 failed")
	}

	invalidCountMetric := &metric.Metric{
		Value:     float64(invalidCount),
		Category:  metric.Basic,
		Type:      metricSpec.Name,
		Owner:     metricSpec.Owner,
		Condition: metricSpec.Condition,
	}
	return invalidCountMetric, nil
}
