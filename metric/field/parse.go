package field

import (
	"fmt"
	"github.com/odpf/predator/metric/common"
	"github.com/odpf/predator/protocol/metric"
)

var metricParserMap = map[metric.Type]common.RowParserType{
	metric.InvalidCount: getInvalidCountMetric,
	metric.Count:        getCountMetric,
	metric.NullCount:    getNullCountMetric,
	metric.Sum:          getSumMetric,
}

func getCountMetric(result map[string]interface{}, alias string, metricSpec *metric.Spec) (*metric.Metric, error) {
	value, ok := result[alias]
	if !ok {
		return nil, fmt.Errorf("count value with alias %s, not found", alias)
	}
	count, ok := value.(int64)
	if !ok {
		return nil, fmt.Errorf("parse count value to int64 with alias %s, failed", alias)
	}
	countMetric := &metric.Metric{
		FieldID:  metricSpec.FieldID,
		Type:     metric.Count,
		Category: metric.Basic,
		Owner:    metric.Field,
		Value:    float64(count),
	}
	return countMetric, nil
}

func getNullCountMetric(result map[string]interface{}, alias string, metricSpec *metric.Spec) (*metric.Metric, error) {
	value, ok := result[alias]
	if !ok {
		return nil, fmt.Errorf("nullcount value with alias %s, not found", alias)
	}
	nullCount, ok := value.(int64)
	if !ok {
		return nil, fmt.Errorf("parse nullcount value to int64 with alias %s, failed", alias)
	}
	nullCountMetric := &metric.Metric{
		FieldID:  metricSpec.FieldID,
		Type:     metric.NullCount,
		Category: metric.Basic,
		Owner:    metric.Field,
		Value:    float64(nullCount),
	}
	return nullCountMetric, nil
}

func getInvalidCountMetric(result map[string]interface{}, alias string, metricSpec *metric.Spec) (*metric.Metric, error) {
	value, ok := result[alias]
	if !ok {
		return nil, fmt.Errorf("invalid count value with alias %s, not found", alias)
	}
	invalidCount, ok := value.(int64)
	if !ok {
		return nil, fmt.Errorf("parse invalid count value to int64 with alias %s, failed", alias)
	}
	invalidCountMetric := &metric.Metric{
		FieldID:   metricSpec.FieldID,
		Type:      metric.InvalidCount,
		Category:  metric.Basic,
		Owner:     metric.Field,
		Value:     float64(invalidCount),
		Condition: metricSpec.Condition,
	}
	return invalidCountMetric, nil
}

func getSumMetric(result map[string]interface{}, alias string, metricSpec *metric.Spec) (*metric.Metric, error) {
	value, ok := result[alias]
	if !ok {
		return nil, fmt.Errorf("sum value with alias %s, not found", alias)
	}
	sum, ok := value.(float64)
	if !ok {
		return nil, fmt.Errorf("parse sum value to float64 with alias %s, failed", alias)
	}
	sumMetric := &metric.Metric{
		FieldID:  metricSpec.FieldID,
		Type:     metric.Sum,
		Category: metric.Quality,
		Owner:    metric.Field,
		Value:    sum,
	}
	return sumMetric, nil
}
