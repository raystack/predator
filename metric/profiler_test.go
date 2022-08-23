package metric

import (
	"errors"
	"github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/odpf/predator/protocol/metric"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDefaultMetricProfiler(t *testing.T) {
	t.Run("Profile", func(t *testing.T) {
		t.Run("should call table profiler and field profiler", func(t *testing.T) {
			profile := &job.Profile{
				URN: "a.b.c",
			}
			label := &protocol.Label{
				Project: "a",
				Dataset: "b",
				Table:   "c",
			}
			entry := protocol.NewEntry()

			metricSpecs := []*metric.Spec{
				{
					Name:  metric.UniqueCount,
					Owner: metric.Table,
				},
				{
					Name:  metric.Count,
					Owner: metric.Table,
				},
				{
					Name:    metric.NullCount,
					FieldID: "order_number",
					Owner:   metric.Field,
				},
				{
					Name:    metric.Count,
					FieldID: "order_number",
					Owner:   metric.Field,
				},
			}

			tableMetricSpec := []*metric.Spec{
				metricSpecs[0],
				metricSpecs[1],
			}

			fieldMetricSpec := []*metric.Spec{
				metricSpecs[2],
				metricSpecs[3],
			}

			metrics := []*metric.Metric{
				{
					Type:  metric.UniqueCount,
					Owner: metric.Table,
					Value: 20,
				},
				{
					Type:  metric.Count,
					Owner: metric.Table,
					Value: 20,
				},
				{
					FieldID: "order_number",
					Type:    metric.NullCount,
					Owner:   metric.Field,
					Value:   0,
				},
				{
					FieldID: "order_number",
					Type:    metric.Count,
					Owner:   metric.Field,
					Value:   20,
				},
			}

			tableMetrics := []*metric.Metric{
				metrics[0],
				metrics[1],
			}

			fieldMetrics := []*metric.Metric{
				metrics[2],
				metrics[3],
			}

			fieldProfiler := mock.NewProfiler()
			defer fieldProfiler.AssertExpectations(t)

			tableProfiler := mock.NewProfiler()
			defer tableProfiler.AssertExpectations(t)

			fieldProfiler.On("Profile", entry, profile, fieldMetricSpec).Return(fieldMetrics, nil)
			tableProfiler.On("Profile", entry, profile, tableMetricSpec).Return(tableMetrics, nil)

			profileStore := mock.NewProfileStoreStub()

			statsClientBuilder := mock.NewStatBuilder()
			defer statsClientBuilder.AssertExpectations(t)

			statsClient := mock.NewDummyStats()
			statsClientBuilder.On("WithURN", label).Return(statsClientBuilder)
			statsClientBuilder.On("Build").Return(statsClient, nil)

			profiler := NewBasicMetricProfiler(tableProfiler, fieldProfiler, profileStore, statsClientBuilder)

			result, err := profiler.Profile(entry, profile, metricSpecs)

			assert.Nil(t, err)
			assert.ElementsMatch(t, metrics, result)
		})
		t.Run("should return error when table profiler failed", func(t *testing.T) {
			someError := errors.New("API error")
			profile := &job.Profile{
				URN: "a.b.c",
			}
			entry := protocol.NewEntry()
			label := &protocol.Label{
				Project: "a",
				Dataset: "b",
				Table:   "c",
			}

			metricSpecs := []*metric.Spec{
				{
					Name:  metric.UniqueCount,
					Owner: metric.Table,
				},
				{
					Name:  metric.Count,
					Owner: metric.Table,
				},
				{
					Name:    metric.NullCount,
					FieldID: "order_number",
					Owner:   metric.Field,
				},
				{
					Name:    metric.Count,
					FieldID: "order_number",
					Owner:   metric.Field,
				},
			}

			tableMetricSpec := []*metric.Spec{
				metricSpecs[0],
				metricSpecs[1],
			}

			fieldMetricSpec := []*metric.Spec{
				metricSpecs[2],
				metricSpecs[3],
			}

			var tableMetrics []*metric.Metric

			fieldMetrics := []*metric.Metric{
				{
					FieldID: "order_number",
					Type:    metric.NullCount,
					Owner:   metric.Field,
					Value:   0,
				},
				{
					FieldID: "order_number",
					Type:    metric.Count,
					Owner:   metric.Field,
					Value:   20,
				},
			}

			fieldProfiler := mock.NewProfiler()
			defer fieldProfiler.AssertExpectations(t)

			tableProfiler := mock.NewProfiler()
			defer tableProfiler.AssertExpectations(t)

			fieldProfiler.On("Profile", entry, profile, fieldMetricSpec).Return(fieldMetrics, nil)
			tableProfiler.On("Profile", entry, profile, tableMetricSpec).Return(tableMetrics, someError)

			profileStore := mock.NewProfileStoreStub()

			statsClientBuilder := mock.NewStatBuilder()
			defer statsClientBuilder.AssertExpectations(t)

			statsClient := mock.NewDummyStats()
			statsClientBuilder.On("WithURN", label).Return(statsClientBuilder)
			statsClientBuilder.On("Build").Return(statsClient, nil)

			profiler := NewBasicMetricProfiler(tableProfiler, fieldProfiler, profileStore, statsClientBuilder)

			result, err := profiler.Profile(entry, profile, metricSpecs)

			assert.Nil(t, result)
			assert.Error(t, err)
		})
		t.Run("should return error when field profiler failed", func(t *testing.T) {
			someError := errors.New("API error")

			profile := &job.Profile{
				URN: "a.b.c",
			}
			entry := protocol.NewEntry()
			label := &protocol.Label{
				Project: "a",
				Dataset: "b",
				Table:   "c",
			}

			metricSpecs := []*metric.Spec{
				{
					Name:  metric.UniqueCount,
					Owner: metric.Table,
				},
				{
					Name:  metric.Count,
					Owner: metric.Table,
				},
				{
					Name:    metric.NullCount,
					FieldID: "order_number",
					Owner:   metric.Field,
				},
				{
					Name:    metric.Count,
					FieldID: "order_number",
					Owner:   metric.Field,
				},
			}

			tableMetricSpec := []*metric.Spec{
				metricSpecs[0],
				metricSpecs[1],
			}

			fieldMetricSpec := []*metric.Spec{
				metricSpecs[2],
				metricSpecs[3],
			}

			tableMetrics := []*metric.Metric{
				{
					Type:  metric.UniqueCount,
					Owner: metric.Table,
					Value: 20,
				},
				{
					Type:  metric.Count,
					Owner: metric.Table,
					Value: 20,
				},
			}

			var fieldMetrics []*metric.Metric

			fieldProfiler := mock.NewProfiler()
			defer fieldProfiler.AssertExpectations(t)

			tableProfiler := mock.NewProfiler()
			defer tableProfiler.AssertExpectations(t)

			fieldProfiler.On("Profile", entry, profile, fieldMetricSpec).Return(fieldMetrics, nil)
			tableProfiler.On("Profile", entry, profile, tableMetricSpec).Return(tableMetrics, someError)

			profileStore := mock.NewProfileStoreStub()

			statsClientBuilder := mock.NewStatBuilder()
			defer statsClientBuilder.AssertExpectations(t)

			statsClient := mock.NewDummyStats()
			statsClientBuilder.On("WithURN", label).Return(statsClientBuilder)
			statsClientBuilder.On("Build").Return(statsClient, nil)

			profiler := NewBasicMetricProfiler(tableProfiler, fieldProfiler, profileStore, statsClientBuilder)

			result, err := profiler.Profile(entry, profile, metricSpecs)

			assert.Nil(t, result)
			assert.Error(t, err)
		})
	})
}

func TestQualityMetricProfiler(t *testing.T) {
	t.Run("Profile", func(t *testing.T) {
		t.Run("should calculate metric", func(t *testing.T) {
			urn := "sample-project.sample_dataset.sample_table"
			profile := &job.Profile{
				ID:           "job-1234",
				URN:          urn,
				TotalRecords: 20,
			}
			label := &protocol.Label{
				Project: "sample-project",
				Dataset: "sample_dataset",
				Table:   "sample_table",
			}
			metricSpecs := []*metric.Spec{
				{
					Name:    metric.DuplicationPct,
					TableID: urn,
					Owner:   metric.Table,
				},
				{
					Name:    metric.RowCount,
					TableID: urn,
					Owner:   metric.Table,
				},
			}

			metrics := []*metric.Metric{
				{
					Category:   metric.Basic,
					Owner:      metric.Table,
					Type:       metric.UniqueCount,
					Value:      100.0,
					GroupValue: "1",
				},
				{
					Category:   metric.Basic,
					Owner:      metric.Table,
					Type:       metric.Count,
					Value:      200.0,
					GroupValue: "1",
				},
				{
					Category:   metric.Basic,
					Owner:      metric.Table,
					Type:       metric.UniqueCount,
					Value:      100.0,
					GroupValue: "2",
				},
				{
					Category:   metric.Basic,
					Owner:      metric.Table,
					Type:       metric.Count,
					Value:      200.0,
					GroupValue: "2",
				},
			}

			expected := []*metric.Metric{
				{
					Category:   metric.Quality,
					Owner:      metric.Table,
					Type:       metric.DuplicationPct,
					Value:      50,
					GroupValue: "1",
				},
				{
					Category:   metric.Quality,
					Owner:      metric.Table,
					Type:       metric.RowCount,
					Value:      200.0,
					GroupValue: "1",
				},
				{
					Category:   metric.Quality,
					Owner:      metric.Table,
					Type:       metric.DuplicationPct,
					Value:      50,
					GroupValue: "2",
				},
				{
					Category:   metric.Quality,
					Owner:      metric.Table,
					Type:       metric.RowCount,
					Value:      200.0,
					GroupValue: "2",
				},
			}

			entry := protocol.NewEntry()

			metricStore := mock.NewMetricStore()
			defer metricStore.AssertExpectations(t)
			metricStore.On("GetMetricsByProfileID", profile.ID).Return(metrics, nil)

			profileStore := mock.NewProfileStoreStub()

			statsClientBuilder := mock.NewStatBuilder()
			defer statsClientBuilder.AssertExpectations(t)

			statsClient := mock.NewDummyStats()
			statsClientBuilder.On("WithURN", label).Return(statsClientBuilder)
			statsClientBuilder.On("Build").Return(statsClient, nil)

			profiler := NewQualityMetricProfiler(metricStore, profileStore, statsClientBuilder)
			result, err := profiler.Profile(entry, profile, metricSpecs)

			assert.Nil(t, err)
			assert.ElementsMatch(t, expected, result)
		})
		t.Run("should return error when get required metrics failed", func(t *testing.T) {
			someError := errors.New("some error")
			urn := "sample-project.sample_dataset.sample_table"
			profile := &job.Profile{
				ID:           "job-1234",
				URN:          urn,
				TotalRecords: 20,
			}
			label := &protocol.Label{
				Project: "sample-project",
				Dataset: "sample_dataset",
				Table:   "sample_table",
			}
			metricSpecs := []*metric.Spec{
				{
					Name:    metric.UniqueCount,
					TableID: urn,
					Owner:   metric.Table,
				},
				{
					Name:    metric.Count,
					TableID: urn,
					Owner:   metric.Table,
				},
			}

			var metrics []*metric.Metric

			entry := protocol.NewEntry()

			metricStore := mock.NewMetricStore()
			defer metricStore.AssertExpectations(t)
			metricStore.On("GetMetricsByProfileID", profile.ID).Return(metrics, someError)

			profileStore := mock.NewProfileStoreStub()

			statsClientBuilder := mock.NewStatBuilder()
			defer statsClientBuilder.AssertExpectations(t)

			statsClient := mock.NewDummyStats()
			statsClientBuilder.On("WithURN", label).Return(statsClientBuilder)
			statsClientBuilder.On("Build").Return(statsClient, nil)

			profiler := NewQualityMetricProfiler(metricStore, profileStore, statsClientBuilder)
			result, err := profiler.Profile(entry, profile, metricSpecs)

			assert.Nil(t, result)
			assert.Error(t, err)
		})
		t.Run("should return when total records is zero", func(t *testing.T) {
			urn := "sample-project.sample_dataset.sample_table"
			profile := &job.Profile{
				ID:           "job-1234",
				URN:          urn,
				TotalRecords: 0,
			}
			label := &protocol.Label{
				Project: "sample-project",
				Dataset: "sample_dataset",
				Table:   "sample_table",
			}

			var metricSpecs []*metric.Spec

			entry := protocol.NewEntry()

			metricStore := mock.NewMetricStore()
			defer metricStore.AssertExpectations(t)

			profileStore := mock.NewProfileStoreStub()
			statsClientBuilder := mock.NewStatBuilder()
			defer statsClientBuilder.AssertExpectations(t)

			statsClient := mock.NewDummyStats()
			statsClientBuilder.On("WithURN", label).Return(statsClientBuilder)
			statsClientBuilder.On("Build").Return(statsClient, nil)

			profiler := NewQualityMetricProfiler(metricStore, profileStore, statsClientBuilder)
			result, err := profiler.Profile(entry, profile, metricSpecs)

			assert.Nil(t, err)
			assert.Nil(t, result)
		})
	})
	t.Run("calculateQualityMetric", func(t *testing.T) {
		t.Run("should return quality metrics", func(t *testing.T) {
			fieldID := "field1"

			condition1 := "1 == 1"
			condition2 := "1 == 2"
			duplicationMetricMetadata := map[string]interface{}{
				metric.UniqueFields: []string{"unique_field"},
			}

			tableMetricSpecs := []*metric.Spec{
				{
					Name:     metric.DuplicationPct,
					Owner:    metric.Table,
					Metadata: duplicationMetricMetadata,
				},
				{
					Name:  metric.RowCount,
					Owner: metric.Table,
				},
				{
					Name:      metric.InvalidPct,
					Owner:     metric.Table,
					Condition: condition1,
				},
				{
					Name:      metric.InvalidPct,
					Owner:     metric.Table,
					Condition: condition2,
				},
			}

			fieldMetricSpecs := []*metric.Spec{
				{
					Name:    metric.NullnessPct,
					FieldID: fieldID,
					Owner:   metric.Field,
				},
				{
					Name:    metric.TrendInconsistencyPct,
					FieldID: fieldID,
					Owner:   metric.Field,
				},
				{
					Name:      metric.InvalidPct,
					FieldID:   fieldID,
					Owner:     metric.Field,
					Condition: condition1,
				},
				{
					Name:      metric.InvalidPct,
					FieldID:   fieldID,
					Owner:     metric.Field,
					Condition: condition2,
				},
			}

			var metricSpecs []*metric.Spec
			metricSpecs = append(metricSpecs, tableMetricSpecs...)
			metricSpecs = append(metricSpecs, fieldMetricSpecs...)

			metrics := []*metric.Metric{
				{
					Category: metric.Basic,
					Owner:    metric.Table,
					Type:     metric.UniqueCount,
					Value:    100.0,
					Metadata: duplicationMetricMetadata,
				},
				{
					Category: metric.Basic,
					Owner:    metric.Table,
					Type:     metric.Count,
					Value:    200.0,
				},
				{
					Category:  metric.Basic,
					Owner:     metric.Table,
					Type:      metric.InvalidCount,
					Condition: condition1,
					Value:     100.0,
				},
				{
					Category:  metric.Basic,
					Owner:     metric.Table,
					Type:      metric.InvalidCount,
					Condition: condition2,
					Value:     50.0,
				},
				{
					Category: metric.Basic,
					Owner:    metric.Field,
					FieldID:  "field1",
					Type:     metric.Count,
					Value:    180.0,
				},
				{
					Category: metric.Basic,
					Owner:    metric.Field,
					FieldID:  "field1",
					Type:     metric.NullCount,
					Value:    20.0,
				},
				{
					Category:  metric.Basic,
					Owner:     metric.Field,
					FieldID:   "field1",
					Type:      metric.InvalidCount,
					Condition: condition1,
					Value:     100.0,
				},
				{
					Category:  metric.Basic,
					Owner:     metric.Field,
					FieldID:   "field1",
					Type:      metric.InvalidCount,
					Condition: condition2,
					Value:     50.0,
				},
			}

			qualityMetrics := []*metric.Metric{
				{
					Category: metric.Quality,
					Owner:    metric.Table,
					Type:     metric.DuplicationPct,
					Value:    50.0,
					Metadata: duplicationMetricMetadata,
				},
				{
					Category: metric.Quality,
					Owner:    metric.Table,
					Type:     metric.RowCount,
					Value:    200.0,
				},
				{
					Category:  metric.Quality,
					Owner:     metric.Table,
					Type:      metric.InvalidPct,
					Condition: condition1,
					Value:     50.0,
				},
				{
					Category:  metric.Quality,
					Owner:     metric.Table,
					Type:      metric.InvalidPct,
					Condition: condition2,
					Value:     25.0,
				},
				{
					Category: metric.Quality,
					Owner:    metric.Field,
					FieldID:  fieldID,
					Type:     metric.NullnessPct,
					Value:    10.0,
				},
				{
					Category: metric.Quality,
					Owner:    metric.Field,
					FieldID:  fieldID, Type: metric.InvalidPct,
					Condition: condition1,
					Value:     50.0,
				},
				{
					Category:  metric.Quality,
					Owner:     metric.Field,
					FieldID:   fieldID,
					Type:      metric.InvalidPct,
					Condition: condition2,
					Value:     25.0,
				},
			}

			result, err := calculateQualityMetric(metrics, metricSpecs)

			assert.Equal(t, qualityMetrics, result)
			assert.Nil(t, err)
		})
	})
}
