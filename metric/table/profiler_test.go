package table

import (
	"errors"
	"fmt"
	"testing"

	"github.com/odpf/predator/metric/common"
	"github.com/odpf/predator/protocol/job"
	"github.com/odpf/predator/protocol/meta"
	"github.com/odpf/predator/protocol/metric"

	"github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol"
	"github.com/stretchr/testify/assert"
	testifyMock "github.com/stretchr/testify/mock"
)

func TestTableProfiler(t *testing.T) {
	entry := protocol.NewEntry()
	profile := &job.Profile{
		Filter:    "active = true",
		GroupName: "grouping_field",
		URN:       "sample-project.sample_dataset.sample_table",
	}

	t.Run("should return Table Metric with unique constraint from metric metadata", func(t *testing.T) {
		uniqueKeys := []string{"unique"}
		metricSpecs := []*metric.Spec{
			{
				Name:    metric.Count,
				TableID: "sample-project.sample_dataset.sample_table",
				Owner:   metric.Table,
			},
			{
				Name:     metric.UniqueCount,
				TableID:  "sample-project.sample_dataset.sample_table",
				Owner:    metric.Table,
				Metadata: map[string]interface{}{metric.UniqueFields: uniqueKeys},
			},
		}

		spec := &meta.TableSpec{
			ProjectName:    "sample-project",
			DatasetName:    "sample_dataset",
			TableName:      "sample_table",
			PartitionField: "_partitiontime",
			Labels:         map[string]string{"key": "value"},
			Fields:         nil,
		}
		recordCount := 300
		recordUniqueCount := 200

		rows := []protocol.Row{
			{
				"count_0":         int64(recordCount),
				"uniquecount_1":   int64(recordUniqueCount),
				common.GroupAlias: "ID",
			},
		}

		expected := []*metric.Metric{
			{
				Type:       metric.Count,
				Category:   metric.Basic,
				Owner:      metric.Table,
				Value:      float64(recordCount),
				GroupValue: "ID",
			},
			{
				Type:       metric.UniqueCount,
				Category:   metric.Basic,
				Owner:      metric.Table,
				Value:      float64(recordUniqueCount),
				GroupValue: "ID",
				Metadata:   map[string]interface{}{metric.UniqueFields: uniqueKeys},
			},
		}

		metadataStore := mock.NewMetadataStore()
		defer metadataStore.AssertExpectations(t)

		queryExecutor := mock.NewQueryExecutor()
		defer queryExecutor.AssertExpectations(t)

		queryExecutor.On("Run", testifyMock.Anything, testifyMock.AnythingOfType("string"), job.TableLevelQuery).Return(rows, nil)

		metadataStore.On("GetMetadata", profile.URN).Return(spec, nil)

		profiler := New(queryExecutor, metadataStore)
		metrics, err := profiler.Profile(entry, profile, metricSpecs)

		assert.Equal(t, expected, metrics)
		assert.Nil(t, err)
	})
	t.Run("should return Table Metric with unique constraint from store", func(t *testing.T) {
		uniqueKeys := []string{"unique"}
		metricSpecs := []*metric.Spec{
			{
				Name:    metric.Count,
				TableID: "sample-project.sample_dataset.sample_table",
				Owner:   metric.Table,
			},
			{
				Name:    metric.UniqueCount,
				TableID: "sample-project.sample_dataset.sample_table",
				Owner:   metric.Table,
			},
		}

		spec := &meta.TableSpec{
			ProjectName:    "sample-project",
			DatasetName:    "sample_dataset",
			TableName:      "sample_table",
			PartitionField: "_partitiontime",
			Labels:         map[string]string{"key": "value"},
			Fields:         nil,
		}
		recordCount := 300
		recordUniqueCount := 200

		rows := []protocol.Row{
			{
				"count_0":         int64(recordCount),
				"uniquecount_1":   int64(recordUniqueCount),
				common.GroupAlias: "ID",
			},
		}

		expected := []*metric.Metric{
			{
				Type:       metric.Count,
				Category:   metric.Basic,
				Owner:      metric.Table,
				Value:      float64(recordCount),
				GroupValue: "ID",
			},
			{
				Type:       metric.UniqueCount,
				Category:   metric.Basic,
				Owner:      metric.Table,
				Value:      float64(recordUniqueCount),
				GroupValue: "ID",
			},
		}

		metadataStore := mock.NewMetadataStore()
		defer metadataStore.AssertExpectations(t)

		queryExecutor := mock.NewQueryExecutor()
		defer queryExecutor.AssertExpectations(t)

		queryExecutor.On("Run", testifyMock.Anything, testifyMock.AnythingOfType("string"), job.TableLevelQuery).Return(rows, nil)

		metadataStore.On("GetMetadata", profile.URN).Return(spec, nil)
		metadataStore.On("GetUniqueConstraints", profile.URN).Return(uniqueKeys, nil)

		profiler := New(queryExecutor, metadataStore)
		metrics, err := profiler.Profile(entry, profile, metricSpecs)

		assert.Equal(t, expected, metrics)
		assert.Nil(t, err)
	})
	t.Run("should return error when there is an error while executing query", func(t *testing.T) {
		uniqueKeys := []string{"unique"}
		metricSpecs := []*metric.Spec{
			{
				Name:    metric.Count,
				TableID: "sample-project.sample_dataset.sample_table",
				Owner:   metric.Table,
			},
			{
				Name:     metric.UniqueCount,
				TableID:  "sample-project.sample_dataset.sample_table",
				Owner:    metric.Table,
				Metadata: map[string]interface{}{metric.UniqueFields: uniqueKeys},
			},
		}

		spec := &meta.TableSpec{
			ProjectName:    "sample-project",
			DatasetName:    "sample_dataset",
			TableName:      "sample_table",
			PartitionField: "_partitiontime",
			Labels:         map[string]string{"key": "value"},
			Fields:         nil,
		}
		queryError := errors.New("random error")

		metadataStore := mock.NewMetadataStore()
		defer metadataStore.AssertExpectations(t)

		queryExecutor := mock.NewQueryExecutor()
		defer queryExecutor.AssertExpectations(t)

		metadataStore.On("GetMetadata", profile.URN).Return(spec, nil)

		var rows []protocol.Row
		queryExecutor.On("Run", testifyMock.Anything, testifyMock.AnythingOfType("string"), job.TableLevelQuery).Return(rows, queryError)

		profiler := New(queryExecutor, metadataStore)
		tableMetric, err := profiler.Profile(entry, profile, metricSpecs)

		assert.Nil(t, tableMetric)
		assert.Equal(t, err, queryError)
	})
	t.Run("query generation", func(t *testing.T) {
		var emptyRows []protocol.Row
		projectName := "sample-project"
		datasetName := "sample_dataset"
		tableName := "sample_table"

		suites := []struct {
			Profile     *job.Profile
			Description string
			Spec        *meta.TableSpec
			Query       string
			MetricSpecs []*metric.Spec
		}{
			{
				Profile:     profile,
				Description: "with group and filter",
				Spec: &meta.TableSpec{
					ProjectName:    "sample-project",
					DatasetName:    "sample_dataset",
					TableName:      "sample_table",
					PartitionField: "",
					Labels:         map[string]string{"key": "value"},
					Fields:         nil,
				},
				Query: fmt.Sprintf(
					"SELECT grouping_field AS __group_value , count(1) as count_0 FROM `%s.%s.%s` WHERE active = true GROUP BY grouping_field",
					projectName, datasetName, tableName,
				),
				MetricSpecs: []*metric.Spec{
					{
						Name:    metric.Count,
						TableID: "sample-project.sample_dataset.sample_table",
					},
				},
			},
			{
				Profile: &job.Profile{
					Filter: "active = true",
					URN:    "sample-project.sample_dataset.sample_table",
				},
				Description: "with empty group name",
				Spec: &meta.TableSpec{
					ProjectName:    "sample-project",
					DatasetName:    "sample_dataset",
					TableName:      "sample_table",
					PartitionField: "",
					Labels:         map[string]string{"key": "value"},
					Fields:         nil,
				},
				Query: fmt.Sprintf(
					"SELECT count(1) as count_0 FROM `%s.%s.%s` WHERE active = true",
					projectName, datasetName, tableName,
				),
				MetricSpecs: []*metric.Spec{
					{
						Name:    metric.Count,
						TableID: "sample-project.sample_dataset.sample_table",
					},
				},
			},
			{
				Profile: &job.Profile{
					URN: "sample-project.sample_dataset.sample_table",
				},
				Description: "with empty filter",
				Spec: &meta.TableSpec{
					ProjectName:    "sample-project",
					DatasetName:    "sample_dataset",
					TableName:      "sample_table",
					PartitionField: "",
					Labels:         map[string]string{"key": "value"},
					Fields:         nil,
				},
				Query: fmt.Sprintf(
					"SELECT count(1) as count_0 FROM `%s.%s.%s` WHERE TRUE",
					projectName, datasetName, tableName,
				),
				MetricSpecs: []*metric.Spec{
					{
						Name:    metric.Count,
						TableID: "sample-project.sample_dataset.sample_table",
					},
				},
			},
			{
				Profile:     profile,
				Description: "with no unique key",
				Spec: &meta.TableSpec{
					ProjectName:    "sample-project",
					DatasetName:    "sample_dataset",
					TableName:      "sample_table",
					PartitionField: "",
					Labels:         map[string]string{"key": "value"},
					Fields:         nil,
				},
				Query: fmt.Sprintf(
					"SELECT grouping_field AS __group_value , count(1) as count_0 FROM `%s.%s.%s` WHERE active = true GROUP BY grouping_field",
					projectName, datasetName, tableName,
				),
				MetricSpecs: []*metric.Spec{
					{
						Name:    metric.Count,
						TableID: "sample-project.sample_dataset.sample_table",
					},
				},
			},
			{
				Profile:     profile,
				Description: "with single unique key",
				Spec: &meta.TableSpec{
					ProjectName:    "sample-project",
					DatasetName:    "sample_dataset",
					TableName:      "sample_table",
					PartitionField: "",
					Labels:         map[string]string{"key": "value"},
					Fields:         nil,
				},
				Query: fmt.Sprintf(
					"SELECT grouping_field AS __group_value , count(1) as count_0 , count(distinct field1) as uniquecount_1 FROM `%s.%s.%s` WHERE active = true GROUP BY grouping_field",
					projectName, datasetName, tableName,
				),
				MetricSpecs: []*metric.Spec{
					{
						Name:    metric.Count,
						TableID: "sample-project.sample_dataset.sample_table",
					},
					{
						Name:     metric.UniqueCount,
						TableID:  "sample-project.sample_dataset.sample_table",
						Metadata: map[string]interface{}{metric.UniqueFields: []string{"field1"}},
					},
				},
			},
			{
				Profile:     profile,
				Description: "with more than one unique key",
				Spec: &meta.TableSpec{
					ProjectName:    "sample-project",
					DatasetName:    "sample_dataset",
					TableName:      "sample_table",
					PartitionField: "",
					Labels:         map[string]string{"key": "value"},
					Fields: []*meta.FieldSpec{
						{
							Name:      "field1",
							FieldType: meta.FieldTypeString,
							Mode:      meta.ModeNullable,
							Parent:    nil,
							Level:     1,
						},
						{
							Name:      "field3",
							FieldType: meta.FieldTypeString,
							Mode:      meta.ModeNullable,
							Parent:    nil,
							Level:     1,
						},
					},
				},
				Query: fmt.Sprintf(
					"SELECT grouping_field AS __group_value , count(1) as count_0 , count(distinct CONCAT(IFNULL(CAST(field1 AS STRING),"+
						"'null'),'|',IFNULL(CAST(field3 AS STRING),'null'))) as uniquecount_1 FROM `%s.%s.%s` WHERE active = true GROUP BY grouping_field",
					projectName, datasetName, tableName),
				MetricSpecs: []*metric.Spec{
					{
						Name:    metric.Count,
						TableID: "sample-project.sample_dataset.sample_table",
					},
					{
						Name:     metric.UniqueCount,
						TableID:  "sample-project.sample_dataset.sample_table",
						Metadata: map[string]interface{}{metric.UniqueFields: []string{"field1", "field3"}},
					},
				},
			},
			{
				Profile:     profile,
				Description: "has byte unique key type",
				Spec: &meta.TableSpec{
					ProjectName:    "sample-project",
					DatasetName:    "sample_dataset",
					TableName:      "sample_table",
					PartitionField: "",
					Labels:         map[string]string{"key": "value"},
					Fields: []*meta.FieldSpec{
						{
							Name:      "field1",
							FieldType: meta.FieldTypeString,
							Mode:      meta.ModeNullable,
							Parent:    nil,
							Level:     1,
						},
						{
							Name:      "field2",
							FieldType: meta.FieldTypeBytes,
							Mode:      meta.ModeNullable,
							Parent:    nil,
							Level:     1,
						},
					},
				},
				Query: fmt.Sprintf(
					"SELECT grouping_field AS __group_value , count(1) as count_0 , count(distinct CONCAT(IFNULL(CAST(field1 AS STRING),"+
						"'null'),'|',IFNULL(TO_BASE64(field2),'null'))) as uniquecount_1 FROM `%s.%s.%s` WHERE active = true GROUP BY grouping_field",
					projectName, datasetName, tableName,
				),
				MetricSpecs: []*metric.Spec{
					{
						Name:    metric.Count,
						TableID: "sample-project.sample_dataset.sample_table",
					},
					{
						Name:     metric.UniqueCount,
						TableID:  "sample-project.sample_dataset.sample_table",
						Metadata: map[string]interface{}{metric.UniqueFields: []string{"field1", "field2"}},
					},
				},
			},
			{
				Profile:     profile,
				Description: "when invalidcount metric is expected",
				Spec: &meta.TableSpec{
					ProjectName:    "sample-project",
					DatasetName:    "sample_dataset",
					TableName:      "sample_table",
					PartitionField: "",
					Labels:         map[string]string{"key": "value"},
					Fields:         nil,
				},
				Query: fmt.Sprintf(
					"SELECT grouping_field AS __group_value , count(1) as count_0 , count(distinct field1) as uniquecount_1 , countif(field_numeric1 - field_numeric2 - field_numeric3 != 0) as invalidcount_2 FROM `%s.%s.%s` WHERE active = true GROUP BY grouping_field",
					projectName, datasetName, tableName,
				),
				MetricSpecs: []*metric.Spec{
					{
						Name:    metric.Count,
						TableID: "sample-project.sample_dataset.sample_table",
					},
					{
						Name:     metric.UniqueCount,
						TableID:  "sample-project.sample_dataset.sample_table",
						Metadata: map[string]interface{}{metric.UniqueFields: []string{"field1"}},
					},
					{
						Name:      metric.InvalidCount,
						TableID:   "sample-project.sample_dataset.sample_table",
						Condition: "field_numeric1 - field_numeric2 - field_numeric3 != 0",
					},
				},
			},
		}
		for _, test := range suites {
			t.Run(test.Description, func(t *testing.T) {
				metadataStore := mock.NewMetadataStore()
				defer metadataStore.AssertExpectations(t)

				queryExecutor := mock.NewQueryExecutor()
				defer queryExecutor.AssertExpectations(t)

				metadataStore.On("GetMetadata", profile.URN).Return(test.Spec, nil)
				queryExecutor.On("Run", testifyMock.Anything, test.Query, job.TableLevelQuery).Return(emptyRows, nil)

				profiler := New(queryExecutor, metadataStore)
				_, err := profiler.Profile(protocol.NewEntry(), test.Profile, test.MetricSpecs)
				assert.Nil(t, err)
			})
		}
	})
}
