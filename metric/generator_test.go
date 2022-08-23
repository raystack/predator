package metric

import (
	"errors"
	"testing"

	metricmock "github.com/odpf/predator/metric/mock"
	"github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/odpf/predator/protocol/meta"
	"github.com/odpf/predator/protocol/metric"
	"github.com/stretchr/testify/assert"
)

func TestDefaultMetricGenerator(t *testing.T) {
	t.Run("DefaultGenerator", func(t *testing.T) {
		t.Run("Generate", func(t *testing.T) {
			t.Run("should generate metric spec, calculate metric and store the result", func(t *testing.T) {
				profile := &job.Profile{
					ID:        "1234",
					Filter:    "field_status = 'sample_status'",
					GroupName: "field_grouping",
					URN:       "sample-project.sample_dataset.sample_table",
				}
				entry := protocol.NewEntry()

				var metrics []*metric.Metric
				var metricSpecs []*metric.Spec

				specGenerator := metricmock.NewMetricSpecGenerator()
				defer specGenerator.AssertExpectations(t)

				metricStore := mock.NewMetricStore()
				defer metricStore.AssertExpectations(t)

				profiler := mock.NewProfiler()
				defer profiler.AssertExpectations(t)

				specGenerator.On("GenerateMetricSpec", profile.URN).Return(metricSpecs, nil)
				profiler.On("Profile", entry, profile, metricSpecs).Return(metrics, nil)
				metricStore.On("Store", profile, metrics).Return(nil)

				generator := NewDefaultGenerator(specGenerator, profiler, metricStore)
				result, err := generator.Generate(entry, profile)

				assert.Nil(t, err)
				assert.Equal(t, metrics, result)
			})
			t.Run("should return error when metric spec generation failed", func(t *testing.T) {
				someErr := errors.New("API error")
				profile := &job.Profile{
					ID:        "1234",
					Filter:    "field_status = 'sample_status'",
					GroupName: "field_grouping",
					URN:       "sample-project.sample_dataset.sample_table",
				}
				entry := protocol.NewEntry()

				var metricSpecs []*metric.Spec

				specGenerator := metricmock.NewMetricSpecGenerator()
				defer specGenerator.AssertExpectations(t)

				metricStore := mock.NewMetricStore()
				defer metricStore.AssertExpectations(t)

				profiler := mock.NewProfiler()
				defer profiler.AssertExpectations(t)

				specGenerator.On("GenerateMetricSpec", profile.URN).Return(metricSpecs, someErr)

				generator := NewDefaultGenerator(specGenerator, profiler, metricStore)
				result, err := generator.Generate(entry, profile)

				assert.Nil(t, result)
				assert.Error(t, err)
			})
			t.Run("should return error profile failed", func(t *testing.T) {
				someErr := errors.New("API error")
				profile := &job.Profile{
					ID:        "1234",
					Filter:    "field_status = 'sample_status'",
					GroupName: "field_grouping",
					URN:       "sample-project.sample_dataset.sample_table",
				}
				entry := protocol.NewEntry()

				var metrics []*metric.Metric
				var metricSpecs []*metric.Spec

				specGenerator := metricmock.NewMetricSpecGenerator()
				defer specGenerator.AssertExpectations(t)

				metricStore := mock.NewMetricStore()
				defer metricStore.AssertExpectations(t)

				profiler := mock.NewProfiler()
				defer profiler.AssertExpectations(t)

				specGenerator.On("GenerateMetricSpec", profile.URN).Return(metricSpecs, nil)
				profiler.On("Profile", entry, profile, metricSpecs).Return(metrics, someErr)

				generator := NewDefaultGenerator(specGenerator, profiler, metricStore)
				result, err := generator.Generate(entry, profile)

				assert.Nil(t, result)
				assert.Error(t, err)
			})
			t.Run("should return error store metrics failed", func(t *testing.T) {
				someErr := errors.New("API error")
				profile := &job.Profile{
					ID:        "1234",
					Filter:    "field_status = 'sample_status'",
					GroupName: "field_grouping",
					URN:       "sample-project.sample_dataset.sample_table",
				}
				entry := protocol.NewEntry()

				var metrics []*metric.Metric
				var metricSpecs []*metric.Spec

				specGenerator := metricmock.NewMetricSpecGenerator()
				defer specGenerator.AssertExpectations(t)

				metricStore := mock.NewMetricStore()
				defer metricStore.AssertExpectations(t)

				profiler := mock.NewProfiler()
				defer profiler.AssertExpectations(t)

				specGenerator.On("GenerateMetricSpec", profile.URN).Return(metricSpecs, nil)
				profiler.On("Profile", entry, profile, metricSpecs).Return(metrics, nil)
				metricStore.On("Store", profile, metrics).Return(someErr)

				generator := NewDefaultGenerator(specGenerator, profiler, metricStore)
				result, err := generator.Generate(entry, profile)

				assert.Nil(t, result)
				assert.Error(t, err)
			})
		})
	})
}

func TestMultistageGenerator(t *testing.T) {
	t.Run("MultistageGenerator", func(t *testing.T) {
		t.Run("Generate", func(t *testing.T) {
			t.Run("should generate metrics", func(t *testing.T) {
				entry := protocol.NewEntry()
				profile := &job.Profile{
					ID:        "1234",
					Filter:    "field_status = 'sample_status'",
					GroupName: "field_grouping",
					URN:       "sample-project.sample_dataset.sample_table",
				}

				basicMetrics := []*metric.Metric{
					{
						ID: "1",
					},
				}

				qualityMetrics := []*metric.Metric{
					{
						ID: "2",
					},
				}

				expected := []*metric.Metric{
					{
						ID: "1",
					},
					{
						ID: "2",
					},
				}

				statisticGenerator := mock.NewProfileStatisticGenerator()
				defer statisticGenerator.AssertExpectations(t)
				statisticGenerator.On("Generate", profile).Return(nil)

				basicMetricGenerator := mock.NewMetricGenerator()
				defer basicMetricGenerator.AssertExpectations(t)
				basicMetricGenerator.On("Generate", entry, profile).Return(basicMetrics, nil)

				qualityMetricGenerator := mock.NewMetricGenerator()
				defer qualityMetricGenerator.AssertExpectations(t)
				qualityMetricGenerator.On("Generate", entry, profile).Return(qualityMetrics, nil)

				generators := []protocol.MetricGenerator{basicMetricGenerator, qualityMetricGenerator}
				multipleMetricGenerator := NewMultistageGenerator(generators, statisticGenerator)

				metrics, err := multipleMetricGenerator.Generate(entry, profile)

				assert.Nil(t, err)
				assert.Equal(t, expected, metrics)
			})
			t.Run("should return error when one stage failed", func(t *testing.T) {
				someError := errors.New("API error")
				entry := protocol.NewEntry()
				profile := &job.Profile{
					ID:        "1234",
					Filter:    "field_status = 'sample_status'",
					GroupName: "field_grouping",
					URN:       "sample-project.sample_dataset.sample_table",
				}

				var basicMetrics []*metric.Metric

				statisticGenerator := mock.NewProfileStatisticGenerator()
				defer statisticGenerator.AssertExpectations(t)
				statisticGenerator.On("Generate", profile).Return(nil)

				basicMetricGenerator := mock.NewMetricGenerator()
				defer basicMetricGenerator.AssertExpectations(t)
				basicMetricGenerator.On("Generate", entry, profile).Return(basicMetrics, someError)

				qualityMetricGenerator := mock.NewMetricGenerator()
				defer qualityMetricGenerator.AssertExpectations(t)

				generators := []protocol.MetricGenerator{basicMetricGenerator, qualityMetricGenerator}
				multipleMetricGenerator := NewMultistageGenerator(generators, statisticGenerator)

				metrics, err := multipleMetricGenerator.Generate(entry, profile)

				assert.Nil(t, metrics)
				assert.Error(t, err)
			})
			t.Run("should return error when generate statistic failed", func(t *testing.T) {
				someError := errors.New("API error")
				entry := protocol.NewEntry()
				profile := &job.Profile{
					ID:        "1234",
					Filter:    "field_status = 'sample_status'",
					GroupName: "field_grouping",
					URN:       "sample-project.sample_dataset.sample_table",
				}

				statisticGenerator := mock.NewProfileStatisticGenerator()
				defer statisticGenerator.AssertExpectations(t)
				statisticGenerator.On("Generate", profile).Return(someError)

				basicMetricGenerator := mock.NewMetricGenerator()
				defer basicMetricGenerator.AssertExpectations(t)

				qualityMetricGenerator := mock.NewMetricGenerator()
				defer qualityMetricGenerator.AssertExpectations(t)

				generators := []protocol.MetricGenerator{basicMetricGenerator, qualityMetricGenerator}
				multipleMetricGenerator := NewMultistageGenerator(generators, statisticGenerator)

				metrics, err := multipleMetricGenerator.Generate(entry, profile)

				assert.Nil(t, metrics)
				assert.Error(t, err)
			})
		})
	})
}

func TestDefaultProfileStatisticGenerator(t *testing.T) {
	t.Run("Generate", func(t *testing.T) {
		t.Run("should generate total records", func(t *testing.T) {
			totalRecords := int64(20)
			profile := &job.Profile{
				ID:        "1234",
				Filter:    "field_status = 'sample_status'",
				GroupName: "field_grouping",
				URN:       "sample-project.sample_dataset.sample_table",
			}

			tableMeta := &meta.TableSpec{}

			queryString := "SELECT count(*) AS total_records FROM `sample-project.sample_dataset.sample_table` WHERE field_status = 'sample_status'"
			rows := []protocol.Row{
				{"total_records": int64(20)},
			}

			updatedProfile := &job.Profile{
				ID:           "1234",
				Filter:       "field_status = 'sample_status'",
				GroupName:    "field_grouping",
				URN:          "sample-project.sample_dataset.sample_table",
				TotalRecords: 20,
				Message:      "records to be profiled: 20",
			}

			metadataStore := mock.NewMetadataStore()
			defer metadataStore.AssertExpectations(t)

			queryExecutor := mock.NewQueryExecutor()
			defer queryExecutor.AssertExpectations(t)

			profileStore := mock.NewProfileStore()
			defer profileStore.AssertExpectations(t)

			metadataStore.On("GetMetadata", profile.URN).Return(tableMeta, nil)

			queryExecutor.On("Run", profile, queryString, job.StatisticalQuery).Return(rows, nil)

			profileStore.On("Update", updatedProfile).Return(nil)

			statisticGenerator := NewDefaultProfileStatisticGenerator(metadataStore, queryExecutor, profileStore)

			err := statisticGenerator.Generate(profile)

			assert.Nil(t, err)
			assert.Equal(t, totalRecords, profile.TotalRecords)
		})
		t.Run("should return error when query run failed", func(t *testing.T) {
			someError := errors.New("API error")
			profile := &job.Profile{
				ID:        "1234",
				Filter:    "field_status = 'sample_status'",
				GroupName: "field_grouping",
				URN:       "sample-project.sample_dataset.sample_table",
			}

			tableMeta := &meta.TableSpec{}

			queryString := "SELECT count(*) AS total_records FROM `sample-project.sample_dataset.sample_table` WHERE field_status = 'sample_status'"
			var rows []protocol.Row

			metadataStore := mock.NewMetadataStore()
			defer metadataStore.AssertExpectations(t)

			queryExecutor := mock.NewQueryExecutor()
			defer queryExecutor.AssertExpectations(t)

			profileStore := mock.NewProfileStore()
			defer profileStore.AssertExpectations(t)

			metadataStore.On("GetMetadata", profile.URN).Return(tableMeta, nil)

			queryExecutor.On("Run", profile, queryString, job.StatisticalQuery).Return(rows, someError)

			statisticGenerator := NewDefaultProfileStatisticGenerator(metadataStore, queryExecutor, profileStore)

			err := statisticGenerator.Generate(profile)

			assert.Error(t, err)
		})
		t.Run("should return error when query return no rows", func(t *testing.T) {
			profile := &job.Profile{
				ID:        "1234",
				Filter:    "field_status = 'sample_status'",
				GroupName: "field_grouping",
				URN:       "sample-project.sample_dataset.sample_table",
			}

			tableMeta := &meta.TableSpec{}

			queryString := "SELECT count(*) AS total_records FROM `sample-project.sample_dataset.sample_table` WHERE field_status = 'sample_status'"
			var rows []protocol.Row

			metadataStore := mock.NewMetadataStore()
			defer metadataStore.AssertExpectations(t)

			queryExecutor := mock.NewQueryExecutor()
			defer queryExecutor.AssertExpectations(t)

			profileStore := mock.NewProfileStore()
			defer profileStore.AssertExpectations(t)

			metadataStore.On("GetMetadata", profile.URN).Return(tableMeta, nil)

			queryExecutor.On("Run", profile, queryString, job.StatisticalQuery).Return(rows, nil)

			statisticGenerator := NewDefaultProfileStatisticGenerator(metadataStore, queryExecutor, profileStore)

			err := statisticGenerator.Generate(profile)

			assert.Error(t, err)
		})
		t.Run("should return error when total_records not found on query result", func(t *testing.T) {
			profile := &job.Profile{
				ID:        "1234",
				Filter:    "field_status = 'sample_status'",
				GroupName: "field_grouping",
				URN:       "sample-project.sample_dataset.sample_table",
			}

			tableMeta := &meta.TableSpec{}

			queryString := "SELECT count(*) AS total_records FROM `sample-project.sample_dataset.sample_table` WHERE field_status = 'sample_status'"
			rows := []protocol.Row{
				{"other_field": 25},
			}

			metadataStore := mock.NewMetadataStore()
			defer metadataStore.AssertExpectations(t)

			queryExecutor := mock.NewQueryExecutor()
			defer queryExecutor.AssertExpectations(t)

			profileStore := mock.NewProfileStore()
			defer profileStore.AssertExpectations(t)

			metadataStore.On("GetMetadata", profile.URN).Return(tableMeta, nil)

			queryExecutor.On("Run", profile, queryString, job.StatisticalQuery).Return(rows, nil)

			statisticGenerator := NewDefaultProfileStatisticGenerator(metadataStore, queryExecutor, profileStore)

			err := statisticGenerator.Generate(profile)

			assert.Error(t, err)
		})
		t.Run("should return error when update profile failed", func(t *testing.T) {
			someError := errors.New("API error")
			profile := &job.Profile{
				ID:        "1234",
				Filter:    "field_status = 'sample_status'",
				GroupName: "field_grouping",
				URN:       "sample-project.sample_dataset.sample_table",
			}

			tableMeta := &meta.TableSpec{}

			queryString := "SELECT count(*) AS total_records FROM `sample-project.sample_dataset.sample_table` WHERE field_status = 'sample_status'"
			rows := []protocol.Row{
				{"total_records": int64(20)},
			}

			updatedProfile := &job.Profile{
				ID:           "1234",
				Filter:       "field_status = 'sample_status'",
				GroupName:    "field_grouping",
				URN:          "sample-project.sample_dataset.sample_table",
				TotalRecords: 20,
				Message:      "records to be profiled: 20",
			}

			metadataStore := mock.NewMetadataStore()
			defer metadataStore.AssertExpectations(t)

			queryExecutor := mock.NewQueryExecutor()
			defer queryExecutor.AssertExpectations(t)

			profileStore := mock.NewProfileStore()
			defer profileStore.AssertExpectations(t)

			metadataStore.On("GetMetadata", profile.URN).Return(tableMeta, nil)

			queryExecutor.On("Run", profile, queryString, job.StatisticalQuery).Return(rows, nil)

			profileStore.On("Update", updatedProfile).Return(someError)

			statisticGenerator := NewDefaultProfileStatisticGenerator(metadataStore, queryExecutor, profileStore)

			err := statisticGenerator.Generate(profile)

			assert.Error(t, err)
		})
		t.Run("should return error when get metadata failed", func(t *testing.T) {
			someError := errors.New("API error")
			profile := &job.Profile{
				ID:        "1234",
				Filter:    "field_status = 'sample_status'",
				GroupName: "field_grouping",
				URN:       "sample-project.sample_dataset.sample_table",
			}

			tableMeta := &meta.TableSpec{}

			metadataStore := mock.NewMetadataStore()
			defer metadataStore.AssertExpectations(t)

			queryExecutor := mock.NewQueryExecutor()
			defer queryExecutor.AssertExpectations(t)

			profileStore := mock.NewProfileStore()
			defer profileStore.AssertExpectations(t)

			metadataStore.On("GetMetadata", profile.URN).Return(tableMeta, someError)

			statisticGenerator := NewDefaultProfileStatisticGenerator(metadataStore, queryExecutor, profileStore)

			err := statisticGenerator.Generate(profile)

			assert.Error(t, err)
		})
	})
}
