package message

import (
	"github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol/job"
	"github.com/odpf/predator/protocol/meta"
	"github.com/odpf/predator/protocol/metric"
	"github.com/odpf/predator/publisher/proto/odpf/predator/v1beta1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
	"testing"
	"time"
)

var profileID = "profile-id"
var groupValue = "2019-10-10"
var urn = "project.dataset.table"
var filter = "date(field_timestamp) == \"2019-01-01\""
var mode = job.ModeComplete

var metrics = []*metric.Metric{
	{
		Type:       metric.RowCount,
		Category:   metric.Basic,
		Owner:      metric.Table,
		GroupValue: groupValue,
		Value:      30,
	},
	{
		Type:       metric.UniqueCount,
		Category:   metric.Basic,
		Owner:      metric.Table,
		GroupValue: groupValue,
		Value:      20,
	},
	{
		Type:       metric.InvalidCount,
		Category:   metric.Basic,
		Owner:      metric.Table,
		GroupValue: groupValue,
		Condition:  "2 = 2",
		Value:      10,
	},
	{
		Type:       metric.DuplicationPct,
		Category:   metric.Quality,
		Owner:      metric.Table,
		GroupValue: groupValue,
		Value:      20,
	},
	{
		Type:       metric.InvalidPct,
		Category:   metric.Quality,
		Owner:      metric.Table,
		GroupValue: groupValue,
		Condition:  "2 = 2",
		Value:      30,
	},
	{
		FieldID:    "field1",
		Type:       metric.Count,
		Category:   metric.Basic,
		Owner:      metric.Field,
		GroupValue: groupValue,
		Value:      30,
	},
	{
		FieldID:    "field1",
		Type:       metric.NullCount,
		Category:   metric.Basic,
		Owner:      metric.Field,
		GroupValue: groupValue,
		Value:      20,
	},
	{
		FieldID:    "field1",
		Type:       metric.Sum,
		Category:   metric.Basic,
		Owner:      metric.Field,
		GroupValue: groupValue,
		Value:      50,
	},
	{
		FieldID:    "field1",
		Type:       metric.InvalidCount,
		Category:   metric.Basic,
		Owner:      metric.Field,
		GroupValue: groupValue,
		Value:      50,
		Condition:  "1 = 1",
	},
	{
		FieldID:    "field1",
		Type:       metric.NullnessPct,
		Category:   metric.Quality,
		Owner:      metric.Field,
		GroupValue: groupValue,
		Value:      60,
	},
	{
		FieldID:    "field1",
		Type:       metric.InvalidPct,
		Category:   metric.Quality,
		Owner:      metric.Field,
		GroupValue: groupValue,
		Value:      50,
		Condition:  "1 = 1",
	},
	{
		FieldID:    "field2",
		Type:       metric.Count,
		Category:   metric.Basic,
		Owner:      metric.Field,
		GroupValue: groupValue,
		Value:      150,
	},
	{
		FieldID:    "field2",
		Type:       metric.NullCount,
		Category:   metric.Basic,
		Owner:      metric.Field,
		GroupValue: groupValue,
		Value:      320,
	},
	{
		FieldID:    "field2",
		Type:       metric.Sum,
		Category:   metric.Basic,
		Owner:      metric.Field,
		GroupValue: groupValue,
		Value:      450,
	},
}
var tableSpec = &meta.TableSpec{
	Fields: []*meta.FieldSpec{
		{
			Name:      "field1",
			FieldType: meta.FieldTypeInteger,
		},
		{
			Name:      "field2",
			FieldType: meta.FieldTypeInteger,
		},
	},
}

func TestProfileKeyProtoBuilder(t *testing.T) {
	t.Run("Build", func(t *testing.T) {
		t.Run("should return proto key", func(t *testing.T) {
			now := time.Now()
			nowProto := timestamppb.New(now)

			profileJob := &job.Profile{
				ID:             profileID,
				EventTimestamp: now,
				URN:            urn,
				GroupName:      "field_date",
			}

			builder := ProfileKeyProtoBuilder{
				Metrics: metrics,
				Profile: profileJob,
			}

			expectedGroup := &predator.Group{
				Column: profileJob.GroupName,
				Value:  groupValue,
			}

			expectedKey := &predator.MetricsLogKey{
				Id:             profileID,
				Group:          expectedGroup,
				EventTimestamp: nowProto,
			}

			keyProto, err := builder.Build()

			assert.Nil(t, err)
			assert.Equal(t, expectedKey, keyProto)
		})
	})
}

func TestProfileValueProtoBuilder(t *testing.T) {
	t.Run("Get", func(t *testing.T) {
		t.Run("should return Message", func(t *testing.T) {
			now := time.Now()
			nowProto := timestamppb.New(now)

			profileJob := &job.Profile{
				ID:             profileID,
				EventTimestamp: now,
				URN:            urn,
				GroupName:      "field_date",
				Filter:         filter,
				Mode:           mode,
			}

			metaStore := mock.NewMetadataStore()
			defer metaStore.AssertExpectations(t)
			metaStore.On("GetMetadata", urn).Return(tableSpec, nil).Once()

			builder := ProfileValueProtoBuilder{
				Metrics:       metrics,
				Profile:       profileJob,
				MetadataStore: metaStore,
			}

			expectedGroup := &predator.Group{
				Column: profileJob.GroupName,
				Value:  groupValue,
			}
			expectedMessage := &predator.MetricsLogMessage{
				Id:     profileID,
				Urn:    urn,
				Group:  expectedGroup,
				Filter: filter,
				Mode:   mode.String(),
				TableMetrics: []*predator.Metric{
					{
						Name:  metric.RowCount.String(),
						Value: 30,
					},
					{
						Name:  metric.UniqueCount.String(),
						Value: 20,
					},
					{
						Name:      metric.InvalidCount.String(),
						Value:     10,
						Condition: "2 = 2",
					},
					{
						Name:  metric.DuplicationPct.String(),
						Value: 20,
					},
					{
						Name:      metric.InvalidPct.String(),
						Value:     30,
						Condition: "2 = 2",
					},
				},
				EventTimestamp: nowProto,
				ColumnMetrics: []*predator.ColumnMetric{
					{
						Id:   "field1",
						Type: meta.FieldTypeInteger.String(),
						Metrics: []*predator.Metric{
							{
								Name:  metric.Count.String(),
								Value: 30,
							},
							{
								Name:  metric.NullCount.String(),
								Value: 20,
							},
							{
								Name:  metric.Sum.String(),
								Value: 50,
							},
							{
								Name:      metric.InvalidCount.String(),
								Value:     50,
								Condition: "1 = 1",
							},

							{
								Name:  metric.NullnessPct.String(),
								Value: 60,
							},
							{
								Name:      metric.InvalidPct.String(),
								Value:     50,
								Condition: "1 = 1",
							},
						},
					},
					{
						Id:   "field2",
						Type: meta.FieldTypeInteger.String(),
						Metrics: []*predator.Metric{
							{
								Name:  metric.Count.String(),
								Value: 150,
							},
							{
								Name:  metric.NullCount.String(),
								Value: 320,
							},
							{
								Name:  metric.Sum.String(),
								Value: 450,
							},
						},
					},
				},
			}

			valueProto, err := builder.Build()

			assert.Nil(t, err)
			assert.Equal(t, expectedMessage, valueProto)
		})
	})
}
