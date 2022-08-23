package message

import (
	"github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/odpf/predator/protocol/metric"
	"github.com/odpf/predator/publisher/proto/odpf/predator/v1beta1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
	"testing"
	"time"
)

var auditID = "audit-abcd"
var tableURN = "table-abcd"

var auditReports = []*protocol.AuditReport{
	{
		AuditID:     auditID,
		GroupValue:  "2019-01-01",
		TableURN:    tableURN,
		MetricName:  metric.DuplicationPct,
		MetricValue: 0.1,
		ToleranceRules: []protocol.ToleranceRule{
			{
				Comparator: protocol.ComparatorLessThanEq,
				Value:      1.0,
			},
		},
		PassFlag: true,
	},
	{
		AuditID:     auditID,
		GroupValue:  "2019-01-01",
		TableURN:    tableURN,
		MetricName:  metric.RowCount,
		MetricValue: 100.0,
		ToleranceRules: []protocol.ToleranceRule{
			{
				Comparator: protocol.ComparatorMoreThanEq,
				Value:      1.0,
			},
		},
		PassFlag: true,
	},
	{
		AuditID:     auditID,
		GroupValue:  "2019-01-01",
		TableURN:    tableURN,
		FieldID:     "sample_field",
		Condition:   "sample_field < 0",
		MetricName:  metric.InvalidPct,
		MetricValue: 0.0,
		ToleranceRules: []protocol.ToleranceRule{
			{
				Comparator: protocol.ComparatorLessThanEq,
				Value:      0.0,
			},
		},
		PassFlag: true,
	},
}

func TestAuditKeyBuilder(t *testing.T) {
	t.Run("Build", func(t *testing.T) {
		t.Run("should return key proto", func(t *testing.T) {
			ts := time.Now().In(time.UTC)

			audit := &job.Audit{
				ID:             auditID,
				ProfileID:      profileID,
				State:          job.StateCreated,
				URN:            tableURN,
				EventTimestamp: ts,
			}

			groupColumn := "created_date"
			prof := &job.Profile{
				GroupName: groupColumn,
			}
			group := &predator.Group{
				Column: groupColumn,
				Value:  "2019-01-01",
			}
			profileStore := mock.NewProfileStore()
			defer profileStore.AssertExpectations(t)
			profileStore.On("Get", profileID).Return(prof, nil)

			protoBuilder := AuditKeyProtoBuilder{
				Audit:        audit,
				AuditResult:  auditReports,
				ProfileStore: profileStore,
			}

			tsProto := timestamppb.New(ts)
			expectedKey := &predator.ResultLogKey{
				Id:             auditID,
				Group:          group,
				EventTimestamp: tsProto,
			}

			keyProto, err := protoBuilder.Build()

			assert.Nil(t, err)
			assert.Equal(t, expectedKey, keyProto)
		})
	})

}

func TestAuditValueBuilder(t *testing.T) {
	t.Run("Build", func(t *testing.T) {
		t.Run("should return value proto", func(t *testing.T) {
			ts := time.Now().In(time.UTC)

			audit := &job.Audit{
				ID:             auditID,
				ProfileID:      profileID,
				State:          job.StateCreated,
				URN:            tableURN,
				EventTimestamp: ts,
			}

			groupColumn := "created_date"
			prof := &job.Profile{
				GroupName: groupColumn,
			}
			group := &predator.Group{
				Column: groupColumn,
				Value:  "2019-01-01",
			}
			profileStore := mock.NewProfileStore()
			defer profileStore.AssertExpectations(t)
			profileStore.On("Get", profileID).Return(prof, nil)

			protoBuilder := AuditValueProtoBuilder{
				Audit:        audit,
				AuditResult:  auditReports,
				ProfileStore: profileStore,
			}

			tsProto := timestamppb.New(ts)
			expectedValue := &predator.ResultLogMessage{
				Id:             auditID,
				ProfileId:      profileID,
				Urn:            tableURN,
				Group:          group,
				EventTimestamp: tsProto,
				Results: []*predator.Result{
					{
						Name:  "duplication_pct",
						Value: 0.1,
						Rules: []*predator.ToleranceRule{
							{
								Name:  "less_than_eq",
								Value: 1.0,
							},
						},
						PassFlag: true,
					},
					{
						Name:  "row_count",
						Value: 100.0,
						Rules: []*predator.ToleranceRule{
							{
								Name:  "more_than_eq",
								Value: 1.0,
							},
						},
						PassFlag: true,
					},
					{
						Name:  metric.InvalidPct.String(),
						Value: 0.0,
						Rules: []*predator.ToleranceRule{
							{
								Name:  "less_than_eq",
								Value: 0.0,
							},
						},
						FieldId:   "sample_field",
						Condition: "sample_field < 0",
						PassFlag:  true,
					},
				},
			}

			valueProto, err := protoBuilder.Build()

			assert.Nil(t, err)
			assert.Equal(t, expectedValue, valueProto)
		})
	})
}
