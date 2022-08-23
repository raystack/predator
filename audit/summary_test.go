package audit

import (
	"github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/odpf/predator/protocol/metric"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestIssueSummary(t *testing.T) {
	t.Run("FormIssueSummary", func(t *testing.T) {
		t.Run("should return issue summary with group", func(t *testing.T) {
			auditID := "audit-abcd"
			eventTimestamp := time.Now().In(time.UTC)

			tolRule := []protocol.ToleranceRule{
				{
					Comparator: protocol.ComparatorLessThanEq,
					Value:      0.0,
				},
			}

			urn := "project.dataset.table"

			auditRes := []*protocol.AuditReport{
				{
					AuditID:        auditID,
					TableURN:       urn,
					FieldID:        "field1",
					GroupValue:     "2019-01-01",
					MetricName:     "count",
					MetricValue:    3000.0,
					ToleranceRules: tolRule,
					PassFlag:       true,
					EventTimestamp: eventTimestamp,
				},
				{
					AuditID:        auditID,
					TableURN:       urn,
					FieldID:        "field2",
					GroupValue:     "2019-01-02",
					MetricName:     "nullness_pct",
					MetricValue:    0.05,
					ToleranceRules: tolRule,
					PassFlag:       false,
					EventTimestamp: eventTimestamp,
				},
			}

			issueSum := FormIssueSummary(auditRes)
			expected := "NULLNESS_PCT OF FIELD2 IS NOT PASSED THE TOLERANCE IN GROUP 2019-01-02\nTolerance: LESS_THAN_EQ 0.00\nACTUAL VALUE: 0.050"

			assert.Equal(t, expected, issueSum)
		})
		t.Run("should return issue summary without group", func(t *testing.T) {
			auditID := "audit-abcd"
			eventTimestamp := time.Now().In(time.UTC)

			tolRule := []protocol.ToleranceRule{
				{
					Comparator: protocol.ComparatorLessThanEq,
					Value:      0.0,
				},
			}

			urn := "project.dataset.table"

			auditRes := []*protocol.AuditReport{
				{
					AuditID:        auditID,
					TableURN:       urn,
					FieldID:        "field1",
					MetricName:     "count",
					MetricValue:    3000.0,
					ToleranceRules: tolRule,
					PassFlag:       true,
					EventTimestamp: eventTimestamp,
				},
				{
					AuditID:        auditID,
					TableURN:       urn,
					FieldID:        "field2",
					MetricName:     "nullness_pct",
					MetricValue:    0.05,
					ToleranceRules: tolRule,
					PassFlag:       false,
					EventTimestamp: eventTimestamp,
				},
			}

			issueSum := FormIssueSummary(auditRes)
			expected := "NULLNESS_PCT OF FIELD2 IS NOT PASSED THE TOLERANCE \nTolerance: LESS_THAN_EQ 0.00\nACTUAL VALUE: 0.050"

			assert.Equal(t, expected, issueSum)
		})
		t.Run("should return invalidity metric issue summary", func(t *testing.T) {
			auditID := "audit-abcd"
			eventTimestamp := time.Now().In(time.UTC)

			tolRule := []protocol.ToleranceRule{
				{
					Comparator: protocol.ComparatorLessThanEq,
					Value:      0.0,
				},
			}

			urn := "project.dataset.table"

			auditRes := []*protocol.AuditReport{
				{
					AuditID:        auditID,
					TableURN:       urn,
					FieldID:        "field1",
					GroupValue:     "2019-01-01",
					MetricName:     "count",
					MetricValue:    3000.0,
					ToleranceRules: tolRule,
					PassFlag:       true,
					EventTimestamp: eventTimestamp,
				},
				{
					AuditID:        auditID,
					TableURN:       urn,
					FieldID:        "field3",
					GroupValue:     "2019-01-02",
					MetricName:     "invalid_pct",
					MetricValue:    0.05,
					ToleranceRules: tolRule,
					Condition:      "field3 <= 0",
					PassFlag:       false,
					EventTimestamp: eventTimestamp,
				},
			}

			issueSum := FormIssueSummary(auditRes)
			expected := "INVALID_PCT OF FIELD3 IS NOT PASSED THE TOLERANCE IN GROUP 2019-01-02\nCONDITION: FIELD3 <= 0\nTolerance: LESS_THAN_EQ 0.00\nACTUAL VALUE: 0.050"

			assert.Equal(t, expected, issueSum)
		})
	})
	t.Run("Create", func(t *testing.T) {
		t.Run("should return not pass when total records more than zero but audit result not found", func(t *testing.T) {
			tableID := "project.dataset.table"

			auditJob := &job.Audit{
				URN:          tableID,
				TotalRecords: 20,
			}
			toleranceStore := mock.NewToleranceStore()
			defer toleranceStore.AssertExpectations(t)

			summaryFactory := NewAuditSummaryFactory(toleranceStore)
			expected := &protocol.AuditSummary{
				IsPass:  false,
				Message: "EXPECT SOME AUDIT RESULT BUT NO AUDIT RESULT FOUND",
			}

			summary, err := summaryFactory.Create(nil, auditJob)

			assert.Nil(t, err)
			assert.Equal(t, expected, summary)
		})
	})
	t.Run("formNoRecordsSummary", func(t *testing.T) {
		tableID := "project.dataset.table"
		auditJob := job.Audit{
			URN: tableID,
		}
		t.Run("should return not pass when record availability requirement is true", func(t *testing.T) {
			toleranceStore := mock.NewToleranceStore()
			defer toleranceStore.AssertExpectations(t)

			tolerances := &protocol.ToleranceSpec{
				URN: tableID,
				Tolerances: []*protocol.Tolerance{
					{
						MetricName: metric.RowCount,
						ToleranceRules: []protocol.ToleranceRule{
							{
								Comparator: protocol.ComparatorMoreThan,
								Value:      0,
							},
						},
					},
				},
			}

			toleranceStore.On("GetByTableID", tableID).Return(tolerances, nil)

			summaryFactory := NewAuditSummaryFactory(toleranceStore)
			expected := &protocol.AuditSummary{
				IsPass:  false,
				Message: "EXPECT SOME RECORDS BUT NO RECORDS FOUND",
			}

			summary, err := summaryFactory.formNoRecordsSummary(&auditJob)

			assert.Nil(t, err)
			assert.Equal(t, expected, summary)
		})
		t.Run("should return pass when record availability requirement is false", func(t *testing.T) {
			toleranceStore := mock.NewToleranceStore()
			defer toleranceStore.AssertExpectations(t)

			tolerances := &protocol.ToleranceSpec{
				URN: tableID,
				Tolerances: []*protocol.Tolerance{
					{
						MetricName: metric.DuplicationPct,
						ToleranceRules: []protocol.ToleranceRule{
							{
								Comparator: protocol.ComparatorLessThan,
								Value:      0,
							},
						},
					},
				},
			}

			toleranceStore.On("GetByTableID", tableID).Return(tolerances, nil)

			summaryFactory := NewAuditSummaryFactory(toleranceStore)
			expected := &protocol.AuditSummary{
				IsPass:  true,
				Message: "NO RECORDS PROFILED AND AUDITED",
			}

			summary, err := summaryFactory.formNoRecordsSummary(&auditJob)

			assert.Nil(t, err)
			assert.Equal(t, expected, summary)
		})
	})
}
