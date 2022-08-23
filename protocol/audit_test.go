package protocol

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAuditGroup(t *testing.T) {
	t.Run("ByPartitionDate", func(t *testing.T) {
		t.Run("should return map with partition date as key", func(t *testing.T) {
			auditID := "audit-abcd"
			tableURN := "p.d.t"

			rules := []ToleranceRule{
				{
					Comparator: ComparatorLessThanEq,
					Value:      1.0,
				},
			}
			a1 := &AuditReport{
				AuditID:        auditID,
				Partition:      "2019-01-01",
				TableURN:       tableURN,
				MetricName:     "duplication_pct",
				MetricValue:    0.1,
				ToleranceRules: rules,
				PassFlag:       true,
			}

			a2 := &AuditReport{
				AuditID:        auditID,
				Partition:      "2019-01-02",
				TableURN:       tableURN,
				MetricName:     "duplication_pct",
				MetricValue:    0.1,
				ToleranceRules: rules,
				PassFlag:       true,
			}

			auditResults := []*AuditReport{a1, a2}

			result := AuditGroup(auditResults).ByPartitionDate()

			expected := map[string][]*AuditReport{
				"2019-01-01": {a1},
				"2019-01-02": {a2},
			}

			assert.Equal(t, expected, result)
		})
	})
}

func TestIssueSummary(t *testing.T) {
	t.Run("FormIssueSummary", func(t *testing.T) {
		t.Run("should return issue summary", func(t *testing.T) {
			auditID := "audit-abcd"
			eventTimestamp := time.Now().In(time.UTC)

			tolRule := []ToleranceRule{
				{
					Comparator: ComparatorLessThanEq,
					Value:      0.0,
				},
			}

			urn := "project.dataset.table"

			auditRes := []*AuditReport{
				{
					AuditID:        auditID,
					TableURN:       urn,
					FieldID:        "field1",
					Partition:      "2019-01-01",
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
					Partition:      "2019-01-02",
					MetricName:     "nullness_pct",
					MetricValue:    0.05,
					ToleranceRules: tolRule,
					PassFlag:       false,
					EventTimestamp: eventTimestamp,
				},
			}

			issueSum := FormIssueSummary(auditRes)
			expected := "NULLNESS_PCT OF FIELD2 IS NOT PASSED THE TOLERANCE IN PARTITION 2019-01-02\nTolerance: LESS_THAN_EQ 0.00\nACTUAL VALUE: 0.050"

			assert.Equal(t, expected, issueSum)
		})

		t.Run("should return invalidity metric issue summary", func(t *testing.T) {
			auditID := "audit-abcd"
			eventTimestamp := time.Now().In(time.UTC)

			tolRule := []ToleranceRule{
				{
					Comparator: ComparatorLessThanEq,
					Value:      0.0,
				},
			}

			urn := "project.dataset.table"

			auditRes := []*AuditReport{
				{
					AuditID:        auditID,
					TableURN:       urn,
					FieldID:        "field1",
					Partition:      "2019-01-01",
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
					Partition:      "2019-01-02",
					MetricName:     "invalid_pct",
					MetricValue:    0.05,
					ToleranceRules: tolRule,
					Condition:      "field2 <= 0",
					PassFlag:       false,
					EventTimestamp: eventTimestamp,
				},
			}

			issueSum := FormIssueSummary(auditRes)
			expected := "INVALID_PCT OF FIELD2 IS NOT PASSED THE TOLERANCE IN PARTITION 2019-01-02\nCONDITION: FIELD2 <= 0\nTolerance: LESS_THAN_EQ 0.00\nACTUAL VALUE: 0.050"

			assert.Equal(t, expected, issueSum)
		})
	})
}
