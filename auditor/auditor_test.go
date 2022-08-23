package auditor

import (
	"errors"
	"fmt"
	"testing"

	"github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/odpf/predator/protocol/metric"
	"github.com/stretchr/testify/assert"
)

func TestAuditor(t *testing.T) {
	t.Run("Audit", func(t *testing.T) {
		tableID := "project-1.dataset_a.table_x"
		auditID := "abcd"
		profileID := "1234"
		toleranceDuplicationPct := &protocol.Tolerance{
			TableURN:   tableID,
			MetricName: "duplication_pct",
			ToleranceRules: []protocol.ToleranceRule{
				{
					Comparator: protocol.ComparatorLessThanEq,
					Value:      0.0,
				},
			},
		}
		metricDuplicationPct := &metric.Metric{
			ID:    "1",
			Type:  metric.DuplicationPct,
			Value: 10.0,
		}

		t.Run("should audit", func(t *testing.T) {
			metrics := []*metric.Metric{{}}
			toleranceSpec := &protocol.ToleranceSpec{
				URN:        tableID,
				Tolerances: []*protocol.Tolerance{toleranceDuplicationPct},
			}
			validatedMetrics := []*protocol.ValidatedMetric{
				{
					Metric:         metricDuplicationPct,
					ToleranceRules: toleranceDuplicationPct.ToleranceRules,
					PassFlag:       false,
				},
			}

			toleranceStore := mock.NewToleranceStore()
			toleranceStore.On("GetByTableID", tableID).Return(toleranceSpec, nil)
			defer toleranceStore.AssertExpectations(t)

			metricStore := mock.NewMetricStore()
			metricStore.On("GetMetricsByProfileID", profileID).Return(metrics, nil)
			defer metricStore.AssertExpectations(t)

			defaultRuleValidator := mockRuleValidator{}
			defaultRuleValidator.On("Validate", metrics, toleranceSpec.Tolerances).Return(validatedMetrics, nil)
			defer defaultRuleValidator.AssertExpectations(t)

			expected := []*protocol.AuditReport{
				{
					AuditID:        auditID,
					TableURN:       tableID,
					MetricName:     metric.DuplicationPct,
					MetricValue:    metricDuplicationPct.Value,
					ToleranceRules: toleranceDuplicationPct.ToleranceRules,
					PassFlag:       false,
				},
			}

			auditor := &Auditor{
				metricStore:    metricStore,
				ruleValidator:  defaultRuleValidator,
				toleranceStore: toleranceStore,
			}
			audit := &job.Audit{
				ID:           auditID,
				ProfileID:    profileID,
				URN:          tableID,
				TotalRecords: 20,
			}
			result, err := auditor.Audit(audit)

			assert.Equal(t, expected, result)
			assert.Nil(t, err)
		})
		t.Run("should failed when no audit result but total records more than 0", func(t *testing.T) {
			metrics := []*metric.Metric{{}}
			toleranceSpec := &protocol.ToleranceSpec{
				URN:        tableID,
				Tolerances: []*protocol.Tolerance{toleranceDuplicationPct},
			}
			var validatedMetrics []*protocol.ValidatedMetric

			toleranceStore := mock.NewToleranceStore()
			toleranceStore.On("GetByTableID", tableID).Return(toleranceSpec, nil)
			defer toleranceStore.AssertExpectations(t)

			metricStore := mock.NewMetricStore()
			metricStore.On("GetMetricsByProfileID", profileID).Return(metrics, nil)
			defer metricStore.AssertExpectations(t)

			defaultRuleValidator := mockRuleValidator{}
			defaultRuleValidator.On("Validate", metrics, toleranceSpec.Tolerances).Return(validatedMetrics, nil)
			defer defaultRuleValidator.AssertExpectations(t)

			auditor := &Auditor{
				metricStore:    metricStore,
				ruleValidator:  defaultRuleValidator,
				toleranceStore: toleranceStore,
			}
			audit := &job.Audit{
				ID:           auditID,
				ProfileID:    profileID,
				URN:          tableID,
				TotalRecords: 20,
			}
			result, err := auditor.Audit(audit)

			assert.Nil(t, result)
			assert.Error(t, err)
		})
		t.Run("should skip audit when no records profiled", func(t *testing.T) {
			toleranceStore := mock.NewToleranceStore()
			defer toleranceStore.AssertExpectations(t)

			audit := &job.Audit{
				ID:        auditID,
				ProfileID: profileID,
				URN:       tableID,
			}
			auditor := &Auditor{
				toleranceStore: toleranceStore,
			}

			actualResult, err := auditor.Audit(audit)

			assert.Nil(t, actualResult)
			assert.Nil(t, err)
		})
		t.Run("should return error when get tolerances failed", func(t *testing.T) {
			toleranceSpec := &protocol.ToleranceSpec{
				URN: tableID,
			}
			apiErr := errors.New("API error")

			toleranceStore := mock.NewToleranceStore()
			toleranceStore.On("GetByTableID", tableID).Return(toleranceSpec, apiErr)
			defer toleranceStore.AssertExpectations(t)

			audit := &job.Audit{
				ID:           auditID,
				ProfileID:    profileID,
				URN:          tableID,
				TotalRecords: 20,
			}
			auditor := &Auditor{
				toleranceStore: toleranceStore,
			}

			expectedErr := fmt.Errorf("failed to try to get tolerances for table %s ,%w", tableID, apiErr)

			actualResult, actualErr := auditor.Audit(audit)

			assert.Nil(t, actualResult)
			assert.Equal(t, expectedErr, actualErr)
		})
		t.Run("should return error when get metrics failed", func(t *testing.T) {
			var metrics []*metric.Metric
			toleranceSpec := &protocol.ToleranceSpec{
				URN:        tableID,
				Tolerances: []*protocol.Tolerance{toleranceDuplicationPct},
			}

			toleranceStore := mock.NewToleranceStore()
			toleranceStore.On("GetByTableID", tableID).Return(toleranceSpec, nil)
			defer toleranceStore.AssertExpectations(t)

			metricStore := mock.NewMetricStore()
			apiErr := errors.New("database error")
			metricStore.On("GetMetricsByProfileID", profileID).Return(metrics, apiErr)
			defer metricStore.AssertExpectations(t)

			expectedErr := fmt.Errorf("failed to get metrics for table %s,%w", tableID, apiErr)

			auditor := &Auditor{
				metricStore:    metricStore,
				toleranceStore: toleranceStore,
			}
			audit := &job.Audit{
				ID:           auditID,
				ProfileID:    profileID,
				URN:          tableID,
				TotalRecords: 20,
			}
			result, err := auditor.Audit(audit)

			assert.Equal(t, expectedErr, err)
			assert.Nil(t, result)
		})
		t.Run("should return error rule validation failed", func(t *testing.T) {
			metrics := []*metric.Metric{{}}
			toleranceSpec := &protocol.ToleranceSpec{
				URN:        tableID,
				Tolerances: []*protocol.Tolerance{toleranceDuplicationPct},
			}
			toleranceStore := mock.NewToleranceStore()
			toleranceStore.On("GetByTableID", tableID).Return(toleranceSpec, nil)
			defer toleranceStore.AssertExpectations(t)

			metricStore := mock.NewMetricStore()
			metricStore.On("GetMetricsByProfileID", profileID).Return(metrics, nil)
			defer metricStore.AssertExpectations(t)

			defaultRuleValidator := mockRuleValidator{}
			validationErr := errors.New("validation error")
			var validatedMetrics []*protocol.ValidatedMetric
			defaultRuleValidator.On("Validate", metrics, toleranceSpec.Tolerances).Return(validatedMetrics, validationErr)
			defer defaultRuleValidator.AssertExpectations(t)

			expectedErr := fmt.Errorf("failed to check score against tolerance rules for table %s,%w", tableID, validationErr)

			auditor := &Auditor{
				metricStore:    metricStore,
				ruleValidator:  defaultRuleValidator,
				toleranceStore: toleranceStore,
			}
			audit := &job.Audit{
				ID:           auditID,
				ProfileID:    profileID,
				URN:          tableID,
				TotalRecords: 20,
			}
			result, err := auditor.Audit(audit)

			assert.Equal(t, expectedErr, err)
			assert.Nil(t, result)
		})
	})
}
