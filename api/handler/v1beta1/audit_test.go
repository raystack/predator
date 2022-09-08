package v1beta1

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/odpf/predator/api/model"
	"github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/odpf/predator/protocol/metric"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
)

func TestAudit(t *testing.T) {
	t.Run("Audit", func(t *testing.T) {
		t.Run("should start audit task", func(t *testing.T) {
			auditID := "audit-1234"
			profileID := "profile-abcd"
			currentTime := time.Now().In(time.UTC)
			tableURN := "project.dataset.table"
			groupName := "group1"
			sampleFieldNullnessPctReportFieldID := "sample_filed"
			sampleFieldNullnessPctReportGroup := "2019-01-01"
			sampleFieldNullnessPctReportMetricName := "nullness_pct"
			sampleFieldNullnessPctReportMetricValue := 0.0
			tolRule := []protocol.ToleranceRule{
				{
					Comparator: protocol.ComparatorLessThanEq,
					Value:      0.0,
				},
			}
			audit := &job.Audit{
				ID:             auditID,
				ProfileID:      profileID,
				URN:            tableURN,
				State:          job.StateCreated,
				EventTimestamp: currentTime,
				TotalRecords:   20,
			}
			profile := &job.Profile{
				ID:           profileID,
				GroupName:    groupName,
				TotalRecords: 20,
			}
			auditReports := []*protocol.AuditReport{
				{
					AuditID:        auditID,
					GroupValue:     sampleFieldNullnessPctReportGroup,
					MetricName:     metric.Type(sampleFieldNullnessPctReportMetricName),
					MetricValue:    sampleFieldNullnessPctReportMetricValue,
					FieldID:        sampleFieldNullnessPctReportFieldID,
					PassFlag:       true,
					ToleranceRules: tolRule,
				},
			}
			auditResult := &protocol.AuditResult{
				Audit:        audit,
				AuditReports: auditReports,
			}
			auditResultGroup := []model.AuditResultGroup{
				{
					GroupValue: sampleFieldNullnessPctReportGroup,
					Pass:       true,
					AuditResults: []model.AuditResult{
						{
							FieldID:        sampleFieldNullnessPctReportFieldID,
							MetricName:     sampleFieldNullnessPctReportMetricName,
							MetricValue:    sampleFieldNullnessPctReportMetricValue,
							Pass:           true,
							ToleranceRules: tolRule,
						},
					},
				},
			}
			auditSummary := &protocol.AuditSummary{
				IsPass:  true,
				Message: "ALL METRICS PASSED THE TOLERANCE",
			}

			auditSummaryFactory := mock.NewAuditSummaryFactory()
			defer auditSummaryFactory.AssertExpectations(t)
			auditSummaryFactory.On("Create", auditReports, audit).Return(auditSummary, nil)

			auditService := mock.NewAuditService()
			auditService.On("RunAudit", profileID).Return(auditResult, nil)
			defer auditService.AssertExpectations(t)

			profileService := mock.NewProfileService()
			profileService.On("Get", profileID).Return(profile, nil)
			defer auditService.AssertExpectations(t)

			expectedResponse := &model.AuditResponse{
				AuditID:      auditID,
				ProfileID:    profileID,
				URN:          tableURN,
				GroupName:    groupName,
				Status:       string(audit.State),
				Pass:         true,
				Message:      "ALL METRICS PASSED THE TOLERANCE",
				Result:       auditResultGroup,
				TotalRecords: 20,
				CreatedAt:    currentTime,
			}

			var result model.AuditResponse
			res := httptest.NewRecorder()
			req := httptest.NewRequest("POST", "/profile/"+profileID+"/audit", nil)
			req = mux.SetURLVars(req, map[string]string{
				"profileID": profileID,
			})
			handler := Audit(auditService, profileService, auditSummaryFactory)
			handler.ServeHTTP(res, req)
			err := json.NewDecoder(res.Body).Decode(&result)
			assert.Nil(t, err)

			assert.Equal(t, expectedResponse, &result)
			assert.Equal(t, http.StatusOK, res.Code)
		})

		t.Run("should return bad request when profileID is invalid", func(t *testing.T) {
			profileID := ""

			auditService := mock.NewAuditService()
			defer auditService.AssertExpectations(t)

			profileService := mock.NewProfileService()
			defer auditService.AssertExpectations(t)

			toleranceStore := mock.NewToleranceStore()
			defer toleranceStore.AssertExpectations(t)

			auditSummaryFactory := mock.NewAuditSummaryFactory()
			defer auditSummaryFactory.AssertExpectations(t)

			req := httptest.NewRequest("POST", "/profile/"+profileID+"/audit", nil)
			req = mux.SetURLVars(req, map[string]string{
				"profileID": profileID,
			})
			res := httptest.NewRecorder()
			handler := Audit(auditService, profileService, auditSummaryFactory)
			handler.ServeHTTP(res, req)

			assert.Equal(t, http.StatusBadRequest, res.Code)
		})

		t.Run("should return bad request when profileID not found", func(t *testing.T) {
			profileID := "profile-abcd"

			auditService := mock.NewAuditService()
			auditService.On("RunAudit", profileID).Return(&protocol.AuditResult{}, protocol.ErrProfileNotFound)
			defer auditService.AssertExpectations(t)

			profileService := mock.NewProfileService()
			defer auditService.AssertExpectations(t)

			toleranceStore := mock.NewToleranceStore()
			defer toleranceStore.AssertExpectations(t)

			auditSummaryFactory := mock.NewAuditSummaryFactory()
			defer auditSummaryFactory.AssertExpectations(t)

			req := httptest.NewRequest("POST", "/profile/"+profileID+"/audit", nil)
			req = mux.SetURLVars(req, map[string]string{
				"profileID": profileID,
			})
			res := httptest.NewRecorder()
			handler := Audit(auditService, profileService, auditSummaryFactory)
			handler.ServeHTTP(res, req)

			assert.Equal(t, http.StatusBadRequest, res.Code)
		})

		t.Run("should return internal server error run audit failed", func(t *testing.T) {
			profileID := "profile-abcd"

			auditService := mock.NewAuditService()
			auditService.On("RunAudit", profileID).Return(&protocol.AuditResult{}, errors.New("another error happened"))
			defer auditService.AssertExpectations(t)

			profileService := mock.NewProfileService()
			defer auditService.AssertExpectations(t)

			toleranceStore := mock.NewToleranceStore()
			defer toleranceStore.AssertExpectations(t)

			auditSummaryFactory := mock.NewAuditSummaryFactory()
			defer auditSummaryFactory.AssertExpectations(t)

			req := httptest.NewRequest("POST", "/profile/"+profileID+"/audit", nil)
			req = mux.SetURLVars(req, map[string]string{
				"profileID": profileID,
			})
			res := httptest.NewRecorder()
			handler := Audit(auditService, profileService, auditSummaryFactory)
			handler.ServeHTTP(res, req)

			assert.Equal(t, http.StatusInternalServerError, res.Code)
		})
	})
}
