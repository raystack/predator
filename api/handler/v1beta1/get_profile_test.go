package v1beta1

import (
	"encoding/json"
	"github.com/odpf/predator/protocol/metric"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/odpf/predator/api/model"
	"github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol/job"
	"github.com/stretchr/testify/assert"
)

func TestGetProfile(t *testing.T) {
	t.Run("should return profile with metrics", func(t *testing.T) {
		ID := "15d697bc-3aac-11eb-b2c9-0242ac110000"

		auditTime, _ := time.Parse(time.RFC3339, "2020-12-01T00:00:00.000Z")
		updatedTimestamp := time.Now().In(time.UTC)

		profile := &job.Profile{
			ID:               ID,
			URN:              "entity-1-project-1.dataset_a.table_x",
			GroupName:        "created_timestamp",
			Filter:           "date(created_timestamp) = \"2020-12-01\"",
			Mode:             job.ModeComplete,
			Status:           job.StateCompleted,
			Message:          "profile completed",
			TotalRecords:     100,
			AuditTimestamp:   auditTime,
			UpdatedTimestamp: updatedTimestamp,
		}

		metrics := []*metric.Metric{
			{
				ID:         "1",
				Type:       metric.DuplicationPct,
				Category:   metric.Quality,
				Owner:      metric.Table,
				GroupValue: "ID",
				Value:      50,
			},
		}

		mr := []*model.Metric{
			{
				Name:     metric.DuplicationPct,
				Category: metric.Quality,
				Owner:    metric.Table,
				Value:    50,
			},
		}

		mg := []*model.MetricGroup{
			{
				Group:   "ID",
				Metrics: mr,
			},
		}

		response := &model.ProfileResponse{
			ID:           ID,
			URN:          "entity-1-project-1.dataset_a.table_x",
			Filter:       "date(created_timestamp) = \"2020-12-01\"",
			Group:        "created_timestamp",
			Mode:         job.ModeComplete,
			AuditTime:    auditTime,
			Message:      "profile completed",
			State:        job.StateCompleted,
			Metrics:      mg,
			TotalRecords: 100,
			UpdatedAt:    updatedTimestamp,
		}

		metricStore := mock.NewMetricStore()
		defer metricStore.AssertExpectations(t)

		profileService := mock.NewProfileService()
		defer profileService.AssertExpectations(t)

		profileService.On("Get", ID).Return(profile, nil)
		metricStore.On("GetMetricsByProfileID", ID).Return(metrics, nil)

		handler := GetProfile(profileService, metricStore)

		req := httptest.NewRequest(http.MethodGet, "/profile/"+ID, nil)
		res := httptest.NewRecorder()
		req = mux.SetURLVars(req, map[string]string{
			"profileID": ID,
		})
		handler.ServeHTTP(res, req)

		result := &model.ProfileResponse{}
		err := json.NewDecoder(res.Body).Decode(result)

		assert.Nil(t, err)
		assert.Equal(t, http.StatusOK, res.Code)
		assert.Equal(t, response, result)
	})
}
