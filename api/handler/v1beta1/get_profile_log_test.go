package v1beta1

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/odpf/predator/api/model"
	"github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/stretchr/testify/assert"
)

func TestGetProfileLog(t *testing.T) {
	t.Run("should return profile logs", func(t *testing.T) {
		ID := "15d697bc-3aac-11eb-b2c9-0242ac110000"
		auditTime, _ := time.Parse(time.RFC3339, "2020-12-01T00:00:00.000Z")
		timestampFirst := time.Now().In(time.UTC)
		timestampSecond := timestampFirst.Add(10 * time.Second)
		profile := &job.Profile{
			ID:               ID,
			URN:              "entity-1-project-1.dataset_a.table_x",
			GroupName:        "created_timestamp",
			Filter:           "date(created_timestamp) = \"2020-12-01\"",
			Mode:             job.ModeComplete,
			Status:           job.StateInProgress,
			TotalRecords:     100,
			AuditTimestamp:   auditTime,
			UpdatedTimestamp: timestampSecond,
		}
		statusFirst := &protocol.Status{
			JobID:          ID,
			JobType:        job.TypeProfile,
			Status:         string(job.StateInProgress),
			Message:        "gathering metadata 1",
			EventTimestamp: timestampFirst,
		}
		statusSecond := &protocol.Status{
			JobID:          ID,
			JobType:        job.TypeProfile,
			Status:         string(job.StateInProgress),
			Message:        "gathering metadata completed",
			EventTimestamp: timestampSecond,
		}
		statusList := []*protocol.Status{
			statusSecond,
			statusFirst,
		}
		logs := []model.Log{
			{
				Status:         statusSecond.Status,
				Message:        statusSecond.Message,
				EventTimestamp: statusSecond.EventTimestamp,
			},
			{
				Status:         statusFirst.Status,
				Message:        statusFirst.Message,
				EventTimestamp: statusFirst.EventTimestamp,
			},
		}

		profileService := mock.NewProfileService()
		defer profileService.AssertExpectations(t)
		profileService.On("Get", ID).Return(profile, nil)
		profileService.On("GetLog", ID).Return(statusList, nil)

		handler := GetProfileLog(profileService)
		req := httptest.NewRequest(http.MethodGet, "/profile/"+ID+"/log", nil)
		res := httptest.NewRecorder()
		req = mux.SetURLVars(req, map[string]string{
			"profileID": ID,
		})
		handler.ServeHTTP(res, req)

		response := &model.ProfileLogResponse{
			ID:           ID,
			URN:          profile.URN,
			Filter:       profile.Filter,
			Group:        profile.GroupName,
			Mode:         profile.Mode,
			AuditTime:    profile.AuditTimestamp,
			State:        profile.Status,
			TotalRecords: profile.TotalRecords,
			UpdatedAt:    profile.UpdatedTimestamp,
			Logs:         logs,
		}

		result := &model.ProfileLogResponse{}
		err := json.NewDecoder(res.Body).Decode(result)

		assert.Nil(t, err)
		assert.Equal(t, http.StatusOK, res.Code)
		assert.Equal(t, response, result)
	})
}
