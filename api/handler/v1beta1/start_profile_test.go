package v1beta1

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/odpf/predator/api/model"
	"github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestStartProfiling(t *testing.T) {
	t.Run("Profile", func(t *testing.T) {
		t.Run("should create profile", func(t *testing.T) {
			ID := "job-1234"

			request := &model.ProfileRequest{
				URN:       "sample-project.sample_dataset.sample_table",
				Group:     "__PARTITION__",
				Filter:    "__PARTITION__ = \"2020-12-01\"",
				Mode:      job.ModeComplete,
				AuditTime: "2020-12-01T00:00:00.000Z",
			}

			auditTime, parseErr := time.Parse(time.RFC3339, request.AuditTime)

			profile := &job.Profile{
				URN:            "sample-project.sample_dataset.sample_table",
				GroupName:      "date(timestamp_field,\"UTC\")",
				Filter:         "date(timestamp_field,\"UTC\") = \"2020-12-01\"",
				Mode:           job.ModeComplete,
				Status:         job.StateCreated,
				Message:        "profile created",
				AuditTimestamp: auditTime,
			}

			createdProfile := &job.Profile{
				ID:             ID,
				URN:            "sample-project.sample_dataset.sample_table",
				GroupName:      "date(timestamp_field,\"UTC\")",
				Filter:         "date(timestamp_field,\"UTC\") = \"2020-12-01\"",
				Mode:           job.ModeComplete,
				Status:         job.StateCreated,
				Message:        "profile created",
				AuditTimestamp: auditTime,
			}

			response := &model.ProfileResponse{
				ID:        ID,
				URN:       "sample-project.sample_dataset.sample_table",
				Filter:    "date(timestamp_field,\"UTC\") = \"2020-12-01\"",
				Group:     "date(timestamp_field,\"UTC\")",
				Mode:      job.ModeComplete,
				AuditTime: auditTime,
			}

			body, _ := json.Marshal(request)

			profileService := mock.NewProfileService()
			profileService.On("CreateProfile", profile).Return(createdProfile, nil)

			sqlExpressionFactory := mock.NewSQLExpressionFactory()
			defer sqlExpressionFactory.AssertExpectations(t)

			sqlExpressionFactory.On("CreatePartitionExpression", profile.URN).Return("date(timestamp_field,\"UTC\")", nil).Twice()

			handler := Profile(profileService, sqlExpressionFactory)

			req := httptest.NewRequest(http.MethodPost, "/profile/", bytes.NewBuffer(body))
			res := httptest.NewRecorder()
			handler.ServeHTTP(res, req)

			result := &model.ProfileResponse{}
			json.NewDecoder(res.Body).Decode(result)

			assert.Nil(t, parseErr)
			assert.Equal(t, http.StatusOK, res.Code)
			assert.Equal(t, response, result)
		})
		t.Run("should return bad request when table is not supported with macros partition expression failed", func(t *testing.T) {
			request := &model.ProfileRequest{
				URN:       "sample-project.sample_dataset.sample_table",
				Group:     "__PARTITION__",
				Filter:    "__PARTITION__ = \"2020-12-01\"",
				Mode:      job.ModeComplete,
				AuditTime: "2020-12-01T00:00:00.000Z",
			}

			body, _ := json.Marshal(request)

			profileService := mock.NewProfileService()

			sqlExpressionFactory := mock.NewSQLExpressionFactory()
			defer sqlExpressionFactory.AssertExpectations(t)

			sqlExpressionFactory.On("CreatePartitionExpression", "sample-project.sample_dataset.sample_table").
				Return("", protocol.ErrPartitionExpressionIsNotSupported).Once()

			handler := Profile(profileService, sqlExpressionFactory)

			req := httptest.NewRequest(http.MethodPost, "/profile/", bytes.NewBuffer(body))
			res := httptest.NewRecorder()
			handler.ServeHTTP(res, req)

			assert.Equal(t, http.StatusBadRequest, res.Code)
		})
		t.Run("should return internal server error when generate partition expression failed", func(t *testing.T) {
			request := &model.ProfileRequest{
				URN:       "sample-project.sample_dataset.sample_table",
				Group:     "__PARTITION__",
				Filter:    "__PARTITION__ = \"2020-12-01\"",
				Mode:      job.ModeComplete,
				AuditTime: "2020-12-01T00:00:00.000Z",
			}

			body, _ := json.Marshal(request)

			profileService := mock.NewProfileService()

			sqlExpressionFactory := mock.NewSQLExpressionFactory()
			defer sqlExpressionFactory.AssertExpectations(t)

			sqlExpressionFactory.On("CreatePartitionExpression", "sample-project.sample_dataset.sample_table").
				Return("", errors.New("API error")).Once()

			handler := Profile(profileService, sqlExpressionFactory)

			req := httptest.NewRequest(http.MethodPost, "/profile/", bytes.NewBuffer(body))
			res := httptest.NewRecorder()
			handler.ServeHTTP(res, req)

			assert.Equal(t, http.StatusInternalServerError, res.Code)
		})
		t.Run("should return bad request when http request body is invalid", func(t *testing.T) {
			body, _ := json.Marshal([]byte("{----------}"))

			profileService := mock.NewProfileService()

			handler := Profile(profileService, nil)

			req := httptest.NewRequest(http.MethodPost, "/profile/", bytes.NewBuffer(body))
			res := httptest.NewRecorder()
			handler.ServeHTTP(res, req)

			result := &model.ProfileResponse{}
			json.NewDecoder(res.Body).Decode(result)

			assert.Equal(t, http.StatusBadRequest, res.Code)
		})
		t.Run("should return request value is invalid", func(t *testing.T) {

			request := &model.ProfileRequest{
				URN:       "sample_urn",
				Group:     "timestamp_field",
				Filter:    "date(timestamp_field) = \"2020-12-01\"",
				Mode:      job.ModeComplete,
				AuditTime: "2020-12-01T00:00:00.000Z",
			}

			body, _ := json.Marshal(request)

			profileService := mock.NewProfileService()

			handler := Profile(profileService, nil)

			req := httptest.NewRequest(http.MethodPost, "/profile/", bytes.NewBuffer(body))
			res := httptest.NewRecorder()
			handler.ServeHTTP(res, req)

			result := &model.ProfileResponse{}
			json.NewDecoder(res.Body).Decode(result)

			assert.Equal(t, http.StatusBadRequest, res.Code)
		})
	})
}
