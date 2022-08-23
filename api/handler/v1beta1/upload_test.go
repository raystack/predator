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
)

func TestUpload(t *testing.T) {
	t.Run("Upload", func(t *testing.T) {
		t.Run("should run upload task", func(t *testing.T) {
			uploadRequest := &model.UploadRequest{
				GitURL:     "git@sample-url:entity-1.git",
				CommitID:   "123abcd",
				PathPrefix: "predator",
			}

			gitRepo := &protocol.GitInfo{
				URL:        "git@sample-url:entity-1.git",
				CommitID:   "123abcd",
				PathPrefix: "predator",
			}

			requestBody, _ := json.Marshal(uploadRequest)

			req := httptest.NewRequest("POST", "/upload", bytes.NewBuffer(requestBody))
			res := httptest.NewRecorder()

			uploadTask := mock.NewMockUpload()
			defer uploadTask.AssertExpectations(t)

			uploadFactory := mock.NewMockUploadFactory()
			defer uploadFactory.AssertExpectations(t)

			uploadFactory.On("Create", gitRepo).Return(uploadTask, nil)
			report := &job.Diff{
				Add: []string{"a.b.c"},
			}
			uploadTask.On("Run").Return(report, nil)

			handler := Upload(uploadFactory)
			handler.ServeHTTP(res, req)

			assert.Equal(t, http.StatusOK, res.Code)
		})
		t.Run("should return bad request when request body invalid", func(t *testing.T) {
			uploadRequest := &model.UploadRequest{}

			requestBody, _ := json.Marshal(uploadRequest)
			req := httptest.NewRequest("POST", "/upload", bytes.NewBuffer(requestBody))
			res := httptest.NewRecorder()

			uploadTask := mock.NewMockUpload()
			defer uploadTask.AssertExpectations(t)

			uploadFactory := mock.NewMockUploadFactory()
			defer uploadFactory.AssertExpectations(t)

			handler := Upload(uploadFactory)
			handler.ServeHTTP(res, req)

			assert.Equal(t, http.StatusBadRequest, res.Code)
		})
		t.Run("should return internal server error when upload task failed", func(t *testing.T) {

			uploadRequest := &model.UploadRequest{
				GitURL:   "git@sample-url:entity-1.git",
				CommitID: "123abcd",
			}

			gitRepo := &protocol.GitInfo{
				URL:      "git@sample-url:entity-1.git",
				CommitID: "123abcd",
			}

			gitError := errors.New("git error")

			requestBody, _ := json.Marshal(uploadRequest)

			req := httptest.NewRequest("POST", "/upload", bytes.NewBuffer(requestBody))
			res := httptest.NewRecorder()

			uploadTask := mock.NewMockUpload()
			defer uploadTask.AssertExpectations(t)

			uploadFactory := mock.NewMockUploadFactory()
			defer uploadFactory.AssertExpectations(t)

			uploadFactory.On("Create", gitRepo).Return(uploadTask, nil)
			report := &job.Diff{
				Add: []string{"a.b.c"},
			}
			uploadTask.On("Run").Return(report, gitError)

			handler := Upload(uploadFactory)
			handler.ServeHTTP(res, req)

			assert.Equal(t, http.StatusInternalServerError, res.Code)
		})
		t.Run("should return bad request and spec validation error when spec is invalid", func(t *testing.T) {

			uploadRequest := &model.UploadRequest{
				GitURL:   "git@sample-url:entity-1.git",
				CommitID: "123abcd",
			}

			gitRepo := &protocol.GitInfo{
				URL:      "git@sample-url:entity-1.git",
				CommitID: "123abcd",
			}

			gitError := &protocol.ErrUploadSpecValidation{}

			requestBody, _ := json.Marshal(uploadRequest)

			req := httptest.NewRequest("POST", "/upload", bytes.NewBuffer(requestBody))
			res := httptest.NewRecorder()

			uploadTask := mock.NewMockUpload()
			defer uploadTask.AssertExpectations(t)

			uploadFactory := mock.NewMockUploadFactory()
			defer uploadFactory.AssertExpectations(t)

			uploadFactory.On("Create", gitRepo).Return(uploadTask, nil)
			var report *job.Diff
			uploadTask.On("Run").Return(report, gitError)

			handler := Upload(uploadFactory)
			handler.ServeHTTP(res, req)

			assert.Equal(t, http.StatusBadRequest, res.Code)
		})
	})
}
