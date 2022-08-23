package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/odpf/predator/api/model"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/stretchr/testify/assert"
)

func TestPredator(t *testing.T) {
	t.Run("Upload", func(t *testing.T) {
		t.Run("should upload", func(t *testing.T) {
			baseURL := "http://localhost:8080"
			resourceURL := "http://localhost:8080/v1beta1/spec/upload"

			gitInfo := &protocol.GitInfo{
				URL:        "git@sample-url/entity-1-project-1.git",
				CommitID:   "123abcd",
				PathPrefix: "predator",
			}

			reqContent := []byte(`{"git_url":"git@sample-url/entity-1-project-1.git","commit_id":"123abcd","path_prefix":"predator"}`)

			expected := &model.UploadReport{
				UploadedCount: 10,
				RemovedCount:  1,
			}

			respContent, _ := json.Marshal(expected)
			respBody := ioutil.NopCloser(bytes.NewBuffer(respContent))
			resp := &http.Response{
				StatusCode: 200,
				Body:       respBody,
			}

			mockClient := mock.NewHttpClient()
			mockClient.On("Post", resourceURL, contentType, bytes.NewBuffer(reqContent)).Return(resp, nil)

			client := New(baseURL, mockClient)
			uploadReport, err := client.Upload(gitInfo)

			assert.Nil(t, err)
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Equal(t, expected, uploadReport)
		})
		t.Run("should return error when http req failed", func(t *testing.T) {
			baseURL := "http://localhost:8080"
			resourceURL := "http://localhost:8080/v1beta1/spec/upload"

			gitInfo := &protocol.GitInfo{
				URL:        "git@sample-url/entity-1-project-1.git",
				CommitID:   "123abcd",
				PathPrefix: "predator",
			}

			reqContent := []byte(`{"git_url":"git@sample-url/entity-1-project-1.git","commit_id":"123abcd","path_prefix":"predator"}`)

			resp := &http.Response{}

			networkErr := errors.New("no connection error")

			mockClient := mock.NewHttpClient()
			mockClient.On("Post", resourceURL, contentType, bytes.NewBuffer(reqContent)).Return(resp, networkErr)

			client := New(baseURL, mockClient)
			uploadReport, err := client.Upload(gitInfo)

			assert.Nil(t, uploadReport)
			assert.Error(t, err)
		})
		t.Run("should return error when parse result failed", func(t *testing.T) {
			baseURL := "http://localhost:8080"
			resourceURL := "http://localhost:8080/v1beta1/spec/upload"

			gitInfo := &protocol.GitInfo{
				URL:        "git@sample-url/entity-1-project-1.git",
				CommitID:   "123abcd",
				PathPrefix: "predator",
			}

			reqContent := []byte(`{"git_url":"git@sample-url/entity-1-project-1.git","commit_id":"123abcd","path_prefix":"predator"}`)

			respBody := ioutil.NopCloser(bytes.NewBufferString("\n\n\n"))
			resp := &http.Response{
				StatusCode: 200,
				Body:       respBody,
			}

			mockClient := mock.NewHttpClient()
			mockClient.On("Post", resourceURL, contentType, bytes.NewBuffer(reqContent)).Return(resp, nil)

			client := New(baseURL, mockClient)
			uploadReport, err := client.Upload(gitInfo)

			assert.Nil(t, uploadReport)
			assert.Error(t, err)
		})
		t.Run("should return error when http status code is not 200", func(t *testing.T) {
			baseURL := "http://localhost:8080"
			resourceURL := "http://localhost:8080/v1beta1/spec/upload"

			gitInfo := &protocol.GitInfo{
				URL:        "git@sample-url/entity-1-project-1.git",
				CommitID:   "123abcd",
				PathPrefix: "predator",
			}

			reqContent := []byte(`{"git_url":"git@sample-url/entity-1-project-1.git","commit_id":"123abcd","path_prefix":"predator"}`)

			respBody := ioutil.NopCloser(bytes.NewBufferString("error from server"))
			resp := &http.Response{
				StatusCode: 500,
				Body:       respBody,
			}

			mockClient := mock.NewHttpClient()
			mockClient.On("Post", resourceURL, contentType, bytes.NewBuffer(reqContent)).Return(resp, nil)

			client := New(baseURL, mockClient)
			uploadReport, err := client.Upload(gitInfo)

			assert.Nil(t, uploadReport)
			assert.Error(t, err)
		})
	})

	t.Run("Profile", func(t *testing.T) {
		baseURL := "http://localhost:8080"
		resourceURL := "http://localhost:8080/v1beta1/profile"
		ID := "job-1234"
		urn := "entity-1-project-1.dataset_a.table_x"
		group := "timestamp_field"
		filter := "date(timestamp_field) = \"2020-12-01\""

		request := &model.ProfileRequest{
			URN:       urn,
			Group:     group,
			Filter:    filter,
			Mode:      job.ModeComplete,
			AuditTime: "2020-12-01T00:00:00.000Z",
		}

		t.Run("should profile", func(t *testing.T) {
			reqContent, err := json.Marshal(request)
			assert.Nil(t, err)

			auditTime, parseErr := time.Parse(time.RFC3339, request.AuditTime)
			expectedResponse := &model.ProfileResponse{
				ID:        ID,
				URN:       urn,
				Filter:    filter,
				Group:     group,
				Mode:      job.ModeComplete,
				AuditTime: auditTime,
			}
			respContent, _ := json.Marshal(expectedResponse)
			respBody := ioutil.NopCloser(bytes.NewBuffer(respContent))
			resp := &http.Response{
				StatusCode: 200,
				Body:       respBody,
			}

			mockClient := mock.NewHttpClient()
			mockClient.On("Post", resourceURL, contentType, bytes.NewBuffer(reqContent)).Return(resp, nil)

			client := New(baseURL, mockClient)
			actualResponse, err := client.Profile(request)

			assert.Nil(t, parseErr)
			assert.Nil(t, err)
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Equal(t, expectedResponse, actualResponse)
		})
		t.Run("should return error when http req failed", func(t *testing.T) {
			reqContent, err := json.Marshal(request)
			assert.Nil(t, err)

			networkErr := errors.New("no connection error")
			resp := &http.Response{}

			mockClient := mock.NewHttpClient()
			mockClient.On("Post", resourceURL, contentType, bytes.NewBuffer(reqContent)).Return(resp, networkErr)

			client := New(baseURL, mockClient)
			profileResponse, err := client.Profile(request)

			assert.Nil(t, profileResponse)
			assert.Error(t, err)
		})
		t.Run("should return error when parse result failed", func(t *testing.T) {
			reqContent, err := json.Marshal(request)
			assert.Nil(t, err)

			respBody := ioutil.NopCloser(bytes.NewBufferString("\n\n\n"))
			resp := &http.Response{
				StatusCode: 200,
				Body:       respBody,
			}

			mockClient := mock.NewHttpClient()
			mockClient.On("Post", resourceURL, contentType, bytes.NewBuffer(reqContent)).Return(resp, nil)

			client := New(baseURL, mockClient)
			profileResponse, err := client.Profile(request)

			assert.Nil(t, profileResponse)
			assert.Error(t, err)
		})
		t.Run("should return error when http status code is not 200", func(t *testing.T) {
			reqContent, err := json.Marshal(request)
			assert.Nil(t, err)

			respBody := ioutil.NopCloser(bytes.NewBufferString("error from server"))
			resp := &http.Response{
				StatusCode: 500,
				Body:       respBody,
			}

			mockClient := mock.NewHttpClient()
			mockClient.On("Post", resourceURL, contentType, bytes.NewBuffer(reqContent)).Return(resp, nil)

			client := New(baseURL, mockClient)
			profileResponse, err := client.Profile(request)

			assert.Nil(t, profileResponse)
			assert.Error(t, err)
		})
	})

	t.Run("GetProfile", func(t *testing.T) {
		baseURL := "http://localhost:8080"
		ID := "job-1234"
		resourceURL := fmt.Sprintf("http://localhost:8080/v1beta1/profile/%s", ID)
		urn := "entity-1-project-1.dataset_a.table_x"
		group := "timestamp_field"
		filter := "date(timestamp_field) = \"2020-12-01\""

		t.Run("should get profile", func(t *testing.T) {
			expectedResponse := &model.ProfileResponse{
				ID:     ID,
				URN:    urn,
				Filter: filter,
				Group:  group,
				Mode:   job.ModeComplete,
				State:  job.StateCompleted,
			}
			respContent, _ := json.Marshal(expectedResponse)
			respBody := ioutil.NopCloser(bytes.NewBuffer(respContent))
			resp := &http.Response{
				StatusCode: 200,
				Body:       respBody,
			}

			mockClient := mock.NewHttpClient()
			mockClient.On("Get", resourceURL).Return(resp, nil)

			client := New(baseURL, mockClient)
			actualResponse, err := client.GetProfile(ID)

			assert.Nil(t, err)
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Equal(t, expectedResponse, actualResponse)
		})
		t.Run("should still call get profile when first attempt is not succeed", func(t *testing.T) {
			firstResponse := &model.ProfileResponse{
				ID:     ID,
				URN:    urn,
				Filter: filter,
				Group:  group,
				Mode:   job.ModeComplete,
				State:  job.StateInProgress,
			}
			firstRespContent, _ := json.Marshal(firstResponse)
			firstRespBody := ioutil.NopCloser(bytes.NewBuffer(firstRespContent))
			firstResp := &http.Response{
				StatusCode: 200,
				Body:       firstRespBody,
			}
			secondResponse := &model.ProfileResponse{
				ID:     ID,
				URN:    urn,
				Filter: filter,
				Group:  group,
				Mode:   job.ModeComplete,
				State:  job.StateCompleted,
			}
			secondRespContent, _ := json.Marshal(secondResponse)
			secondRespBody := ioutil.NopCloser(bytes.NewBuffer(secondRespContent))
			secondResp := &http.Response{
				StatusCode: 200,
				Body:       secondRespBody,
			}

			mockClient := mock.NewHttpClient()
			mockClient.On("Get", resourceURL).Return(firstResp, nil).Once()
			mockClient.On("Get", resourceURL).Return(secondResp, nil).Once()

			client := New(baseURL, mockClient)
			actualResponse, err := client.GetProfile(ID)

			assert.Nil(t, err)
			assert.Equal(t, http.StatusOK, firstResp.StatusCode)
			assert.Equal(t, secondResponse, actualResponse)
		})
		t.Run("should return error when http req failed", func(t *testing.T) {
			networkErr := errors.New("no connection error")
			resp := &http.Response{}

			mockClient := mock.NewHttpClient()
			mockClient.On("Get", resourceURL).Return(resp, networkErr)

			client := New(baseURL, mockClient)
			profileResponse, err := client.GetProfile(ID)

			assert.Nil(t, profileResponse)
			assert.Error(t, err)
		})
		t.Run("should return error when parse result failed", func(t *testing.T) {
			respBody := ioutil.NopCloser(bytes.NewBufferString("\n\n\n"))
			resp := &http.Response{
				StatusCode: 200,
				Body:       respBody,
			}

			mockClient := mock.NewHttpClient()
			mockClient.On("Get", resourceURL).Return(resp, nil)

			client := New(baseURL, mockClient)
			profileResponse, err := client.GetProfile(ID)

			assert.Nil(t, profileResponse)
			assert.Error(t, err)
		})
		t.Run("should return error when http status code is not 200", func(t *testing.T) {
			respBody := ioutil.NopCloser(bytes.NewBufferString("error from server"))
			resp := &http.Response{
				StatusCode: 500,
				Body:       respBody,
			}

			mockClient := mock.NewHttpClient()
			mockClient.On("Get", resourceURL).Return(resp, nil)

			client := New(baseURL, mockClient)
			profileResponse, err := client.GetProfile(ID)

			assert.Nil(t, profileResponse)
			assert.Error(t, err)
		})
	})

	t.Run("Audit", func(t *testing.T) {
		baseURL := "http://localhost:8080"
		profileID := "profile-1234"
		auditID := "audit-1234"
		resourceURL := fmt.Sprintf("http://localhost:8080/v1beta1/profile/%s/audit", profileID)
		urn := "entity-1-project-1.dataset_a.table_x"
		groupName := "timestamp_field"
		auditResultGroup := []model.AuditResultGroup{}

		t.Run("should audit", func(t *testing.T) {
			expectedResponse := &model.AuditResponse{
				AuditID:   auditID,
				ProfileID: profileID,
				URN:       urn,
				GroupName: groupName,
				Status:    job.StateCompleted.String(),
				Pass:      true,
				Message:   "",
				Result:    auditResultGroup,
			}
			respContent, _ := json.Marshal(expectedResponse)
			respBody := ioutil.NopCloser(bytes.NewBuffer(respContent))
			resp := &http.Response{
				StatusCode: 200,
				Body:       respBody,
			}

			mockClient := mock.NewHttpClient()
			mockClient.On("Post", resourceURL, contentType, nil).Return(resp, nil)

			client := New(baseURL, mockClient)
			actualResponse, err := client.Audit(profileID)

			assert.Nil(t, err)
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Equal(t, expectedResponse, actualResponse)
		})
		t.Run("should return error when http req failed", func(t *testing.T) {
			networkErr := errors.New("no connection error")
			resp := &http.Response{}

			mockClient := mock.NewHttpClient()
			mockClient.On("Post", resourceURL, contentType, nil).Return(resp, networkErr)

			client := New(baseURL, mockClient)
			profileResponse, err := client.Audit(profileID)

			assert.Nil(t, profileResponse)
			assert.Error(t, err)
		})
		t.Run("should return error when parse result failed", func(t *testing.T) {
			respBody := ioutil.NopCloser(bytes.NewBufferString("\n\n\n"))
			resp := &http.Response{
				StatusCode: 200,
				Body:       respBody,
			}

			mockClient := mock.NewHttpClient()
			mockClient.On("Post", resourceURL, contentType, nil).Return(resp, nil)

			client := New(baseURL, mockClient)
			profileResponse, err := client.Audit(profileID)

			assert.Nil(t, profileResponse)
			assert.Error(t, err)
		})
		t.Run("should return error when http status code is not 200", func(t *testing.T) {
			respBody := ioutil.NopCloser(bytes.NewBufferString("error from server"))
			resp := &http.Response{
				StatusCode: 500,
				Body:       respBody,
			}

			mockClient := mock.NewHttpClient()
			mockClient.On("Post", resourceURL, contentType, nil).Return(resp, nil)

			client := New(baseURL, mockClient)
			profileResponse, err := client.Audit(profileID)

			assert.Nil(t, profileResponse)
			assert.Error(t, err)
		})
	})
}
