package v1beta1

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/gorilla/mux"
	"github.com/odpf/predator/api/model"
	"github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCreateUpdateEntity(t *testing.T) {
	t.Run("CreateUpdateEntity", func(t *testing.T) {
		t.Run("should create or update entity", func(t *testing.T) {
			entityID := "entity-1"

			entityRequest := &model.CreateUpdateEntityRequest{
				EntityName:    "entity-1-name",
				GitURL:        "git@sample-url:entity-1.git",
				Environment:   "integration",
				GcpProjectIDs: []string{"entity-1-project-1"},
			}

			entity := &protocol.Entity{
				ID:            entityID,
				Name:          "entity-1-name",
				GitURL:        "git@sample-url:entity-1.git",
				Environment:   "integration",
				GcpProjectIDs: []string{"entity-1-project-1"},
			}

			requestBody, _ := json.Marshal(entityRequest)

			entityStore := &mock.EntityStoreMock{}
			defer entityStore.AssertExpectations(t)
			var entities []*protocol.Entity
			entityStore.On("GetAll").Return(entities, nil)
			entityStore.On("Save", entity).Return(entity, nil)

			response := &model.CreateUpdateEntityResponse{
				EntityID:      entityID,
				EntityName:    "entity-1-name",
				GitURL:        "git@sample-url:entity-1.git",
				Environment:   "integration",
				GcpProjectIDs: []string{"entity-1-project-1"},
			}

			req := httptest.NewRequest("POST", "/entity/"+entityID, bytes.NewBuffer(requestBody))
			req = mux.SetURLVars(req, map[string]string{
				"entityID": entityID,
			})

			res := httptest.NewRecorder()

			handler := CreateUpdateEntity(entityStore)
			handler.ServeHTTP(res, req)

			var result model.CreateUpdateEntityResponse
			err := json.NewDecoder(res.Body).Decode(&result)
			assert.Nil(t, err)

			assert.Equal(t, response, &result)
			assert.Equal(t, http.StatusOK, res.Code)
		})
		t.Run("should return 400 when gcp project id is contains duplicate", func(t *testing.T) {
			entityID := "entity-1"

			entityRequest := &model.CreateUpdateEntityRequest{
				EntityName:    "entity-1-name",
				GitURL:        "git@sample-url:entity-1.git",
				Environment:   "integration",
				GcpProjectIDs: []string{"entity-1-project-1", "entity-1-project-1"},
			}

			requestBody, _ := json.Marshal(entityRequest)

			entityStore := &mock.EntityStoreMock{}
			defer entityStore.AssertExpectations(t)

			req := httptest.NewRequest("POST", "/entity/"+entityID, bytes.NewBuffer(requestBody))
			req = mux.SetURLVars(req, map[string]string{
				"entityID": entityID,
			})

			res := httptest.NewRecorder()

			handler := CreateUpdateEntity(entityStore)
			handler.ServeHTTP(res, req)

			assert.Equal(t, http.StatusBadRequest, res.Code)
		})
		t.Run("should return 400 when entity request is invalid", func(t *testing.T) {
			entityID := "entity-1"

			entityRequest := &model.CreateUpdateEntityRequest{
				EntityName:    "",
				GitURL:        "git@sample-url:entity-1.git",
				Environment:   "integration",
				GcpProjectIDs: []string{"entity-1-project-1"},
			}

			requestBody, _ := json.Marshal(entityRequest)

			entityStore := &mock.EntityStoreMock{}
			defer entityStore.AssertExpectations(t)

			req := httptest.NewRequest("POST", "/entity/"+entityID, bytes.NewBuffer(requestBody))
			req = mux.SetURLVars(req, map[string]string{
				"entityID": entityID,
			})

			res := httptest.NewRecorder()

			handler := CreateUpdateEntity(entityStore)
			handler.ServeHTTP(res, req)

			assert.Equal(t, http.StatusBadRequest, res.Code)
		})
		t.Run("should return 400 when request body format is wrong", func(t *testing.T) {
			entityID := "entity-1"

			entityStore := &mock.EntityStoreMock{}
			defer entityStore.AssertExpectations(t)

			body := `{ "entity_Naame": "abcd" }`
			req := httptest.NewRequest("POST", "/entity/"+entityID, bytes.NewBufferString(body))
			req = mux.SetURLVars(req, map[string]string{
				"entityID": entityID,
			})

			res := httptest.NewRecorder()

			handler := CreateUpdateEntity(entityStore)
			handler.ServeHTTP(res, req)

			assert.Equal(t, http.StatusBadRequest, res.Code)
		})
		t.Run("should return 400 when entity gcp project id is already exist", func(t *testing.T) {
			entityID := "entity-1"

			entityRequest := &model.CreateUpdateEntityRequest{
				EntityName:    "entity-1-name",
				GitURL:        "git@sample-url:entity-1.git",
				Environment:   "integration",
				GcpProjectIDs: []string{"entity-1-project-1"},
			}

			entities := []*protocol.Entity{
				{
					ID:            "g-entity-1-name",
					GcpProjectIDs: []string{"entity-1-project-1"},
				},
			}

			requestBody, _ := json.Marshal(entityRequest)

			entityStore := &mock.EntityStoreMock{}
			defer entityStore.AssertExpectations(t)
			entityStore.On("GetAll").Return(entities, nil)

			req := httptest.NewRequest("POST", "/entity/"+entityID, bytes.NewBuffer(requestBody))
			req = mux.SetURLVars(req, map[string]string{
				"entityID": entityID,
			})

			res := httptest.NewRecorder()

			handler := CreateUpdateEntity(entityStore)
			handler.ServeHTTP(res, req)

			assert.Equal(t, http.StatusBadRequest, res.Code)
		})
		t.Run("should return 500 when failed to create entity", func(t *testing.T) {
			entityID := "entity-1"

			entityRequest := &model.CreateUpdateEntityRequest{
				EntityName:    "entity-1-name",
				GitURL:        "git@sample-url:entity-1.git",
				Environment:   "integration",
				GcpProjectIDs: []string{"entity-1-project-1"},
			}

			entity := &protocol.Entity{
				ID:            entityID,
				Name:          "entity-1-name",
				GitURL:        "git@sample-url:entity-1.git",
				Environment:   "integration",
				GcpProjectIDs: []string{"entity-1-project-1"},
			}

			dbError := errors.New("db connection error")

			requestBody, _ := json.Marshal(entityRequest)

			entityStore := &mock.EntityStoreMock{}
			defer entityStore.AssertExpectations(t)
			var entities []*protocol.Entity
			entityStore.On("GetAll").Return(entities, nil)
			entityStore.On("Save", entity).Return(&protocol.Entity{}, dbError)

			req := httptest.NewRequest("POST", "/entity/"+entityID, bytes.NewBuffer(requestBody))
			req = mux.SetURLVars(req, map[string]string{
				"entityID": entityID,
			})

			res := httptest.NewRecorder()

			handler := CreateUpdateEntity(entityStore)
			handler.ServeHTTP(res, req)

			assert.Equal(t, http.StatusInternalServerError, res.Code)
		})
	})
}
