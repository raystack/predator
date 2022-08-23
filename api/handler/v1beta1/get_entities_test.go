package v1beta1

import (
	"encoding/json"
	"errors"
	"github.com/odpf/predator/api/model"
	"github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGetAllEntity(t *testing.T) {
	t.Run("CreateUpdateEntity", func(t *testing.T) {
		t.Run("should get all entity", func(t *testing.T) {
			entities := []*protocol.Entity{
				{
					ID: "sample-entity-1",
				},
				{
					ID: "sample-entity-2",
				},
			}

			entityStore := &mock.EntityStoreMock{}
			defer entityStore.AssertExpectations(t)
			entityStore.On("GetAll").Return(entities, nil)

			elements := []*model.CreateUpdateEntityResponse{
				{
					EntityID: "sample-entity-1",
				},
				{
					EntityID: "sample-entity-2",
				},
			}

			response := &model.ListEntityResponse{
				Entities: elements,
			}

			req := httptest.NewRequest("GET", "/entity", nil)
			res := httptest.NewRecorder()

			handler := GetAllEntities(entityStore)
			handler.ServeHTTP(res, req)

			var result model.ListEntityResponse
			json.NewDecoder(res.Body).Decode(&result)

			assert.Equal(t, response, &result)
			assert.Equal(t, http.StatusOK, res.Code)
		})
		t.Run("should return 500 when get entities failed", func(t *testing.T) {
			dbError := errors.New("db connection error")

			entityStore := &mock.EntityStoreMock{}
			defer entityStore.AssertExpectations(t)
			entityStore.On("GetAll").Return([]*protocol.Entity{}, dbError)

			req := httptest.NewRequest("GET", "/entity", nil)
			res := httptest.NewRecorder()

			handler := GetAllEntities(entityStore)
			handler.ServeHTTP(res, req)

			assert.Equal(t, http.StatusInternalServerError, res.Code)
		})
	})
}
