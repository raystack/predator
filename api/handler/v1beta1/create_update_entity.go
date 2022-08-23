package v1beta1

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/odpf/predator/api/model"
	"github.com/odpf/predator/protocol"
	"net/http"
)

//CreateUpdateEntity to create and update entity information
func CreateUpdateEntity(entityStore protocol.EntityStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		ID := vars["entityID"]

		if ID == "" {
			printError(w, errors.New("invalid entityID"), http.StatusBadRequest)
			return
		}

		var body model.CreateUpdateEntityRequest
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&body); err != nil {
			printError(w, err, http.StatusBadRequest)
			return
		}

		if err := body.Validate(); err != nil {
			printError(w, err, http.StatusBadRequest)
			return
		}

		newEntity := &protocol.Entity{
			ID:            ID,
			Name:          body.EntityName,
			Environment:   body.Environment,
			GitURL:        body.GitURL,
			GcpProjectIDs: body.GcpProjectIDs,
		}

		eValidator := &entityValidator{entityStore: entityStore}
		if err := eValidator.Validate(newEntity); err != nil {
			printError(w, err, http.StatusBadRequest)
			return
		}

		storedEntity, err := entityStore.Save(newEntity)
		if err != nil {
			printError(w, err, http.StatusInternalServerError)
			return
		}

		resp := &model.CreateUpdateEntityResponse{
			EntityID:         ID,
			EntityName:       storedEntity.Name,
			GitURL:           storedEntity.GitURL,
			Environment:      storedEntity.Environment,
			GcpProjectIDs:    storedEntity.GcpProjectIDs,
			CreatedTimestamp: storedEntity.CreatedAt,
			UpdatedTimestamp: storedEntity.UpdatedAt,
		}

		w.Header().Set("Content-Type", "application/json")

		if err := json.NewEncoder(w).Encode(resp); err != nil {
			printError(w, err, http.StatusInternalServerError)
		}
	}
}

type entityValidator struct {
	entityStore protocol.EntityStore
}

//Validate ensure a gcp project is not registered on more than one entity
func (v *entityValidator) Validate(entity *protocol.Entity) error {

	projectIDCount := make(map[string]int)
	for _, projectID := range entity.GcpProjectIDs {
		projectIDCount[projectID]++
	}

	for p, count := range projectIDCount {
		if count > 1 {
			return fmt.Errorf("duplicated project id %s", p)
		}
	}

	entities, err := v.entityStore.GetAll()
	if err != nil {
		return err
	}

	projectIDMap := make(map[string]string)
	for _, e := range entities {
		if e.ID != entity.ID {
			for _, projectID := range e.GcpProjectIDs {
				projectIDMap[projectID] = entity.ID
			}
		}
	}

	for _, projectID := range entity.GcpProjectIDs {
		if e, ok := projectIDMap[projectID]; ok {
			return fmt.Errorf("error, gcp project id used in the entity %s is exist in other entity : %s", projectID, e)
		}
	}

	return nil
}
