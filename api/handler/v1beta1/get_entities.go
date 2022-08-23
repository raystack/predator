package v1beta1

import (
	"encoding/json"
	"github.com/odpf/predator/api/model"
	"github.com/odpf/predator/protocol"
	"net/http"
)

//GetAllEntities get all entities information
func GetAllEntities(entityStore protocol.EntityStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		entities, err := entityStore.GetAll()
		if err != nil {
			printError(w, err, http.StatusInternalServerError)
			return
		}

		var elements []*model.CreateUpdateEntityResponse
		for _, ent := range entities {
			elem := &model.CreateUpdateEntityResponse{
				EntityID:         ent.ID,
				EntityName:       ent.Name,
				GitURL:           ent.GitURL,
				Environment:      ent.Environment,
				GcpProjectIDs:    ent.GcpProjectIDs,
				CreatedTimestamp: ent.CreatedAt,
				UpdatedTimestamp: ent.UpdatedAt,
			}

			elements = append(elements, elem)
		}

		resp := &model.ListEntityResponse{
			Entities: elements,
		}

		w.Header().Set("Content-Type", "application/json")

		if err := json.NewEncoder(w).Encode(resp); err != nil {
			printError(w, err, http.StatusInternalServerError)
		}
	}
}
