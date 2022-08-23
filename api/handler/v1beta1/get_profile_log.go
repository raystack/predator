package v1beta1

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/odpf/predator/api/model"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/util"
)

//GetProfileLog provide profile logs
func GetProfileLog(profileService protocol.ProfileService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		ID := vars["profileID"]

		if !util.IsUUIDValid(ID) {
			printError(w, errors.New("invalid profileID"), http.StatusBadRequest)
			return
		}

		profile, err := profileService.Get(ID)
		if err != nil {
			printError(w, err, http.StatusInternalServerError)
			return
		}

		statusList, err := profileService.GetLog(ID)
		if err != nil {
			printError(w, err, http.StatusInternalServerError)
			return
		}

		var logs []model.Log
		for _, singleStatus := range statusList {
			log := model.Log{
				Status:         singleStatus.Status,
				Message:        singleStatus.Message,
				EventTimestamp: singleStatus.EventTimestamp,
			}
			logs = append(logs, log)
		}

		response := &model.ProfileLogResponse{
			ID:           profile.ID,
			URN:          profile.URN,
			Filter:       profile.Filter,
			Group:        profile.GroupName,
			Mode:         profile.Mode,
			AuditTime:    profile.AuditTimestamp,
			CreatedAt:    profile.EventTimestamp,
			UpdatedAt:    profile.UpdatedTimestamp,
			State:        profile.Status,
			TotalRecords: profile.TotalRecords,
			Logs:         logs,
		}

		w.Header().Set("Content-Type", "application/json")

		if err := json.NewEncoder(w).Encode(response); err != nil {
			printError(w, err, http.StatusInternalServerError)
		}
	}
}
