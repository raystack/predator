package v1beta1

import (
	"encoding/json"
	"github.com/odpf/predator/api/model"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/odpf/predator/protocol/macros"
	"net/http"
	"time"
)

//Profile handle profile creation request
func Profile(profileService protocol.ProfileService, sqlExpressionFac protocol.SQLExpressionFactory) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		var body model.ProfileRequest
		if err := getRequestBody(r, &body); err != nil {
			printError(w, err, http.StatusBadRequest)
			return
		}

		if err := body.Validate(); err != nil {
			printError(w, err, http.StatusBadRequest)
			return
		}

		currentTime := time.Now().In(time.UTC)
		profile := &job.Profile{
			URN:            body.URN,
			Mode:           body.Mode,
			Filter:         body.Filter,
			GroupName:      body.Group,
			Status:         job.StateCreated,
			Message:        "profile created",
			EventTimestamp: currentTime,
		}

		if len(body.AuditTime) > 0 {
			auditTime, err := time.Parse(time.RFC3339, body.AuditTime)
			if err != nil {
				printError(w, err, http.StatusBadRequest)
			}
			profile.AuditTimestamp = auditTime
		} else {
			profile.AuditTimestamp = currentTime
		}

		if macros.IsUsingMacros(profile.GroupName, macros.Partition) {
			renderedExpression, err := sqlExpressionFac.CreatePartitionExpression(profile.URN)
			if err != nil {
				if err == protocol.ErrPartitionExpressionIsNotSupported {
					printError(w, err, http.StatusBadRequest)
					return
				}
				printError(w, err, http.StatusInternalServerError)
				return
			}

			newGroup, err := macros.ReplaceMacros(profile.GroupName, renderedExpression, macros.Partition)
			if err != nil {
				printError(w, err, http.StatusInternalServerError)
				return
			}
			profile.GroupName = newGroup
		}

		if macros.IsUsingMacros(profile.Filter, macros.Partition) {
			renderedExpression, err := sqlExpressionFac.CreatePartitionExpression(profile.URN)
			if err != nil {
				if err == protocol.ErrPartitionExpressionIsNotSupported {
					printError(w, err, http.StatusBadRequest)
					return
				}
				printError(w, err, http.StatusInternalServerError)
				return
			}
			newFilter, err := macros.ReplaceMacros(profile.Filter, renderedExpression, macros.Partition)
			if err != nil {
				printError(w, err, http.StatusInternalServerError)
				return
			}
			profile.Filter = newFilter
		}

		profile, err := profileService.CreateProfile(profile)
		if err != nil {
			printError(w, err, http.StatusInternalServerError)
			return
		}

		response := &model.ProfileResponse{
			ID:        profile.ID,
			URN:       profile.URN,
			Filter:    profile.Filter,
			Group:     profile.GroupName,
			Mode:      profile.Mode,
			AuditTime: profile.AuditTimestamp,
			CreatedAt: profile.EventTimestamp,
			UpdatedAt: profile.UpdatedTimestamp,
		}

		w.Header().Set("Content-Type", "application/json")

		if err := json.NewEncoder(w).Encode(response); err != nil {
			printError(w, err, http.StatusInternalServerError)
		}
	}
}
