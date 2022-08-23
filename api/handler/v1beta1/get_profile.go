package v1beta1

import (
	"encoding/json"
	"errors"
	"github.com/odpf/predator/protocol/metric"
	"github.com/odpf/predator/util"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/odpf/predator/api/model"
	"github.com/odpf/predator/protocol"
)

//GetProfile provide profile information
func GetProfile(profileService protocol.ProfileService, metricStore protocol.MetricStore) http.HandlerFunc {
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

		metrics, err := metricStore.GetMetricsByProfileID(ID)
		if err != nil && err != protocol.ErrNoProfileMetricFound {
			printError(w, err, http.StatusInternalServerError)
			return
		}

		metricGroupMap := make(map[string][]*metric.Metric)
		for _, m := range metrics {
			metricGroupMap[m.GroupValue] = append(metricGroupMap[m.GroupValue], m)
		}

		var metricGroups []*model.MetricGroup
		for groupValue, mgs := range metricGroupMap {
			var metricsResponse []*model.Metric

			for _, m := range mgs {
				mr := &model.Metric{
					FieldID:   m.FieldID,
					Name:      m.Type,
					Category:  m.Category,
					Owner:     m.Owner,
					Value:     m.Value,
					Condition: m.Condition,
					Metadata:  m.Metadata,
				}
				metricsResponse = append(metricsResponse, mr)
			}

			mg := &model.MetricGroup{
				Group:   groupValue,
				Metrics: metricsResponse,
			}

			metricGroups = append(metricGroups, mg)
		}

		response := &model.ProfileResponse{
			ID:           profile.ID,
			URN:          profile.URN,
			Filter:       profile.Filter,
			Group:        profile.GroupName,
			Mode:         profile.Mode,
			AuditTime:    profile.AuditTimestamp,
			CreatedAt:    profile.EventTimestamp,
			State:        profile.Status,
			Message:      profile.Message,
			TotalRecords: profile.TotalRecords,
			Metrics:      metricGroups,
			UpdatedAt:    profile.UpdatedTimestamp,
		}

		w.Header().Set("Content-Type", "application/json")

		if err := json.NewEncoder(w).Encode(response); err != nil {
			printError(w, err, http.StatusInternalServerError)
		}
	}
}
