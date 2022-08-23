package v1beta1

import (
	"encoding/json"
	"errors"
	"github.com/gorilla/mux"
	"github.com/odpf/predator/api/model"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"net/http"
	"sort"
)

//Audit to validate the request and start auditing
func Audit(auditService protocol.AuditService, profileService protocol.ProfileService, summaryCreator protocol.AuditSummaryFactory) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		id := vars["profileID"]
		if id == "" {
			printError(w, errors.New("invalid profileID"), http.StatusBadRequest)
			return
		}

		auditResult, err := auditService.RunAudit(id)
		if err != nil {
			if err == protocol.ErrProfileNotFound {
				printError(w, err, http.StatusBadRequest)
				return
			}
			printError(w, err, http.StatusInternalServerError)
			return
		}

		summary, err := summaryCreator.Create(auditResult.AuditReports, auditResult.Audit)
		if err != nil {
			printError(w, err, http.StatusBadRequest)
			return
		}

		profile, err := profileService.Get(id)
		if err != nil {
			printError(w, err, http.StatusBadRequest)
			return
		}

		response := convertToResponse(auditResult, profile)
		response.Pass = summary.IsPass
		response.Message = summary.Message

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			printError(w, err, http.StatusInternalServerError)
			return
		}
	}
}

func convertToResponse(auditResult *protocol.AuditResult, profile *job.Profile) *model.AuditResponse {
	group := protocol.AuditGroup(auditResult.AuditReports)
	auditResGrouped := group.ByGroupValue()

	var keys []string
	for key := range auditResGrouped {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var auditGroupedResp []model.AuditResultGroup
	for _, group := range keys {
		res := auditResGrouped[group]
		var passFlag = true
		var resultList []model.AuditResult
		for _, element := range res {
			converted := model.AuditResult{
				FieldID:        element.FieldID,
				MetricName:     element.MetricName.String(),
				MetricValue:    element.MetricValue,
				Condition:      element.Condition,
				Metadata:       element.Metadata,
				Pass:           element.PassFlag,
				ToleranceRules: element.ToleranceRules,
			}
			passFlag = passFlag && converted.Pass
			resultList = append(resultList, converted)
		}
		r := model.AuditResultGroup{
			GroupValue:   group,
			AuditResults: resultList,
			Pass:         passFlag,
		}
		auditGroupedResp = append(auditGroupedResp, r)
	}

	return &model.AuditResponse{
		AuditID:      auditResult.Audit.ID,
		ProfileID:    auditResult.Audit.ProfileID,
		URN:          auditResult.Audit.URN,
		GroupName:    profile.GroupName,
		Filter:       profile.Filter,
		Mode:         profile.Mode,
		Status:       string(auditResult.Audit.State),
		Result:       auditGroupedResp,
		TotalRecords: profile.TotalRecords,
		CreatedAt:    auditResult.Audit.EventTimestamp,
	}
}
