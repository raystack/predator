package v1beta1

import (
	"encoding/json"
	"errors"
	"github.com/odpf/predator/api/model"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"net/http"
)

//Upload handle upload tolerance spec to repository
func Upload(uploadFactory protocol.UploadFactory) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body model.UploadRequest
		if err := getRequestBody(r, &body); err != nil {
			printError(w, err, http.StatusBadRequest)
			return
		}

		if err := body.Validate(); err != nil {
			printError(w, err, http.StatusBadRequest)
			return
		}

		gitRepo := &protocol.GitInfo{
			URL:        body.GitURL,
			CommitID:   body.CommitID,
			PathPrefix: body.PathPrefix,
		}

		uploadTask, err := uploadFactory.Create(gitRepo)
		if err != nil {
			printError(w, err, http.StatusInternalServerError)
			return
		}

		result, err := uploadTask.Run()
		if err != nil {
			if protocol.IsUploadSpecValidationError(err) {
				printError(w, err, http.StatusBadRequest)
				return
			}
			printError(w, err, http.StatusInternalServerError)
			return
		}

		diff, ok := result.(*job.Diff)
		if !ok {
			err := errors.New("something wrong with upload job")
			printError(w, err, http.StatusInternalServerError)
			return
		}

		report := &model.UploadReport{
			RemovedCount:  diff.RemovedCount(),
			UploadedCount: diff.AddedCount() + diff.UpdatedCount(),
		}

		if err := json.NewEncoder(w).Encode(report); err != nil {
			printError(w, err, http.StatusInternalServerError)
		}
	}
}
