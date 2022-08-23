package model

import (
	"errors"
	"github.com/odpf/predator/protocol"
)

type UploadRequest struct {
	GitURL     string `json:"git_url"`
	CommitID   string `json:"commit_id"`
	PathPrefix string `json:"path_prefix"`
}

func (u *UploadRequest) Validate() error {
	if len(u.GitURL) == 0 {
		return errors.New("git_url cannot be empty")
	}

	if !protocol.GitSshUrlPattern.MatchString(u.GitURL) {
		return errors.New("unsupported git_url format")
	}

	return nil
}

type UploadReport struct {
	UploadedCount int `json:"uploaded"`
	RemovedCount  int `json:"removed"`
}
