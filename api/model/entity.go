package model

import (
	"errors"
	"github.com/odpf/predator/protocol"
	"time"
)

//CreateUpdateEntityRequest request to create and update entity
type CreateUpdateEntityRequest struct {
	EntityName    string   `json:"entity_name"`
	GitURL        string   `json:"git_url"`
	Environment   string   `json:"environment"`
	GcpProjectIDs []string `json:"gcloud_project_ids"`
}

func (c *CreateUpdateEntityRequest) Validate() error {

	if len(c.EntityName) == 0 {
		return errors.New("entity_name cannot be empty")
	}

	if len(c.GitURL) == 0 {
		return errors.New("git_url cannot be empty")
	}

	if !protocol.GitSshUrlPattern.MatchString(c.GitURL) {
		return errors.New("unsupported git_url format")
	}

	if len(c.Environment) == 0 {
		return errors.New("environment cannot be empty")
	}

	if len(c.GcpProjectIDs) > 0 {
		for _, projectID := range c.GcpProjectIDs {
			if len(projectID) == 0 {
				return errors.New("gcp project id cannot be an empty string")
			}
		}
	}

	return nil
}

//CreateUpdateEntityResponse response of creation and update entity
type CreateUpdateEntityResponse struct {
	EntityID         string    `json:"entity_id"`
	EntityName       string    `json:"entity_name"`
	GitURL           string    `json:"git_url"`
	Environment      string    `json:"environment"`
	GcpProjectIDs    []string  `json:"gcloud_project_ids"`
	CreatedTimestamp time.Time `json:"created_timestamp"`
	UpdatedTimestamp time.Time `json:"updated_timestamp"`
}

//ListEntityResponse
type ListEntityResponse struct {
	Entities []*CreateUpdateEntityResponse `json:"entities"`
}
