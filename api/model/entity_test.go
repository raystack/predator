package model

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEntity(t *testing.T) {
	t.Run("Validate", func(t *testing.T) {
		t.Run("should return error when entity name is empty", func(t *testing.T) {
			req := &CreateUpdateEntityRequest{
				EntityName:    "",
				GitURL:        "git@sample-url:entity-1.git",
				Environment:   "env-a",
				GcpProjectIDs: []string{"entity-1-project-1"},
			}

			err := req.Validate()
			assert.NotNil(t, err)
		})
		t.Run("should return error when git url is empty", func(t *testing.T) {
			req := &CreateUpdateEntityRequest{
				EntityName:    "entity-1",
				Environment:   "env-a",
				GcpProjectIDs: []string{"entity-1-project-1"},
			}

			err := req.Validate()
			assert.NotNil(t, err)
		})
		t.Run("should return error when git url is not in the supported format", func(t *testing.T) {
			req := &CreateUpdateEntityRequest{
				EntityName:    "entity-1",
				Environment:   "env-a",
				GitURL:        "invalid-git-url",
				GcpProjectIDs: []string{"entity-1-project-1"},
			}

			err := req.Validate()
			assert.NotNil(t, err)
		})
		t.Run("should return error when environment is empty", func(t *testing.T) {
			req := &CreateUpdateEntityRequest{
				EntityName:    "entity-1",
				GitURL:        "git@sample-url:entity-1.git",
				GcpProjectIDs: []string{"entity-1-project-1"},
			}

			err := req.Validate()
			assert.NotNil(t, err)
		})
		t.Run("should return error when one of gcp project id is empty string", func(t *testing.T) {
			req := &CreateUpdateEntityRequest{
				EntityName:    "entity-1",
				GitURL:        "git@sample-url:entity-1.git",
				GcpProjectIDs: []string{""},
			}

			err := req.Validate()

			assert.NotNil(t, err)
		})
	})
}
