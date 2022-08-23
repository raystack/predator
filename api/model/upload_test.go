package model

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestUpload(t *testing.T) {
	t.Run("Validate", func(t *testing.T) {
		t.Run("should return success when git url format is correct", func(t *testing.T) {
			request := UploadRequest{
				GitURL:   "git@sample-url:entity-1.git",
				CommitID: "123abcd",
			}

			err := request.Validate()
			assert.Nil(t, err)
		})
		t.Run("should return error when git url format is not supported", func(t *testing.T) {
			request := UploadRequest{
				GitURL:   "invalid-git-url-format",
				CommitID: "123abcd",
			}

			err := request.Validate()
			assert.NotNil(t, err)
		})
	})
}
