package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProfile(t *testing.T) {
	t.Run("validate", func(t *testing.T) {
		t.Run("it should return error when Table URN is empty", func(t *testing.T) {
			tableURN := ""
			profile := &ProfileRequest{
				URN: tableURN,
			}
			err := profile.Validate()

			assert.NotNil(t, err)
		})

		t.Run("it should return error when Table URN is in the wrong format", func(t *testing.T) {
			tableURN := "wrong-format"
			profile := &ProfileRequest{
				URN: tableURN,
			}
			err := profile.Validate()

			assert.NotNil(t, err)
		})
	})
}
