package protocol

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLabel(t *testing.T) {
	t.Run("ParseLabel", func(t *testing.T) {
		t.Run("should return label", func(t *testing.T) {
			urn := "entity-1-project-1.dataset_a.table_x"
			expected := &Label{
				Project: "entity-1-project-1",
				Dataset: "dataset_a",
				Table:   "table_x",
			}
			label, err := ParseLabel(urn)

			assert.Nil(t, err)
			assert.Equal(t, expected, label)
		})
		t.Run("should return error when format is wrong", func(t *testing.T) {
			urn := "entity-1-project-1.dataset_a"
			_, err := ParseLabel(urn)

			assert.NotNil(t, err)
		})
	})
}
