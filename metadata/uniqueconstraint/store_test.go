package uniqueconstraint_test

import (
	"testing"

	"github.com/odpf/predator/metadata/uniqueconstraint"
	"github.com/odpf/predator/protocol"
	"github.com/stretchr/testify/assert"
)

func TestStore(t *testing.T) {
	t.Run("FetchConstraints", func(t *testing.T) {
		t.Run("should return list of column name", func(t *testing.T) {
			tableID := "project.dataset.table"
			fields := []string{"a", "b", "c"}

			dictionary := map[string][]string{
				"project.dataset.table": fields,
			}

			dictionaryStore := &mockUniqueConstraintDictionaryStore{}
			dictionaryStore.On("Get").Return(dictionary, nil)
			uniqueConstraintStore := uniqueconstraint.NewStore(dictionaryStore)

			result, err := uniqueConstraintStore.FetchConstraints(tableID)

			assert.Equal(t, fields, result)
			assert.Nil(t, err)
		})
		t.Run("should return error not found when unique constraint found for the table do not exist", func(t *testing.T) {
			tableID := "project.dataset.table"

			dictionaryStore := &mockUniqueConstraintDictionaryStore{}
			var m map[string][]string
			dictionaryStore.On("Get").Return(m, protocol.ErrUniqueConstraintNotFound)
			uniqueConstraintStore := uniqueconstraint.NewStore(dictionaryStore)

			_, err := uniqueConstraintStore.FetchConstraints(tableID)

			assert.Equal(t, protocol.ErrUniqueConstraintNotFound, err)
		})
	})
}
