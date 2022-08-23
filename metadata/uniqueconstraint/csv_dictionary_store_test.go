package uniqueconstraint_test

import (
	"testing"

	"github.com/odpf/predator/metadata/uniqueconstraint"
	"github.com/stretchr/testify/assert"
)

func TestLocalCSVUniqueConstraintStore(t *testing.T) {
	t.Run("FetchConstraints", func(t *testing.T) {
		t.Run("should return unique constrains", func(t *testing.T) {
			filePath := "uniqueconstraint.csv"

			content := []byte(`sample-project.sample_dataset.sample_table_a;field1,field2
sample-project.sample_dataset.sample_table_b;field1`)

			expected := map[string][]string{
				"sample-project.sample_dataset.sample_table_a": {"field1", "field2"},
				"sample-project.sample_dataset.sample_table_b": {"field1"},
			}

			reader := &mockFileReader{}
			reader.On("ReadFile", filePath).Return(content, nil)
			defer reader.AssertExpectations(t)

			store := uniqueconstraint.NewCSVDictionaryStore(filePath, reader)

			constrainsDict, err := store.Get()

			assert.Nil(t, err)
			assert.Equal(t, expected, constrainsDict)
		})
	})
}
