package uniqueconstraint_test

import (
	"testing"

	"github.com/odpf/predator/metadata/uniqueconstraint"
	"github.com/stretchr/testify/assert"
)

func TestDictionaryStoreFactory(t *testing.T) {
	t.Run("CreateDictionaryStore", func(t *testing.T) {
		t.Run("should return csv unique constrait dictionary store when no scheme", func(t *testing.T) {
			factory := uniqueconstraint.NewDictionaryStoreFactory()

			store, err := factory.CreateDictionaryStore("abcd.csv")

			_, ok := store.(*uniqueconstraint.CSVDictionaryStore)
			assert.True(t, ok)
			assert.Nil(t, err)

		})
	})
}
