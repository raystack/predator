package uniqueconstraint_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/predator/metadata/uniqueconstraint"
)

func TestCachedUniqueConstraintSource(t *testing.T) {
	t.Run("CachedUniqueConstraintSource", func(t *testing.T) {
		t.Run("Get", func(t *testing.T) {
			t.Run("should return unique constraint dictionary", func(t *testing.T) {
				fields := []string{"a", "b", "c"}

				dict := map[string][]string{
					"project.dataset.table": fields,
				}

				directDictionaryStore := &mockUniqueConstraintDictionaryStore{}
				directDictionaryStore.On("Get").Return(dict, nil)
				defer directDictionaryStore.AssertExpectations(t)

				cachedDictionaryStore := uniqueconstraint.NewCachedDictionaryStore(60, directDictionaryStore)

				result, err := cachedDictionaryStore.Get()

				assert.Nil(t, err)
				assert.Equal(t, dict, result)
			})
			t.Run("should only call original source once", func(t *testing.T) {
				fields := []string{"a", "b", "c"}

				dict := map[string][]string{
					"project.dataset.table": fields,
				}

				directStore := &mockUniqueConstraintDictionaryStore{}
				directStore.On("Get").Return(dict, nil).Once()
				defer directStore.AssertExpectations(t)

				cachedStore := uniqueconstraint.NewCachedDictionaryStore(10, directStore)

				_, err := cachedStore.Get()
				assert.Nil(t, err)

				time.Sleep(1 * time.Second)

				for i := 1; i <= 30; i++ {
					_, err := cachedStore.Get()
					assert.Nil(t, err)
				}
			})
		})
	})
}
