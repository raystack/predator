package macros

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMacros(t *testing.T) {
	t.Run("IsUsingMacros", func(t *testing.T) {
		t.Run("should return true when text includes macros", func(t *testing.T) {
			filterName := "__PARTITION__ = '2021-01-01'"
			actual := IsUsingMacros(filterName, Partition)
			assert.True(t, actual)
		})
		t.Run("should return false when text not include macros", func(t *testing.T) {
			filterName := "date(field_timestamp) = '2021-01-01'"
			actual := IsUsingMacros(filterName, Partition)
			assert.False(t, actual)
		})
	})
}
