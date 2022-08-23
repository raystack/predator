package xlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValue(t *testing.T) {
	t.Run("serialise", func(t *testing.T) {
		t.Run("should return key=value separated by comma", func(t *testing.T) {
			result := serialise([]Value{
				NewValue("abc", 1),
				NewValue("def", 2),
			})

			assert.Equal(t, "abc=1 ,def=2", result)
		})
		t.Run("should return empty string when no value given", func(t *testing.T) {
			result := serialise([]Value{})

			assert.Equal(t, "", result)
		})
	})
}
