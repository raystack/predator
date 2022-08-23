package client

import (
	"github.com/odpf/predator/stats"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFormatTags(t *testing.T) {
	t.Run("should merge duplicated tags", func(t *testing.T) {
		tags := []stats.KV{
			{
				K: "pod",
				V: "123",
			},
			{
				K: "environment",
				V: "prod",
			},
			{
				K: "pod",
				V: "456",
			},
		}
		tagStr := []string{"environment=prod", "pod=456"}
		formatted := formatTags(tags)

		assert.Equal(t, tagStr, formatted)
	})
}
