package common

import (
	"github.com/odpf/predator/protocol/meta"
	"github.com/odpf/predator/protocol/query"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGenerateFilterExpression(t *testing.T) {
	t.Run("should return filter when filter not empty", func(t *testing.T) {
		filter := "active = true"
		tableSpec := &meta.TableSpec{
			PartitionField:         "",
			RequirePartitionFilter: false,
		}

		expected := &query.CustomFilterExpression{
			Expression: filter,
		}

		expression := GenerateFilterExpression(filter, tableSpec)
		assert.Equal(t, expected, expression)
	})
	t.Run("should return filter when filter empty but not require partition", func(t *testing.T) {
		filter := ""
		tableSpec := &meta.TableSpec{
			PartitionField:         "",
			RequirePartitionFilter: false,
		}
		expected := &query.NoFilter{}

		expression := GenerateFilterExpression(filter, tableSpec)
		assert.Equal(t, expected, expression)
	})
	t.Run("should return filter when filter empty and require partition", func(t *testing.T) {
		filter := ""
		tableSpec := &meta.TableSpec{
			PartitionField:         "sample_field",
			RequirePartitionFilter: true,
		}
		expected := &query.AllPartitionFilter{PartitionColumn: "sample_field"}

		expression := GenerateFilterExpression(filter, tableSpec)
		assert.Equal(t, expected, expression)
	})
}
