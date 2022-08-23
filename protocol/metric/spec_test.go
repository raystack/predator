package metric

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetricSpec(t *testing.T) {
	t.Run("GetMetricSpecsByFieldID", func(t *testing.T) {
		tableID := "project.dataset.table"
		allMetricSpecs := []*Spec{
			{
				Name:    "count",
				FieldID: "field1",
				TableID: tableID,
			},
			{
				Name:    "null_count",
				FieldID: "field1",
				TableID: tableID,
			},
			{
				Name:    "count",
				FieldID: "field2",
				TableID: tableID,
			},
		}

		t.Run("should return fields if field Partition match", func(t *testing.T) {
			expected := []*Spec{
				{
					Name:    "count",
					FieldID: "field1",
					TableID: tableID,
				},
				{
					Name:    "null_count",
					FieldID: "field1",
					TableID: tableID,
				},
			}

			actual, err := SpecFinder(allMetricSpecs).GetMetricSpecsByFieldID("field1")
			assert.Equal(t, expected, actual)
			assert.Nil(t, err)
		})

		t.Run("should return error when specs with specified field Partition not found", func(t *testing.T) {
			_, err := SpecFinder(allMetricSpecs).GetMetricSpecsByFieldID("field3")
			assert.NotNil(t, err)
		})
	})
}
