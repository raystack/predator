package metric

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGroup(t *testing.T) {
	t.Run("ByPartition", func(t *testing.T) {
		t.Run("should return partitioned group", func(t *testing.T) {
			partition1 := "2019-01-01"
			partition2 := "2019-01-02"

			metrics := []*Metric{
				{
					ID:        "1",
					Partition: partition1,
				},
				{
					ID:        "2",
					Partition: partition1,
				},
				{
					ID:        "3",
					Partition: partition2,
				},
				{
					ID:        "4",
					Partition: partition2,
				},
			}

			expected := []Group{
				{
					{
						ID:        "1",
						Partition: partition1,
					},
					{
						ID:        "2",
						Partition: partition1,
					},
				},
				{
					{
						ID:        "3",
						Partition: partition2,
					},
					{
						ID:        "4",
						Partition: partition2,
					},
				},
			}

			groups := Group(metrics).ByPartition()

			assert.Equal(t, expected, groups)
		})
	})

	t.Run("ByGroupValue", func(t *testing.T) {
		t.Run("should return grouped metrics", func(t *testing.T) {
			group1 := "2019-01-01"
			group2 := "2019-01-02"

			metrics := []*Metric{
				{
					ID:         "1",
					GroupValue: group1,
				},
				{
					ID:         "2",
					GroupValue: group1,
				},
				{
					ID:         "3",
					GroupValue: group2,
				},
				{
					ID:         "4",
					GroupValue: group2,
				},
			}

			expected := []Group{
				{
					{
						ID:         "1",
						GroupValue: group1,
					},
					{
						ID:         "2",
						GroupValue: group1,
					},
				},
				{
					{
						ID:         "3",
						GroupValue: group2,
					},
					{
						ID:         "4",
						GroupValue: group2,
					},
				},
			}

			groups := Group(metrics).ByGroupValue()

			assert.Equal(t, expected, groups)
		})
	})
}
