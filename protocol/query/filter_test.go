package query_test

import (
	"github.com/odpf/predator/protocol/query"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFilter(t *testing.T) {
	t.Run("PartitionFilter", func(t *testing.T) {
		t.Run("Build", func(t *testing.T) {
			t.Run("should return filter by column given the timestamp data type", func(t *testing.T) {
				partitionFilter := query.PartitionFilter{
					DataType:        query.DataTypeTimestamp,
					PartitionDate:   "2019-01-01",
					PartitionColumn: "created_time",
				}

				filter := partitionFilter.Build()

				assert.Equal(t, "DATE(created_time) = '2019-01-01'", filter)
			})
			t.Run("should return filter by _PARTITIONDATE given the date data type", func(t *testing.T) {
				partitionFilter := query.PartitionFilter{
					DataType:        query.DataTypeDate,
					PartitionDate:   "2019-01-01",
					PartitionColumn: "_PARTITIONDATE",
				}

				filter := partitionFilter.Build()

				assert.Equal(t, "_PARTITIONDATE = '2019-01-01'", filter)
			})
		})
		t.Run("Equal", func(t *testing.T) {
			t.Run("should return true when same type", func(t *testing.T) {
				fcA := &query.PartitionFilter{
					DataType:        query.DataTypeTimestamp,
					PartitionDate:   "2019-01-01",
					PartitionColumn: "created_time",
				}
				fcB := &query.PartitionFilter{
					DataType:        query.DataTypeTimestamp,
					PartitionDate:   "2019-01-01",
					PartitionColumn: "created_time",
				}

				equal := fcA.Equal(fcB)

				assert.True(t, equal)
			})
			t.Run("should return false when different content", func(t *testing.T) {
				fcA := &query.PartitionFilter{
					DataType:        query.DataTypeTimestamp,
					PartitionDate:   "2019-01-01",
					PartitionColumn: "created_time",
				}
				fcB := &query.PartitionFilter{
					DataType:        query.DataTypeTimestamp,
					PartitionDate:   "2019-01-02",
					PartitionColumn: "created_time",
				}

				equal := fcA.Equal(fcB)

				assert.False(t, equal)
			})
		})
	})
	t.Run("NoFilter", func(t *testing.T) {
		t.Run("Build", func(t *testing.T) {
			t.Run("should return no filter", func(t *testing.T) {
				filterClause := query.NoFilter{}

				filter := filterClause.Build()

				assert.Equal(t, "TRUE", filter)
			})
		})
		t.Run("Equal", func(t *testing.T) {
			t.Run("should return true when same type", func(t *testing.T) {
				fcA := &query.NoFilter{}
				fcB := &query.NoFilter{}

				equal := fcA.Equal(fcB)

				assert.True(t, equal)
			})
		})
	})
}
