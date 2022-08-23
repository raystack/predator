package protocol

import (
	"github.com/odpf/predator/protocol/job"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEntry(t *testing.T) {
	t.Run("Entry", func(t *testing.T) {
		t.Run("Status", func(t *testing.T) {
			t.Run("should return empty string when not found", func(t *testing.T) {
				entry := NewEntry().WithJobID("abcd")
				result := entry.Status()

				assert.Equal(t, "", result)
			})
			t.Run("should return status", func(t *testing.T) {
				entry := NewEntry().WithStatus("abcd")
				result := entry.Status()

				assert.Equal(t, "abcd", result)
			})
		})
		t.Run("PartitionedMetric", func(t *testing.T) {
			t.Run("should return empty string when not found", func(t *testing.T) {
				entry := NewEntry().WithJobID("abcd")
				result := entry.Partition()

				assert.Equal(t, "", result)
			})
			t.Run("should return partition", func(t *testing.T) {
				entry := NewEntry().WithPartition("2019-01-01")
				result := entry.Partition()

				assert.Equal(t, "2019-01-01", result)
			})
		})
		t.Run("URN", func(t *testing.T) {
			t.Run("should return empty string when not found", func(t *testing.T) {
				entry := NewEntry().WithJobID("abcd")
				result := entry.TableURN()

				assert.Equal(t, "", result)
			})
			t.Run("should return URN", func(t *testing.T) {
				entry := NewEntry().WithTableURN("p.d.t")
				result := entry.TableURN()

				assert.Equal(t, "p.d.t", result)
			})
		})
		t.Run("JobType", func(t *testing.T) {
			t.Run("should return empty job type when not found", func(t *testing.T) {
				entry := NewEntry().WithJobID("abcd")
				result := entry.JobType()

				assert.Equal(t, job.Type(""), result)
			})
			t.Run("should return JobType", func(t *testing.T) {
				entry := NewEntry().WithJobType(job.TypeAudit)
				result := entry.JobType()

				assert.Equal(t, job.TypeAudit, result)
			})
		})
		t.Run("JobID", func(t *testing.T) {
			t.Run("should return empty string when job id not found", func(t *testing.T) {
				entry := NewEntry()
				result := entry.JobID()

				assert.Equal(t, "", result)
			})
			t.Run("should return job id", func(t *testing.T) {
				entry := NewEntry().WithJobID("abcd")
				result := entry.JobID()

				assert.Equal(t, "abcd", result)
			})
		})
	})
}
