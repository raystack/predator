package protocol

import (
	"github.com/odpf/predator/protocol/metric"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestProfileGroup(t *testing.T) {
	t.Run("ByPartitionDate", func(t *testing.T) {
		t.Run("should return map with key partition date and with array of profile group as value ", func(t *testing.T) {
			profileID := "profile-abcd"
			tableURN := "project.dataset.table"
			currentTimestamp := time.Now().In(time.UTC)

			fieldID := "field1"
			metricValue := 3000.0

			pm1 := &ProfileMetric{
				ProfileID:      profileID,
				TableURN:       tableURN,
				Partition:      "2019-01-01",
				FieldID:        fieldID,
				MetricName:     metric.Count,
				MetricValue:    metricValue,
				EventTimestamp: currentTimestamp,
			}

			pm2 := &ProfileMetric{
				ProfileID:      profileID,
				TableURN:       tableURN,
				Partition:      "2019-01-02",
				FieldID:        fieldID,
				MetricName:     metric.Count,
				MetricValue:    metricValue,
				EventTimestamp: currentTimestamp,
			}

			profileMetrics := []*ProfileMetric{pm1, pm2}

			result := ProfileGroup(profileMetrics).ByPartitionDate()

			expected := map[string][]*ProfileMetric{
				"2019-01-01": {pm1},
				"2019-01-02": {pm2},
			}

			assert.Equal(t, expected, result)
		})
	})
}
