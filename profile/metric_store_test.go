package profile

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/odpf/predator/protocol/metric"
	"github.com/stretchr/testify/assert"
)

func GetMockDB() (*gorm.DB, func()) {
	db, _ := gorm.Open("sqlite3", ":memory:")

	metricRecordTable := new(metricRecord)

	clearDB := func() {
		db.Close()
		db.DropTableIfExists(metricRecordTable)
	}
	if exists := db.HasTable(metricRecordTable); !exists {
		db.CreateTable(metricRecordTable)
	}

	return db, clearDB
}

func TestMetricStore(t *testing.T) {
	t.Run("Store", func(t *testing.T) {
		t.Run("should store metric", func(t *testing.T) {
			db, clear := GetMockDB()
			defer clear()

			profileID := "profile-abcd"
			tableID := "project.dataset.table"
			eventTimestamp := time.Now().In(time.UTC)

			profile := &job.Profile{
				ID:             profileID,
				URN:            tableID,
				EventTimestamp: eventTimestamp,
			}

			uniqueFieldMetadata := map[string]interface{}{
				metric.UniqueFields: []string{"unique_field"},
			}

			metrics := []*metric.Metric{
				{
					ID:         "1",
					FieldID:    "sample_field",
					Type:       metric.Count,
					Category:   metric.Basic,
					Owner:      metric.Field,
					GroupValue: "2020-01-01",
					Value:      3000,
					Timestamp:  eventTimestamp,
				},
				{
					ID:         "2",
					Type:       metric.UniqueCount,
					Category:   metric.Basic,
					Owner:      metric.Table,
					GroupValue: "2020-01-01",
					Value:      3000,
					Timestamp:  eventTimestamp,
					Metadata:   uniqueFieldMetadata,
				},
				{
					ID:         "3",
					FieldID:    "sample_field",
					Type:       metric.InvalidCount,
					Category:   metric.Basic,
					Owner:      metric.Field,
					GroupValue: "2020-01-01",
					Value:      100,
					Condition:  "sample_field < 0",
					Timestamp:  eventTimestamp,
				},
			}

			uniqueFieldMetadataInBytes, err := json.Marshal(uniqueFieldMetadata)
			assert.Nil(t, err)

			expected := []*metricRecord{
				{
					ID:          "1",
					ProfileID:   profileID,
					GroupValue:  "2020-01-01",
					FieldID:     "sample_field",
					OwnerType:   metric.Field,
					MetricName:  metric.Count,
					MetricValue: 3000,
					Category:    metric.Basic,
					CreatedAt:   eventTimestamp,
				},
				{
					ID:          "2",
					ProfileID:   profileID,
					GroupValue:  "2020-01-01",
					OwnerType:   metric.Table,
					MetricName:  metric.UniqueCount,
					MetricValue: 3000,
					Category:    metric.Basic,
					Metadata:    uniqueFieldMetadataInBytes,
					CreatedAt:   eventTimestamp,
				},
				{
					ID:          "3",
					ProfileID:   profileID,
					GroupValue:  "2020-01-01",
					FieldID:     "sample_field",
					OwnerType:   metric.Field,
					MetricName:  metric.InvalidCount,
					MetricValue: 100,
					Category:    metric.Basic,
					Condition:   "sample_field < 0",
					CreatedAt:   eventTimestamp,
				},
			}

			store := NewMetricStore(db, "metric_records")
			err = store.Store(profile, metrics)

			var result []*metricRecord
			db.Find(&result)

			assert.Equal(t, expected, result)
			assert.Nil(t, err)
		})
		t.Run("should not return error when insert zero", func(t *testing.T) {
			db, clear := GetMockDB()
			defer clear()

			profileID := "profile-abcd"
			tableID := "project.dataset.table"
			eventTimestamp := time.Now().In(time.UTC)

			profile := &job.Profile{
				ID:             profileID,
				URN:            tableID,
				EventTimestamp: eventTimestamp,
			}

			var metrics []*metric.Metric

			store := NewMetricStore(db, "metric_records")
			err := store.Store(profile, metrics)

			var result []*metricRecord
			db.Find(&result)

			assert.Len(t, result, 0)
			assert.Nil(t, err)
		})
		t.Run("should return error when insert metric failed", func(t *testing.T) {
			db, clear := GetMockDB()
			defer clear()

			profileID := "profile-abcd"
			tableID := "project.dataset.table"
			eventTimestamp := time.Now().In(time.UTC)

			profile := &job.Profile{
				ID:             profileID,
				URN:            tableID,
				EventTimestamp: eventTimestamp,
			}

			metrics := []*metric.Metric{
				{
					ID:        "1",
					FieldID:   "sample_field",
					Type:      metric.Count,
					Category:  metric.Basic,
					Owner:     metric.Field,
					Value:     3000,
					Timestamp: eventTimestamp,
				},
			}

			store := NewMetricStore(db, "other_table")
			err := store.Store(profile, metrics)

			assert.Error(t, err)
		})
	})
	t.Run("GetMetricsByProfileID", func(t *testing.T) {
		t.Run("should store metric", func(t *testing.T) {
			db, clear := GetMockDB()
			defer clear()

			profileID := "profile-abcd"
			tableID := "project.dataset.table"
			eventTimestamp := time.Now().In(time.UTC)

			profile := &job.Profile{
				ID:             profileID,
				URN:            tableID,
				EventTimestamp: eventTimestamp,
			}

			metrics := []*metric.Metric{
				{
					ID:        "1",
					FieldID:   "sample_field",
					Type:      metric.Count,
					Category:  metric.Basic,
					Owner:     metric.Field,
					Value:     3000,
					Timestamp: eventTimestamp,
				},
				{
					ID:        "2",
					Type:      metric.UniqueCount,
					Category:  metric.Basic,
					Owner:     metric.Table,
					Value:     3000,
					Timestamp: eventTimestamp,
				},
				{
					ID:        "3",
					FieldID:   "sample_field",
					Type:      metric.InvalidCount,
					Category:  metric.Basic,
					Owner:     metric.Field,
					Value:     100,
					Condition: "sample_field < 0",
					Timestamp: eventTimestamp,
				},
			}

			store := NewMetricStore(db, "metric_records")
			err := store.Store(profile, metrics)

			result, err := store.GetMetricsByProfileID(profileID)

			assert.Equal(t, metrics, result)
			assert.Nil(t, err)
		})
		t.Run("should return error when db failed", func(t *testing.T) {
			db, clear := GetMockDB()
			defer clear()

			profileID := "profile-abcd"

			store := NewMetricStore(db, "other table")

			result, err := store.GetMetricsByProfileID(profileID)

			assert.Error(t, err)
			assert.Nil(t, result)
		})
		t.Run("should return protocol.ErrNoProfileMetricFound when no metric available", func(t *testing.T) {
			db, clear := GetMockDB()
			defer clear()

			profileID := "profile-abcd"

			store := NewMetricStore(db, "metric_records")

			result, err := store.GetMetricsByProfileID(profileID)

			assert.Equal(t, protocol.ErrNoProfileMetricFound, err)
			assert.Nil(t, result)
		})
	})
}
