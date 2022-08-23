package audit

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/odpf/predator/protocol/metric"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	"github.com/odpf/predator/protocol"
	"github.com/stretchr/testify/assert"
)

func getMockDB() (*gorm.DB, func()) {
	db, _ := gorm.Open("sqlite3", ":memory:")

	a := new(Report)

	clearDB := func() {
		db.Close()
		db.DropTableIfExists(a)
	}
	if exists := db.HasTable(a); !exists {
		db.CreateTable(a)
	}

	return db, clearDB
}

func TestResultStore(t *testing.T) {
	t.Run("StoreResults", func(t *testing.T) {
		t.Run("should store audit results", func(t *testing.T) {
			db, clear := getMockDB()
			defer clear()

			currentTime := time.Now().In(time.UTC)

			auditReports := []*protocol.AuditReport{
				{
					AuditID:     "abc",
					TableURN:    "project.dataset.table",
					GroupValue:  "2020-01-01",
					MetricName:  "duplication_pct",
					MetricValue: 0.1,
					ToleranceRules: []protocol.ToleranceRule{
						{
							Comparator: protocol.ComparatorMoreThanEq,
							Value:      1.0,
						},
					},
					PassFlag:       true,
					EventTimestamp: currentTime,
				},
				{
					AuditID:     "abc",
					TableURN:    "project.dataset.table",
					GroupValue:  "2020-01-01",
					MetricName:  "row_count",
					MetricValue: 100.0,
					ToleranceRules: []protocol.ToleranceRule{
						{
							Comparator: protocol.ComparatorLessThanEq,
							Value:      1.0,
						},
					},
					PassFlag:       true,
					EventTimestamp: currentTime,
				},
			}

			store := NewResultStore(db, "reports")
			err := store.StoreResults(auditReports)

			var reports []*Report
			db.Find(&reports)

			expected := []*Report{
				{
					AuditID:        "abc",
					GroupValue:     "2020-01-01",
					MetricName:     "duplication_pct",
					MetricValue:    0.1,
					PassFlag:       true,
					CreatedAt:      currentTime,
					ToleranceRules: "[{\"comparator\":\"more_than_eq\",\"value\":1}]",
				},
				{
					AuditID:        "abc",
					GroupValue:     "2020-01-01",
					MetricName:     "row_count",
					MetricValue:    100.0,
					PassFlag:       true,
					CreatedAt:      currentTime,
					ToleranceRules: "[{\"comparator\":\"less_than_eq\",\"value\":1}]",
				},
			}

			assert.Equal(t, expected, reports)
			assert.Nil(t, err)
		})
		t.Run("should store audit results given zero results", func(t *testing.T) {
			db, clear := getMockDB()
			defer clear()

			var auditReports []*protocol.AuditReport

			store := NewResultStore(db, "reports")
			err := store.StoreResults(auditReports)

			var reports []*Report
			db.Find(&reports)

			assert.Len(t, reports, 0)
			assert.Nil(t, err)
		})
		t.Run("should store audit results with metric metadata defined", func(t *testing.T) {
			db, clear := getMockDB()
			defer clear()

			currentTime := time.Now().In(time.UTC)

			uniqueFieldsMetadata := map[string]interface{}{
				metric.UniqueFields: []string{"sample_field"},
			}

			auditReports := []*protocol.AuditReport{
				{
					AuditID:     "abc",
					TableURN:    "project.dataset.table",
					GroupValue:  "2020-01-01",
					MetricName:  "duplication_pct",
					MetricValue: 0.1,
					ToleranceRules: []protocol.ToleranceRule{
						{
							Comparator: protocol.ComparatorMoreThanEq,
							Value:      1.0,
						},
					},
					Metadata:       uniqueFieldsMetadata,
					PassFlag:       true,
					EventTimestamp: currentTime,
				},
				{
					AuditID:     "abc",
					TableURN:    "project.dataset.table",
					GroupValue:  "2020-01-01",
					MetricName:  "row_count",
					MetricValue: 100.0,
					ToleranceRules: []protocol.ToleranceRule{
						{
							Comparator: protocol.ComparatorLessThanEq,
							Value:      1.0,
						},
					},
					PassFlag:       true,
					EventTimestamp: currentTime,
				},
			}

			store := NewResultStore(db, "reports")
			err := store.StoreResults(auditReports)

			var reports []*Report
			db.Find(&reports)

			metadataInBytes, err := json.Marshal(uniqueFieldsMetadata)
			assert.Nil(t, err)

			expected := []*Report{
				{
					AuditID:        "abc",
					GroupValue:     "2020-01-01",
					MetricName:     "duplication_pct",
					MetricValue:    0.1,
					PassFlag:       true,
					CreatedAt:      currentTime,
					ToleranceRules: "[{\"comparator\":\"more_than_eq\",\"value\":1}]",
					Metadata:       metadataInBytes,
				},
				{
					AuditID:        "abc",
					GroupValue:     "2020-01-01",
					MetricName:     "row_count",
					MetricValue:    100.0,
					PassFlag:       true,
					CreatedAt:      currentTime,
					ToleranceRules: "[{\"comparator\":\"less_than_eq\",\"value\":1}]",
				},
			}

			assert.Equal(t, expected, reports)
			assert.Nil(t, err)
		})
	})
}
