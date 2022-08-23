package audit

import (
	"encoding/json"
	"gorm.io/datatypes"
	"time"

	"github.com/jinzhu/gorm"
	"github.com/odpf/predator/protocol"
)

//Report as a struct to store audit result to DB
type Report struct {
	AuditID        string
	GroupValue     string
	FieldID        string
	MetricName     string
	MetricValue    float64
	Condition      string
	Metadata       datatypes.JSON
	ToleranceRules string
	PassFlag       bool
	CreatedAt      time.Time
}

//ResultStore as a model for resultstore struct
type ResultStore struct {
	db *gorm.DB
}

//NewResultStore to construct result store
func NewResultStore(db *gorm.DB, tableName string) *ResultStore {
	return &ResultStore{
		db: db.Table(tableName),
	}
}

//StoreResults to store auditing result
func (rs *ResultStore) StoreResults(results []*protocol.AuditReport) error {
	storedResults, err := convertToStored(results)
	if err != nil {
		return err
	}

	for _, rec := range storedResults {
		if err := rs.db.Create(rec).Error; err != nil {
			return err
		}
	}

	return nil
}

func convertToStored(auditReports []*protocol.AuditReport) ([]Report, error) {
	var auditResults []Report
	for _, r := range auditReports {
		content, err := json.Marshal(r.ToleranceRules)
		if err != nil {
			return nil, err
		}

		var metadataInBytes []byte
		if r.Metadata != nil {
			var err error
			metadataInBytes, err = json.Marshal(r.Metadata)
			if err != nil {
				return nil, err
			}
		}

		a := Report{
			AuditID:        r.AuditID,
			GroupValue:     r.GroupValue,
			FieldID:        r.FieldID,
			MetricName:     r.MetricName.String(),
			MetricValue:    r.MetricValue,
			Condition:      r.Condition,
			Metadata:       metadataInBytes,
			ToleranceRules: string(content),
			PassFlag:       r.PassFlag,
			CreatedAt:      r.EventTimestamp,
		}
		auditResults = append(auditResults, a)
	}
	return auditResults, nil
}
