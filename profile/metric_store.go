package profile

import (
	"encoding/json"
	"time"

	"github.com/jinzhu/gorm"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/odpf/predator/protocol/metric"
	"gorm.io/datatypes"
)

type metricRecord struct {
	ID          string
	ProfileID   string
	GroupValue  string
	FieldID     string
	OwnerType   metric.Owner
	MetricName  metric.Type
	MetricValue float64
	Condition   string
	Category    metric.Category
	Metadata    datatypes.JSON
	CreatedAt   time.Time
}

func newMetricRecord(profile *job.Profile, metric *metric.Metric) (*metricRecord, error) {
	var metadataInBytes []byte
	if metric.Metadata != nil {
		var err error
		metadataInBytes, err = json.Marshal(metric.Metadata)
		if err != nil {
			return nil, err
		}
	}

	return &metricRecord{
		ID:          metric.ID,
		ProfileID:   profile.ID,
		GroupValue:  metric.GroupValue,
		FieldID:     metric.FieldID,
		OwnerType:   metric.Owner,
		MetricName:  metric.Type,
		MetricValue: metric.Value,
		Condition:   metric.Condition,
		Category:    metric.Category,
		Metadata:    metadataInBytes,
		CreatedAt:   metric.Timestamp,
	}, nil
}

func (m *metricRecord) toMetric() (*metric.Metric, error) {
	var metadata map[string]interface{}
	if m.Metadata != nil {
		if err := json.Unmarshal(m.Metadata, &metadata); err != nil {
			return nil, err
		}
	}

	return &metric.Metric{
		ID:         m.ID,
		FieldID:    m.FieldID,
		Type:       m.MetricName,
		Category:   m.Category,
		Owner:      m.OwnerType,
		GroupValue: m.GroupValue,
		Value:      m.MetricValue,
		Condition:  m.Condition,
		Metadata:   metadata,
		Timestamp:  m.CreatedAt,
	}, nil
}

type MetricStore struct {
	db *gorm.DB
}

//NewMetricStore is constructor of MetricStore
func NewMetricStore(db *gorm.DB, tableName string) *MetricStore {
	return &MetricStore{
		db: db.Table(tableName),
	}
}

func (m *MetricStore) Store(profile *job.Profile, metrics []*metric.Metric) error {
	var records []*metricRecord
	for _, mt := range metrics {
		rec, err := newMetricRecord(profile, mt)
		if err != nil {
			return err
		}
		records = append(records, rec)
	}

	for _, rec := range records {
		err := m.db.Create(rec).Error
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *MetricStore) GetMetricsByProfileID(ID string) ([]*metric.Metric, error) {
	var records []*metricRecord
	handler := m.db.Where("profile_id = ?", ID).Find(&records)

	if err := handler.Error; err != nil {
		if handler.RecordNotFound() {
			return nil, protocol.ErrNoProfileMetricFound
		}
		return nil, err
	}

	if len(records) == 0 {
		return nil, protocol.ErrNoProfileMetricFound
	}

	var metrics []*metric.Metric
	for _, rec := range records {
		mt, err := rec.toMetric()
		if err != nil {
			return nil, err
		}
		metrics = append(metrics, mt)
	}

	return metrics, nil
}
