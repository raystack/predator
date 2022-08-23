package status

import (
	"github.com/jinzhu/gorm"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"time"
)

type statusRecord struct {
	ID        string
	JobID     string
	JobType   string
	Status    string
	Message   string
	CreatedAt time.Time
}

func newStatusRecord(status *protocol.Status) *statusRecord {
	return &statusRecord{
		ID:        status.ID,
		JobID:     status.JobID,
		JobType:   status.JobType.String(),
		Status:    status.Status,
		Message:   status.Message,
		CreatedAt: status.EventTimestamp,
	}
}

func (s *statusRecord) toStatus() *protocol.Status {
	return &protocol.Status{
		ID:             s.ID,
		JobID:          s.JobID,
		JobType:        job.Type(s.JobType),
		Status:         s.Status,
		Message:        s.Message,
		EventTimestamp: s.CreatedAt,
	}
}

//Store to store status
type Store struct {
	db *gorm.DB
}

//NewStore to construct status store
func NewStore(db *gorm.DB, tableName string) protocol.StatusStore {
	return &Store{db.Table(tableName)}
}

//Store to store status of profile or audit
func (s *Store) Store(state *protocol.Status) error {
	record := newStatusRecord(state)
	handler := s.db.Create(record)
	if err := handler.Error; err != nil {
		return err
	}

	state.EventTimestamp = record.CreatedAt
	return nil
}

//GetLatestStatusByIDandType to get latest status of profile or audit by job Partition and job Type
func (s *Store) GetLatestStatusByIDandType(jobID string, jobType job.Type) (*protocol.Status, error) {
	var stateRecord statusRecord

	query := s.db.Where("job_id = ? and job_type = ?", jobID, jobType).Order("created_at desc", true).Take(&stateRecord)

	handler := query.First(&stateRecord)
	if err := handler.Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, protocol.ErrStatusNotFound
		}
		return nil, err
	}

	return stateRecord.toStatus(), nil
}

//GetStatusLogByIDandType to get status history of profile or audit by job Partition and job Type
func (s *Store) GetStatusLogByIDandType(jobID string, jobType job.Type) ([]*protocol.Status, error) {
	var stateRecords []*statusRecord

	query := s.db.
		Where("job_id = ? and job_type = ?", jobID, jobType).
		Order("created_at desc", true).
		Take(&stateRecords)

	handler := query.Find(&stateRecords)
	if err := handler.Error; err != nil {
		return nil, err
	}
	if len(stateRecords) == 0 {
		return nil, protocol.ErrStatusNotFound
	}

	var status []*protocol.Status
	for _, sr := range stateRecords {
		s := sr.toStatus()

		status = append(status, s)
	}

	return status, nil
}
