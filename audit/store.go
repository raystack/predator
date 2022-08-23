package audit

import (
	"time"

	"github.com/jinzhu/gorm"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
)

type audit struct {
	ID             string
	ProfileID      string
	TotalRecords   int64
	EventTimestamp time.Time
}

func newAuditFromJob(auditJob *job.Audit) *audit {
	return &audit{
		ID:             auditJob.ID,
		ProfileID:      auditJob.ProfileID,
		TotalRecords:   auditJob.TotalRecords,
		EventTimestamp: auditJob.EventTimestamp,
	}
}

//Store as a model for resultstore struct
type Store struct {
	db          *gorm.DB
	statusStore protocol.StatusStore
}

//NewStore to construct result store
func NewStore(db *gorm.DB, tableName string, statusStore protocol.StatusStore) *Store {
	return &Store{
		db:          db.Table(tableName),
		statusStore: statusStore,
	}
}

//CreateAudit is implementation on create audit
func (a *Store) CreateAudit(auditJob *job.Audit) (*job.Audit, error) {
	auditDBModel := newAuditFromJob(auditJob)

	if err := a.db.Create(auditDBModel).Error; err != nil {
		return nil, err
	}

	auditOut := &job.Audit{
		ID:             auditDBModel.ID,
		ProfileID:      auditJob.ProfileID,
		State:          auditJob.State,
		Message:        auditJob.Message,
		Detail:         auditJob.Detail,
		URN:            auditJob.URN,
		EventTimestamp: auditJob.EventTimestamp,
		TotalRecords:   auditJob.TotalRecords,
	}

	status := &protocol.Status{
		JobID:          auditOut.ID,
		JobType:        job.TypeAudit,
		Message:        auditOut.Message,
		Status:         string(job.StateCreated),
		EventTimestamp: time.Now().In(time.UTC),
	}

	if err := a.statusStore.Store(status); err != nil {
		return nil, err
	}
	return auditOut, nil
}

//UpdateAudit is implementation to insert status based on audit event
func (a *Store) UpdateAudit(auditJob *job.Audit) error {
	status := &protocol.Status{
		JobID:          auditJob.ID,
		JobType:        job.TypeAudit,
		Message:        auditJob.Message,
		Status:         string(auditJob.State),
		EventTimestamp: time.Now().In(time.UTC),
	}

	if err := a.statusStore.Store(status); err != nil {
		return err
	}
	return nil
}
