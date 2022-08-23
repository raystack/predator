package profile

import (
	"errors"
	"github.com/odpf/predator/util"
	"time"

	"github.com/jinzhu/gorm"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
)

type profileRecord struct {
	ID             string `gorm:"primary_key"`
	URN            string `gorm:"not null"`
	GroupName      string
	Filter         string
	Mode           string
	TotalRecords   int64
	AuditTime      time.Time
	EventTimestamp time.Time `gorm:"not null"`
}

func newProfileRecord(prof *job.Profile) *profileRecord {
	return &profileRecord{
		ID:             prof.ID,
		URN:            prof.URN,
		GroupName:      prof.GroupName,
		Filter:         prof.Filter,
		Mode:           prof.Mode.String(),
		TotalRecords:   prof.TotalRecords,
		AuditTime:      prof.AuditTimestamp,
		EventTimestamp: prof.EventTimestamp,
	}
}

func (p *profileRecord) toProfile(status *protocol.Status) *job.Profile {
	return &job.Profile{
		ID:               p.ID,
		EventTimestamp:   p.EventTimestamp,
		Status:           job.State(status.Status),
		Message:          status.Message,
		URN:              p.URN,
		GroupName:        p.GroupName,
		Filter:           p.Filter,
		Mode:             job.Mode(p.Mode),
		TotalRecords:     p.TotalRecords,
		AuditTimestamp:   p.AuditTime,
		UpdatedTimestamp: status.EventTimestamp,
	}
}

//Store as profile store
type Store struct {
	db          *gorm.DB
	statusStore protocol.StatusStore
}

//NewStore to construct status store
func NewStore(db *gorm.DB, tableName string, statusStore protocol.StatusStore) protocol.ProfileStore {
	return &Store{
		db:          db.Table(tableName),
		statusStore: statusStore,
	}
}

//Create to store created profile
func (s *Store) Create(profile *job.Profile) (*job.Profile, error) {
	storedProfile := newProfileRecord(profile)

	handler := s.db.Create(storedProfile)
	if err := handler.Error; err != nil {
		return nil, err
	}

	status := &protocol.Status{
		JobID:   storedProfile.ID,
		JobType: job.TypeProfile,
		Message: profile.Message,
		Status:  job.StateCreated.String(),
	}
	if err := s.statusStore.Store(status); err != nil {
		return nil, err
	}

	newProfile := storedProfile.toProfile(status)

	return newProfile, nil
}

//Update only insert new status of profile, all field in profile is immutable
func (s *Store) Update(profile *job.Profile) error {
	status := &protocol.Status{
		JobID:   profile.ID,
		JobType: job.TypeProfile,
		Message: profile.Message,
		Status:  profile.Status.String(),
	}

	storedProfile := newProfileRecord(profile)

	handler := s.db.Model(storedProfile).Update("total_records", storedProfile.TotalRecords)
	if err := handler.Error; err != nil {
		return err
	}

	if err := s.statusStore.Store(status); err != nil {
		return err
	}

	profile.UpdatedTimestamp = status.EventTimestamp
	return nil
}

//Get to get profile
func (s *Store) Get(ID string) (*job.Profile, error) {
	if !util.IsUUIDValid(ID) {
		return nil, errors.New("invalid ID")
	}

	var p profileRecord
	handle := s.db.First(&p, "id = ?", ID)

	if err := handle.Error; err != nil {
		if gorm.IsRecordNotFoundError(err) {
			return nil, protocol.ErrProfileNotFound
		}
		return nil, err
	}

	status, err := s.statusStore.GetLatestStatusByIDandType(ID, job.TypeProfile)
	if err != nil {
		if err == protocol.ErrStatusNotFound {
			return nil, protocol.ErrProfileInvalid
		}
		return nil, err
	}

	return p.toProfile(status), nil
}
