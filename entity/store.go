package entity

import (
	"github.com/jinzhu/gorm"
	"github.com/odpf/predator/protocol"
	"strings"
	"time"
)

type entityRecord struct {
	ID            string
	Name          string
	Environment   string
	GitURL        string
	GcpProjectIDs string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

const projectIdsSeparator = ","

func newRecord(entity *protocol.Entity) *entityRecord {
	projectIds := strings.Join(entity.GcpProjectIDs, projectIdsSeparator)
	return &entityRecord{
		ID:            entity.ID,
		Name:          entity.Name,
		Environment:   entity.Environment,
		GitURL:        entity.GitURL,
		GcpProjectIDs: projectIds,
		CreatedAt:     entity.CreatedAt,
		UpdatedAt:     entity.UpdatedAt,
	}
}

func (e *entityRecord) toEntity() *protocol.Entity {
	var projectIDs []string

	if len(e.GcpProjectIDs) > 0 {
		projectIDs = strings.Split(e.GcpProjectIDs, projectIdsSeparator)
	}

	return &protocol.Entity{
		ID:            e.ID,
		Name:          e.Name,
		Environment:   e.Environment,
		GitURL:        e.GitURL,
		GcpProjectIDs: projectIDs,
		CreatedAt:     e.CreatedAt,
		UpdatedAt:     e.UpdatedAt,
	}
}

type Store struct {
	db *gorm.DB
}

func (s *Store) GetEntityByProjectID(gcpProjectID string) (*protocol.Entity, error) {
	entities, err := s.GetAll()
	if err != nil {
		return nil, err
	}
	finder := protocol.EntityFinder(entities)
	return finder.FindByProjectID(gcpProjectID)
}

func (s *Store) Save(entity *protocol.Entity) (*protocol.Entity, error) {
	_, err := s.Get(entity.ID)
	if err != nil && err != protocol.ErrEntityNotFound {
		return nil, err
	}

	if err == protocol.ErrEntityNotFound {
		return s.Create(entity)
	}
	return s.Update(entity)
}

func (s *Store) GetEntityByGitURL(gitURL string) (*protocol.Entity, error) {
	var record entityRecord

	query := s.db.Where("git_url = ?", gitURL)

	handler := query.First(&record)
	if err := handler.Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, protocol.ErrEntityNotFound
		}
		return nil, err
	}

	return record.toEntity(), nil
}

//NewStore to construct status store
func NewStore(db *gorm.DB, tableName string) *Store {
	return &Store{db.Table(tableName)}
}

func (s *Store) Create(entity *protocol.Entity) (*protocol.Entity, error) {
	record := newRecord(entity)

	handler := s.db.Create(record)
	if err := handler.Error; err != nil {
		return nil, err
	}

	return record.toEntity(), nil
}

func (s *Store) Get(ID string) (*protocol.Entity, error) {
	var record entityRecord

	query := s.db.Where("id = ?", ID)

	handler := query.First(&record)
	if err := handler.Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, protocol.ErrEntityNotFound
		}
		return nil, err
	}

	return record.toEntity(), nil
}

func (s *Store) GetAll() ([]*protocol.Entity, error) {
	var records []entityRecord

	handler := s.db.Find(&records)
	if err := handler.Error; err != nil {
		return nil, err
	}

	var entities []*protocol.Entity
	for _, rec := range records {
		ent := rec.toEntity()
		entities = append(entities, ent)
	}

	return entities, nil
}

func (s *Store) Update(entity *protocol.Entity) (*protocol.Entity, error) {
	record := newRecord(entity)
	handler := s.db.Save(record)
	if err := handler.Error; err != nil {
		return nil, err
	}
	return record.toEntity(), nil
}
