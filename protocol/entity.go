package protocol

import (
	"errors"
	"time"
)

//Entity is information about an entity
type Entity struct {
	ID            string
	Name          string
	Environment   string
	GitURL        string
	GcpProjectIDs []string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

var ErrEntityNotFound = errors.New("entity not found")

//EntityStore is storage for Entity
type EntityStore interface {
	Save(entity *Entity) (*Entity, error)
	Create(entity *Entity) (*Entity, error)
	Get(ID string) (*Entity, error)
	GetEntityByGitURL(gitURL string) (*Entity, error)
	GetEntityByProjectID(gcpProjectID string) (*Entity, error)
	GetAll() ([]*Entity, error)
	Update(entity *Entity) (*Entity, error)
}

type EntityFinder []*Entity

func (e EntityFinder) FindByProjectID(projectID string) (*Entity, error) {
	for _, ent := range e {
		for _, ID := range ent.GcpProjectIDs {
			if ID == projectID {
				return ent, nil
			}
		}
	}

	return nil, ErrEntityNotFound
}
