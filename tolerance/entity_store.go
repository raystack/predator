package tolerance

import (
	"fmt"
	"github.com/odpf/predator/protocol"
)

type EntityBasedStore struct {
	entity       *protocol.Entity
	source       protocol.ToleranceStore
	projectIDMap map[string]struct{}
}

func NewEntityBasedStore(entity *protocol.Entity, source protocol.ToleranceStore) *EntityBasedStore {
	projectIDMap := make(map[string]struct{})

	for _, projectID := range entity.GcpProjectIDs {
		projectIDMap[projectID] = struct{}{}
	}

	return &EntityBasedStore{
		entity:       entity,
		source:       source,
		projectIDMap: projectIDMap,
	}
}

func (e *EntityBasedStore) Create(spec *protocol.ToleranceSpec) error {
	label, err := protocol.ParseLabel(spec.URN)
	if err != nil {
		return err
	}
	if e.isBelongToEntity(label.Project) {
		return e.source.Create(spec)
	}

	return fmt.Errorf("%s table doesnt belong to %s entity", spec.URN, e.entity.ID)
}

func (e *EntityBasedStore) GetByTableID(tableID string) (*protocol.ToleranceSpec, error) {
	label, err := protocol.ParseLabel(tableID)
	if err != nil {
		return nil, err
	}
	if e.isBelongToEntity(label.Project) {
		return e.source.GetByTableID(tableID)
	}

	return nil, fmt.Errorf("%s table doesnt belong to %s entity", tableID, e.entity.ID)
}

func (e *EntityBasedStore) Delete(tableID string) error {
	label, err := protocol.ParseLabel(tableID)
	if err != nil {
		return err
	}
	if e.isBelongToEntity(label.Project) {
		return e.source.Delete(tableID)
	}

	return fmt.Errorf("%s table doesnt belong to %s entity", tableID, e.entity.ID)
}

func (e *EntityBasedStore) GetAll() ([]*protocol.ToleranceSpec, error) {
	specs, err := e.source.GetAll()
	if err != nil {
		return nil, err
	}

	var selectedSpecs []*protocol.ToleranceSpec

	for _, spec := range specs {
		label, err := protocol.ParseLabel(spec.URN)
		if err != nil {
			return nil, err
		}

		if e.isBelongToEntity(label.Project) {
			selectedSpecs = append(selectedSpecs, spec)
		}
	}

	return selectedSpecs, nil
}

func (e *EntityBasedStore) GetByProjectID(projectID string) ([]*protocol.ToleranceSpec, error) {
	if e.isBelongToEntity(projectID) {
		return e.source.GetByProjectID(projectID)
	}

	return nil, fmt.Errorf("%s project doesnt belong to %s entity", projectID, e.entity.ID)
}

func (e *EntityBasedStore) GetResourceNames() ([]string, error) {
	var sourceURNs []string
	urns, err := e.source.GetResourceNames()
	if err != nil {
		return nil, fmt.Errorf("failed to get resources from repository\n%w", err)
	}

	for _, urn := range urns {
		label, err := protocol.ParseLabel(urn)
		if err != nil {
			return nil, err
		}

		if e.isBelongToEntity(label.Project) {
			sourceURNs = append(sourceURNs, urn)
		}
	}

	return sourceURNs, nil
}

func (e *EntityBasedStore) isBelongToEntity(projectID string) bool {
	_, ok := e.projectIDMap[projectID]
	return ok
}
