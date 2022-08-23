package tolerance

import (
	"github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEntityStore(t *testing.T) {
	entity := &protocol.Entity{
		GcpProjectIDs: []string{
			"entity-1-sample-project-1",
			"entity-1-sample-project-2",
		},
	}

	t.Run("Create", func(t *testing.T) {
		t.Run("should create entity", func(t *testing.T) {
			spec := &protocol.ToleranceSpec{
				URN: "entity-1-sample-project-1.a.b",
			}
			source := mock.NewToleranceStore()
			defer source.AssertExpectations(t)

			source.On("Create", spec).Return(nil)

			store := NewEntityBasedStore(entity, source)
			err := store.Create(spec)
			assert.Nil(t, err)
		})
		t.Run("should return error when spec tableID is not belong to the entity", func(t *testing.T) {
			spec := &protocol.ToleranceSpec{
				URN: "entity-2-sample-project-1.a.b",
			}
			source := mock.NewToleranceStore()
			defer source.AssertExpectations(t)

			store := NewEntityBasedStore(entity, source)
			err := store.Create(spec)
			assert.Error(t, err)
		})
	})
	t.Run("GetByTableID", func(t *testing.T) {
		t.Run("should return spec", func(t *testing.T) {
			tableID := "entity-1-sample-project-1.a.b"
			spec := &protocol.ToleranceSpec{
				URN: tableID,
			}
			source := mock.NewToleranceStore()
			defer source.AssertExpectations(t)

			source.On("GetByTableID", tableID).Return(spec, nil)

			store := NewEntityBasedStore(entity, source)
			result, err := store.GetByTableID(tableID)
			assert.Nil(t, err)
			assert.Equal(t, spec, result)
		})
		t.Run("should return error when spec tableID is not belong to the entity", func(t *testing.T) {
			source := mock.NewToleranceStore()
			defer source.AssertExpectations(t)

			store := NewEntityBasedStore(entity, source)
			_, err := store.GetByTableID("entity-2.a.c")
			assert.Error(t, err)
		})
	})
	t.Run("Delete", func(t *testing.T) {
		t.Run("should remove spec", func(t *testing.T) {
			tableID := "entity-1-sample-project-1.a.b"

			source := mock.NewToleranceStore()
			defer source.AssertExpectations(t)

			source.On("Delete", tableID).Return(nil)

			store := NewEntityBasedStore(entity, source)
			err := store.Delete(tableID)
			assert.Nil(t, err)
		})
		t.Run("should return error when spec tableID is not belong to the entity", func(t *testing.T) {
			source := mock.NewToleranceStore()
			defer source.AssertExpectations(t)

			store := NewEntityBasedStore(entity, source)
			err := store.Delete("unknown-project.a.c")
			assert.Error(t, err)
		})
	})
	t.Run("GetAll", func(t *testing.T) {
		t.Run("should return all spec belong to entity", func(t *testing.T) {
			allSpecs := []*protocol.ToleranceSpec{
				{
					URN: "entity-1-sample-project-1.a.b",
				},
				{
					URN: "entity-2.a.b",
				},
			}
			entitySpecs := []*protocol.ToleranceSpec{
				{
					URN: "entity-1-sample-project-1.a.b",
				},
			}

			source := mock.NewToleranceStore()
			defer source.AssertExpectations(t)

			source.On("GetAll").Return(allSpecs, nil)

			store := NewEntityBasedStore(entity, source)
			result, err := store.GetAll()
			assert.Nil(t, err)
			assert.Equal(t, entitySpecs, result)
		})
	})
	t.Run("GetByProjectID", func(t *testing.T) {
		t.Run("should return all spec belong to the projectID", func(t *testing.T) {
			specs := []*protocol.ToleranceSpec{
				{
					URN: "entity-1-sample-project-1.a.b",
				},
			}

			source := mock.NewToleranceStore()
			defer source.AssertExpectations(t)

			source.On("GetByProjectID", "entity-1-sample-project-1").Return(specs, nil)

			store := NewEntityBasedStore(entity, source)
			result, err := store.GetByProjectID("entity-1-sample-project-1")
			assert.Nil(t, err)
			assert.Equal(t, specs, result)
		})
		t.Run("should return error when projectID is not belong to the entity", func(t *testing.T) {
			source := mock.NewToleranceStore()
			defer source.AssertExpectations(t)

			store := NewEntityBasedStore(entity, source)
			_, err := store.GetByProjectID("entity-2-sample-project-1")
			assert.Error(t, err)
		})
	})
	t.Run("GetResourceNames", func(t *testing.T) {
		t.Run("should return all spec belong to the entity", func(t *testing.T) {
			resourceNames := []string{
				"entity-1-sample-project-1.a.b",
				"entity-2.a.b",
			}
			entityResources := []string{
				"entity-1-sample-project-1.a.b",
			}

			source := mock.NewToleranceStore()
			defer source.AssertExpectations(t)

			source.On("GetResourceNames").Return(resourceNames, nil)

			store := NewEntityBasedStore(entity, source)
			result, err := store.GetResourceNames()
			assert.Nil(t, err)
			assert.Equal(t, entityResources, result)
		})
	})
}
