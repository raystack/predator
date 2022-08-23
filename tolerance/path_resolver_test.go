package tolerance

import (
	"fmt"
	predatormock "github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPathResolverFactory(t *testing.T) {
	t.Run("Create", func(t *testing.T) {
		t.Run("should create default path resolver", func(t *testing.T) {
			factory := NewPathResolverFactory(nil)
			resolver := factory.CreateResolver(protocol.Default)

			assert.IsType(t, &DefaultPathResolver{}, resolver)
		})
		t.Run("should create multi tenancy path resolver", func(t *testing.T) {
			factory := NewPathResolverFactory(nil)
			resolver := factory.CreateResolver(protocol.MultiTenancy)

			assert.IsType(t, &MultiTenancyPathResolver{}, resolver)
		})
		t.Run("should create git path resolver", func(t *testing.T) {
			factory := NewPathResolverFactory(nil)
			resolver := factory.CreateResolver(protocol.Git)

			assert.IsType(t, &GitPathResolver{}, resolver)
		})
	})
}

func TestDefaultPathResolver(t *testing.T) {
	t.Run("GetPath", func(t *testing.T) {
		t.Run("should return filepath", func(t *testing.T) {
			tableID := "entity-1-project-1.dataset_a.table_x"
			filePath := fmt.Sprintf("%s.yaml", tableID)

			resolver := &DefaultPathResolver{}
			result, err := resolver.GetPath(tableID)

			assert.Nil(t, err)
			assert.Equal(t, filePath, result)
		})
		t.Run("should error when path format is wrong filepath", func(t *testing.T) {
			tableID := "entity-1-project-1.dataset_a"

			resolver := &DefaultPathResolver{}
			_, err := resolver.GetPath(tableID)

			assert.NotNil(t, err)
		})
	})
}

func TestMultiTenancyPathResolver(t *testing.T) {
	t.Run("GetPath", func(t *testing.T) {
		t.Run("should return filepath", func(t *testing.T) {
			tableID := "entity-1-project-1.dataset_a.table_x"
			label := &protocol.Label{
				Project: "entity-1-project-1",
				Dataset: "dataset_a",
				Table:   "table_x",
			}

			entity := &protocol.Entity{
				ID:            "entity-1",
				Name:          "entity-1-name",
				Environment:   "sample-env",
				GitURL:        "git@sample-url:entity-1.git",
				GcpProjectIDs: []string{"entity-1-project-1"},
			}

			resolvedPath := fmt.Sprintf("%s/%s/%s/%s/%s.yaml", entity.Environment, entity.Name, label.Project, label.Dataset, label.Table)

			store := predatormock.NewEntityStore()
			defer store.AssertExpectations(t)

			store.On("GetAll").Return([]*protocol.Entity{entity}, nil)

			resolver := &MultiTenancyPathResolver{entityStore: store}
			result, _ := resolver.GetPath(tableID)

			assert.Equal(t, resolvedPath, result)
		})
	})
	t.Run("GetURN", func(t *testing.T) {
		t.Run("should return urn", func(t *testing.T) {
			tableID := "entity-1-project-1.dataset_a.table_x"
			label := &protocol.Label{
				Project: "entity-1-project-1",
				Dataset: "dataset_a",
				Table:   "table_x",
			}

			entity := &protocol.Entity{
				ID:            "entity-1",
				Name:          "entity-1-name",
				Environment:   "sample-env",
				GitURL:        "git@sample-url:entity-1.git",
				GcpProjectIDs: []string{"entity-1-project-1"},
			}

			resolvedPath := fmt.Sprintf("%s/%s/%s/%s/%s.yaml", entity.Environment, entity.Name, label.Project, label.Dataset, label.Table)

			store := predatormock.NewEntityStore()
			defer store.AssertExpectations(t)

			store.On("GetAll").Return([]*protocol.Entity{entity}, nil)

			resolver := &MultiTenancyPathResolver{entityStore: store}
			result, _ := resolver.GetPath(tableID)

			assert.Equal(t, resolvedPath, result)
		})
	})
}

func TestGitPathResolver(t *testing.T) {
	t.Run("GetPath", func(t *testing.T) {
		t.Run("should return filepath", func(t *testing.T) {
			tableID := "entity-1-project-1.dataset_a.table_x"
			filePath := "entity-1-project-1/dataset_a/table_x.yaml"

			resolver := &GitPathResolver{}
			result, err := resolver.GetPath(tableID)

			assert.Nil(t, err)
			assert.Equal(t, filePath, result)
		})
		t.Run("should error when path format is wrong filepath", func(t *testing.T) {
			tableID := "entity-1-project-1.acbd"

			resolver := &GitPathResolver{}
			_, err := resolver.GetPath(tableID)

			assert.NotNil(t, err)
		})
	})
	t.Run("GetURN", func(t *testing.T) {
		t.Run("should return urn", func(t *testing.T) {
			filePath := "entity-1-project-1/dataset_a/table_x.yaml"
			tableID := "entity-1-project-1.dataset_a.table_x"

			resolver := &GitPathResolver{}
			urn, err := resolver.GetURN(filePath)

			assert.Nil(t, err)
			assert.Equal(t, tableID, urn)
		})
		t.Run("should error when path format is wrong filepath", func(t *testing.T) {
			filePath := "entity-1-project-1/dataset_a"

			resolver := &GitPathResolver{}
			_, err := resolver.GetURN(filePath)

			assert.NotNil(t, err)
		})
	})
}
