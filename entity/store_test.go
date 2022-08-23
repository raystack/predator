package entity

import (
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	"github.com/odpf/predator/protocol"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func getMockDB() (*gorm.DB, func()) {
	db, _ := gorm.Open("sqlite3", ":memory:")

	entity := new(entityRecord)

	clearDB := func() {
		if err := db.Close(); err != nil {
			return
		}
		db.DropTableIfExists(entity)
	}

	if exists := db.HasTable(entity); !exists {
		db.CreateTable(entity)
	}

	return db, clearDB
}

func removeTime(entity *protocol.Entity) *protocol.Entity {
	entity.UpdatedAt = time.Time{}
	entity.CreatedAt = time.Time{}
	return entity
}

func TestStore(t *testing.T) {
	t.Run("Save", func(t *testing.T) {
		t.Run("should create new entity", func(t *testing.T) {
			entity := &protocol.Entity{
				ID:            "sample-entity-1",
				Name:          "sample-entity-1-name",
				Environment:   "env-a",
				GitURL:        "git@sample-url:sample-entity.go",
				GcpProjectIDs: []string{"sample-entity-1-project-1", "sample-entity-1-project-2"},
			}

			db, clearDB := getMockDB()
			defer clearDB()

			entityStore := NewStore(db, "entity_records")

			_, err := entityStore.Create(entity)

			var record entityRecord
			db.Find(&record)
			result := record.toEntity()

			assert.Nil(t, err)
			assert.Equal(t, entity, removeTime(result))
		})
		t.Run("should update new entity", func(t *testing.T) {
			entity := &protocol.Entity{
				ID:            "sample-entity-1",
				Name:          "sample-entity-1-name",
				Environment:   "env-a",
				GitURL:        "git@sample-url:sample-entity.go",
				GcpProjectIDs: []string{"sample-entity-1-project-1"},
			}

			updatedEntity := &protocol.Entity{
				ID:            "sample-entity-1",
				Name:          "sample-entity-1-name",
				Environment:   "env-a",
				GitURL:        "git@sample-url:sample-entity.go",
				GcpProjectIDs: []string{"sample-entity-1-project-1", "sample-entity-1-project-2"},
			}

			db, clearDB := getMockDB()
			defer clearDB()

			entityStore := NewStore(db, "entity_records")

			_, err := entityStore.Save(entity)
			_, err = entityStore.Save(updatedEntity)

			var record entityRecord
			db.Find(&record)
			result := record.toEntity()

			assert.Nil(t, err)
			assert.Equal(t, updatedEntity, removeTime(result))
		})
		t.Run("should return error when insert failed", func(t *testing.T) {
			entity := &protocol.Entity{
				ID:            "sample-entity-1",
				Name:          "sample-entity-1-name",
				Environment:   "env-a",
				GitURL:        "git@sample-url:sample-entity.go",
				GcpProjectIDs: []string{"sample-entity-1-project-1", "sample-entity-1-project-2"},
			}

			db, clearDB := getMockDB()
			defer clearDB()

			entityStore := NewStore(db, "another_table")

			_, err := entityStore.Save(entity)

			assert.NotNil(t, err)
		})
		t.Run("should return error when update failed", func(t *testing.T) {
			entity := &protocol.Entity{
				ID:            "sample-entity-1",
				Name:          "sample-entity-1-name",
				Environment:   "env-a",
				GitURL:        "git@sample-url:sample-entity.go",
				GcpProjectIDs: []string{"sample-entity-1-project-1"},
			}

			updatedEntity := &protocol.Entity{
				ID:            "sample-entity-1",
				Name:          "sample-entity-1-name",
				Environment:   "env-a",
				GitURL:        "git@sample-url:sample-entity.go",
				GcpProjectIDs: []string{"sample-entity-1-project-1", "sample-entity-1-project-2"},
			}

			db, clearDB := getMockDB()
			defer clearDB()

			entityStore := NewStore(db, "entity_records")

			_, err := entityStore.Save(entity)
			db.Close()
			_, err = entityStore.Save(updatedEntity)

			assert.Error(t, err)
		})
	})
	t.Run("Create", func(t *testing.T) {
		t.Run("should create new entity", func(t *testing.T) {
			entity := &protocol.Entity{
				ID:            "sample-entity-1",
				Name:          "sample-entity-1-name",
				Environment:   "env-a",
				GitURL:        "git@sample-url:sample-entity.go",
				GcpProjectIDs: []string{"sample-entity-1-project-1", "sample-entity-1-project-2"},
			}

			db, clearDB := getMockDB()
			defer clearDB()

			entityStore := NewStore(db, "entity_records")

			_, err := entityStore.Create(entity)

			var record entityRecord
			db.Find(&record)
			result := record.toEntity()

			assert.Nil(t, err)
			assert.Equal(t, entity, removeTime(result))
		})
		t.Run("should return error when insertion failed", func(t *testing.T) {
			entity := &protocol.Entity{
				ID:            "sample-entity-1",
				Name:          "sample-entity-1-name",
				Environment:   "env-a",
				GitURL:        "git@sample-url:sample-entity.go",
				GcpProjectIDs: []string{"sample-entity-1-project-1", "sample-entity-1-project-2"},
			}

			db, clearDB := getMockDB()
			defer clearDB()

			entityStore := NewStore(db, "another_table")

			_, err := entityStore.Create(entity)

			assert.NotNil(t, err)
		})
	})
	t.Run("Get", func(t *testing.T) {
		t.Run("should get entity given entity ID", func(t *testing.T) {
			entity := &protocol.Entity{
				ID:            "sample-entity-1",
				Name:          "sample-entity-1-name",
				Environment:   "env-a",
				GitURL:        "git@sample-url:sample-entity.go",
				GcpProjectIDs: []string{"sample-entity-1-project-1", "sample-entity-1-project-2"},
			}

			db, clearDB := getMockDB()
			defer clearDB()

			entityStore := NewStore(db, "entity_records")

			createdEntity, err := entityStore.Create(entity)
			result, err := entityStore.Get(createdEntity.ID)

			assert.Nil(t, err)
			assert.Equal(t, entity, removeTime(result))
		})
		t.Run("should return not found when entity with ID not found", func(t *testing.T) {
			db, clearDB := getMockDB()
			defer clearDB()

			entityStore := NewStore(db, "entity_records")

			result, err := entityStore.Get("sample-entity-1")

			assert.Nil(t, result)
			assert.Equal(t, protocol.ErrEntityNotFound, err)
		})
		t.Run("should return error when unable to try to retrieve entity", func(t *testing.T) {
			db, clearDB := getMockDB()
			defer clearDB()

			entityStore := NewStore(db, "another_table")

			_, err := entityStore.Get("sample-entity-1")

			assert.NotNil(t, err)
		})
	})
	t.Run("GetEntityByGitURL", func(t *testing.T) {
		t.Run("should get entity given git url", func(t *testing.T) {
			gitURL := "git@sample-url:sample-entity.go"
			entity := &protocol.Entity{
				ID:            "sample-entity-1",
				Name:          "sample-entity-1-name",
				Environment:   "env-a",
				GitURL:        gitURL,
				GcpProjectIDs: []string{"sample-entity-1-project-1", "sample-entity-1-project-2"},
			}

			db, clearDB := getMockDB()
			defer clearDB()

			entityStore := NewStore(db, "entity_records")

			_, err := entityStore.Create(entity)
			result, err := entityStore.GetEntityByGitURL(gitURL)

			assert.Nil(t, err)
			assert.Equal(t, entity, removeTime(result))
		})
		t.Run("should return not found when entity with git url not found", func(t *testing.T) {
			gitURL := "git@sample-url:sample-entity.go"

			db, clearDB := getMockDB()
			defer clearDB()

			entityStore := NewStore(db, "entity_records")

			result, err := entityStore.GetEntityByGitURL(gitURL)

			assert.Nil(t, result)
			assert.Equal(t, protocol.ErrEntityNotFound, err)
		})
		t.Run("should return error when unable to try to retrieve entity", func(t *testing.T) {
			gitURL := "git@sample-url:sample-entity.go"

			db, clearDB := getMockDB()
			defer clearDB()

			entityStore := NewStore(db, "another_table")

			_, err := entityStore.Get(gitURL)

			assert.NotNil(t, err)
		})
	})
	t.Run("Update", func(t *testing.T) {
		t.Run("should update all fields", func(t *testing.T) {
			initialEntity := &protocol.Entity{
				ID:            "sample-entity-1",
				Name:          "sample-entity-1-name",
				Environment:   "env-a",
				GitURL:        "git@sample-url:sample-entity.go",
				GcpProjectIDs: []string{"sample-entity-1-project-1"},
			}

			updatedEntity := &protocol.Entity{
				ID:            "sample-entity-1",
				Name:          "sample-entity-1-name",
				Environment:   "env-a",
				GitURL:        "git@sample-url:sample-entity.go",
				GcpProjectIDs: []string{"sample-entity-1-project-1", "sample-entity-1-project-2"},
			}

			db, clearDB := getMockDB()
			defer clearDB()

			entityStore := NewStore(db, "entity_records")

			_, err := entityStore.Create(initialEntity)
			_, err = entityStore.Update(updatedEntity)

			result, err := entityStore.Get("sample-entity-1")

			assert.Nil(t, err)
			assert.Equal(t, updatedEntity, removeTime(result))
		})
		t.Run("should return error when update failed", func(t *testing.T) {
			entity := &protocol.Entity{
				ID:            "sample-entity-1",
				Name:          "sample-entity-1-name",
				Environment:   "env-a",
				GitURL:        "git@sample-url:sample-entity.go",
				GcpProjectIDs: []string{"sample-entity-1-project-1", "sample-entity-1-project-2"},
			}

			db, clearDB := getMockDB()
			defer clearDB()

			entityStore := NewStore(db, "another table")

			_, err := entityStore.Update(entity)

			assert.NotNil(t, err)
		})
	})
	t.Run("GetEntityByProjectID", func(t *testing.T) {
		t.Run("should get entity given project ID", func(t *testing.T) {
			gitURL := "git@sample-url:entity-1.git"
			entityA := &protocol.Entity{
				ID:            "sample-entity-1",
				Name:          "sample-entity-1-name",
				Environment:   "env-a",
				GitURL:        gitURL,
				GcpProjectIDs: []string{"sample-entity-1-project-1", "sample-entity-1-project-2"},
			}
			entityB := &protocol.Entity{
				ID:            "sample-entity-2",
				Name:          "sample-entity-2-name",
				Environment:   "env-b",
				GitURL:        gitURL,
				GcpProjectIDs: []string{"sample-entity-2-project-1", "sample-entity-2-project-2"},
			}

			db, clearDB := getMockDB()
			defer clearDB()

			entityStore := NewStore(db, "entity_records")

			_, err := entityStore.Create(entityA)
			_, err = entityStore.Create(entityB)
			result, err := entityStore.GetEntityByProjectID("sample-entity-1-project-1")

			assert.Nil(t, err)
			assert.Equal(t, entityA, removeTime(result))
		})
		t.Run("should return not found when entity with git url not found", func(t *testing.T) {

			db, clearDB := getMockDB()
			defer clearDB()

			entityStore := NewStore(db, "entity_records")

			result, err := entityStore.GetEntityByProjectID("abcd")

			assert.Nil(t, result)
			assert.Equal(t, protocol.ErrEntityNotFound, err)
		})
		t.Run("should return error when failed to retrieve entity", func(t *testing.T) {
			db, clearDB := getMockDB()
			defer clearDB()

			entityStore := NewStore(db, "another_table")

			_, err := entityStore.GetEntityByProjectID("abcd")

			assert.NotNil(t, err)
		})
	})
}
