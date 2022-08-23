package audit

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jinzhu/gorm"
	"github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/stretchr/testify/assert"
)

func emptyDB() (*gorm.DB, func()) {
	db, _ := gorm.Open("sqlite3", ":memory:")

	closeDB := func() {
		db.Close()
	}
	return db, closeDB
}

func newAuditTable() (*gorm.DB, func()) {
	db, _ := gorm.Open("sqlite3", ":memory:")

	audit := new(job.Audit)

	clearDB := func() {
		db.Close()
		db.DropTableIfExists(audit)
	}
	if exists := db.HasTable(audit); !exists {
		db.CreateTable(audit)
	}

	return db, clearDB
}

func TestAuditStore(t *testing.T) {
	t.Run("StoreAudit", func(t *testing.T) {
		t.Run("should store audit results", func(t *testing.T) {
			ID := uuid.Must(uuid.NewRandom()).String()
			db, clear := newAuditTable()
			defer clear()

			currentTime := time.Now().In(time.UTC)
			auditObj := &job.Audit{
				ID:             ID,
				ProfileID:      "profile-1234",
				EventTimestamp: currentTime,
				State:          job.StateCreated,
				TotalRecords:   20,
			}

			status := &protocol.Status{
				JobID:   auditObj.ID,
				JobType: job.TypeAudit,
				Status:  string(job.StateCreated),
				Message: "",
			}

			statusStore := mock.NewStatusStore()
			statusStore.On("Store", status).Return(nil)

			store := NewStore(db, "audits", statusStore)
			_, err := store.CreateAudit(auditObj)

			var result []job.Audit
			db.Find(&result)

			expected := []job.Audit{
				{
					ID:             ID,
					ProfileID:      "profile-1234",
					EventTimestamp: currentTime,
					TotalRecords:   20,
				},
			}

			assert.Equal(t, expected, result)
			assert.Nil(t, err)
		})
		t.Run("should return error when table doesnt exist", func(t *testing.T) {
			db, clear := emptyDB()
			defer clear()

			currentTime := time.Now().In(time.UTC)
			auditObj := &job.Audit{
				ID:             "abcd",
				ProfileID:      "profile-1234",
				EventTimestamp: currentTime,
				State:          job.StateCreated,
			}

			statusStore := mock.NewStatusStore()
			store := NewStore(db, "audits", statusStore)
			_, err := store.CreateAudit(auditObj)

			assert.NotNil(t, err)
		})
	})

	t.Run("UpdateAudit", func(t *testing.T) {
		t.Run("should insert status in status store based on audit job", func(t *testing.T) {
			auditID := "audit-id"
			URN := "project.dataset.table"
			completedMessage := fmt.Sprintf("Table %s has all audited", URN)
			db, clear := newAuditTable()
			defer clear()

			audit := &job.Audit{
				ID:      auditID,
				State:   job.StateCompleted,
				Message: completedMessage,
				URN:     URN,
			}
			status := &protocol.Status{
				JobID:   auditID,
				JobType: job.TypeAudit,
				Status:  string(job.StateCompleted),
				Message: completedMessage,
			}

			statusStore := mock.NewStatusStore()
			statusStore.On("Store", status).Return(nil)
			defer statusStore.AssertExpectations(t)

			store := NewStore(db, "audits", statusStore)
			err := store.UpdateAudit(audit)

			assert.Nil(t, err)
		})
	})
}
