package profile

import (
	"errors"
	"github.com/google/uuid"
	"testing"
	"time"

	pmock "github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/stretchr/testify/assert"
)

func TestProfileStore(t *testing.T) {
	t.Run("Create", func(t *testing.T) {
		t.Run("should create profile", func(t *testing.T) {
			db, clearDb := pmock.NewDatabase(new(profileRecord))
			defer clearDb()

			currentTime := time.Now().In(time.UTC)
			tableURN := "project.dataset.table"
			groupName := "field_grouping"

			prof := &job.Profile{
				ID:             "1",
				EventTimestamp: currentTime,
				Status:         job.StateCreated,
				URN:            tableURN,
				GroupName:      groupName,
				Mode:           job.ModeComplete,
				Filter:         "field_status = 'sample_status'",
			}

			status := &protocol.Status{
				JobID:   "1",
				JobType: job.TypeProfile,
				Status:  string(job.StateCreated),
			}

			sStore := pmock.NewStatusStore()
			defer sStore.AssertExpectations(t)
			sStore.On("Store", status).Return(nil)

			store := NewStore(db, "profile_records", sStore)
			_, err := store.Create(prof)

			var result profileRecord
			db.Find(&result)

			assert.Nil(t, err)
			assert.Equal(t, prof, result.toProfile(status))
		})
		t.Run("should return error when db insert failed", func(t *testing.T) {
			db, clearDb := pmock.NewDatabase(new(profileRecord))
			defer clearDb()

			currentTime := time.Now().In(time.UTC)
			tableURN := "project.dataset.table"
			groupName := "field_grouping"

			prof := &job.Profile{
				ID:             "1",
				EventTimestamp: currentTime,
				Status:         job.StateCreated,
				URN:            tableURN,
				GroupName:      groupName,
				Mode:           job.ModeComplete,
				Filter:         "field_status = 'sample_status'",
			}

			sStore := pmock.NewStatusStore()
			defer sStore.AssertExpectations(t)

			store := NewStore(db, "another_table", sStore)
			p, err := store.Create(prof)

			assert.Nil(t, p)
			assert.Error(t, err)
		})
		t.Run("should return error when status insert failed", func(t *testing.T) {
			someError := errors.New("db error")
			db, clearDb := pmock.NewDatabase(new(profileRecord))
			defer clearDb()

			currentTime := time.Now().In(time.UTC)
			tableURN := "project.dataset.table"
			groupName := "field_grouping"

			prof := &job.Profile{
				ID:             "1",
				EventTimestamp: currentTime,
				Status:         job.StateCreated,
				URN:            tableURN,
				GroupName:      groupName,
				Mode:           job.ModeComplete,
				Filter:         "field_status = 'sample_status'",
			}

			status := &protocol.Status{
				JobID:   "1",
				JobType: job.TypeProfile,
				Status:  string(job.StateCreated),
			}

			sStore := pmock.NewStatusStore()
			defer sStore.AssertExpectations(t)
			sStore.On("Store", status).Return(someError)

			store := NewStore(db, "profile_records", sStore)
			p, err := store.Create(prof)

			assert.Nil(t, p)
			assert.Error(t, err)
		})
	})
	t.Run("Update", func(t *testing.T) {
		t.Run("should update profile and insert status", func(t *testing.T) {
			db, clearDb := pmock.NewDatabase(new(profileRecord))
			defer clearDb()

			currentTime := time.Now().In(time.UTC)

			prof := &job.Profile{
				ID:             "1",
				EventTimestamp: currentTime,
				Status:         job.StateCreated,
				Message:        "state update",
			}

			updatedProf := &job.Profile{
				ID:             "1",
				EventTimestamp: currentTime,
				Status:         job.StateInProgress,
				Message:        "in progress",
				TotalRecords:   20,
			}

			status := &protocol.Status{
				JobID:   "1",
				JobType: job.TypeProfile,
				Status:  string(job.StateCreated),
				Message: "state update",
			}

			updatedStatis := &protocol.Status{
				JobID:   "1",
				JobType: job.TypeProfile,
				Status:  job.StateInProgress.String(),
				Message: "in progress",
			}

			sStore := pmock.NewStatusStore()
			defer sStore.AssertExpectations(t)
			sStore.On("Store", status).Return(nil)
			sStore.On("Store", updatedStatis).Return(nil)

			store := NewStore(db, "profile_records", sStore)
			_, err := store.Create(prof)
			err = store.Update(updatedProf)

			var result profileRecord
			db.Find(&result)

			assert.Nil(t, err)
			assert.Equal(t, int64(20), result.TotalRecords)
		})
		t.Run("should failed when update profile failed", func(t *testing.T) {
			db, clearDb := pmock.NewEmptyDatabase()
			defer clearDb()

			currentTime := time.Now().In(time.UTC)

			updatedProf := &job.Profile{
				ID:             "1",
				EventTimestamp: currentTime,
				Status:         job.StateInProgress,
				Message:        "in progress",
				TotalRecords:   20,
			}

			sStore := pmock.NewStatusStore()
			defer sStore.AssertExpectations(t)

			store := NewStore(db, "profile_records", sStore)
			err := store.Update(updatedProf)

			assert.Error(t, err)
		})
		t.Run("should return error when status insert failed", func(t *testing.T) {
			someError := errors.New("db error")
			db, clearDb := pmock.NewDatabase(new(profileRecord))
			defer clearDb()

			currentTime := time.Now().In(time.UTC)

			prof := &job.Profile{
				ID:             "1",
				EventTimestamp: currentTime,
				Status:         job.StateCreated,
				Message:        "state update",
			}

			status := &protocol.Status{
				JobID:   "1",
				JobType: job.TypeProfile,
				Status:  string(job.StateCreated),
				Message: "state update",
			}

			sStore := pmock.NewStatusStore()
			defer sStore.AssertExpectations(t)
			sStore.On("Store", status).Return(someError)

			store := NewStore(db, "profile_records", sStore)
			err := store.Update(prof)

			assert.Error(t, err)
		})
	})
	t.Run("Get", func(t *testing.T) {
		t.Run("should return Profile", func(t *testing.T) {
			currentTime := time.Now().In(time.UTC)

			ID := uuid.Must(uuid.NewRandom()).String()
			tableURN := "project.dataset.table"
			prof := &job.Profile{
				ID:             ID,
				EventTimestamp: currentTime,
				URN:            tableURN,
				Status:         job.StateCreated,
			}
			status := &protocol.Status{
				JobID:   ID,
				JobType: job.TypeProfile,
				Status:  string(job.StateCreated),
				Message: "",
			}

			sStore := pmock.NewStatusStore()
			defer sStore.AssertExpectations(t)
			sStore.On("Store", status).Return(nil)
			sStore.On("GetLatestStatusByIDandType", ID, job.TypeProfile).Return(status, nil)

			db, clearDb := pmock.NewDatabase(new(profileRecord))
			defer clearDb()
			store := NewStore(db, "profile_records", sStore)

			_, err := store.Create(prof)
			resultProfile, err := store.Get(ID)

			assert.Nil(t, err)
			assert.Equal(t, prof, resultProfile)
		})
		t.Run("should return error when db query failed", func(t *testing.T) {
			sStore := pmock.NewStatusStore()
			db, clearDb := pmock.NewDatabase(new(profileRecord))
			clearDb()
			store := NewStore(db, "another_db", sStore)

			_, err := store.Get("random-id")

			assert.NotNil(t, err)
		})
		t.Run("should return ErrProfileNotFound when no profile exist", func(t *testing.T) {
			ID := uuid.Must(uuid.NewRandom()).String()
			sStore := pmock.NewStatusStore()

			db, clearDb := pmock.NewDatabase(new(profileRecord))
			defer clearDb()

			store := NewStore(db, "profile_records", sStore)
			_, err := store.Get(ID)

			assert.Equal(t, protocol.ErrProfileNotFound, err)
		})
	})
}
