package status

import (
	"github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestStore_Store(t *testing.T) {
	t.Run("should store status", func(t *testing.T) {
		db, clearFunc := mock.NewDatabase(new(statusRecord))
		defer clearFunc()

		currentTime := time.Now().In(time.UTC)
		status := &protocol.Status{
			ID:             "1",
			JobID:          "job-abcd",
			JobType:        job.TypeProfile,
			Status:         string(job.StateInProgress),
			Message:        "gathering metadata",
			EventTimestamp: currentTime,
		}

		store := NewStore(db, "status_records")

		err := store.Store(status)

		var result statusRecord
		db.Find(&result)

		assert.Nil(t, err)
		assert.Equal(t, status, result.toStatus())
	})
	t.Run("should return error when insert failed", func(t *testing.T) {
		db, clearFunc := mock.NewDatabase(new(statusRecord))
		defer clearFunc()

		currentTime := time.Now().In(time.UTC)
		status := &protocol.Status{
			ID:             "1",
			JobID:          "job-abcd",
			JobType:        job.TypeProfile,
			Status:         string(job.StateInProgress),
			Message:        "gathering metadata",
			EventTimestamp: currentTime,
		}

		store := NewStore(db, "another table")

		err := store.Store(status)

		assert.Error(t, err)
	})
}

func TestStore_GetLatestStatusByIDandType(t *testing.T) {
	t.Run("should return status given correct profile job and type", func(t *testing.T) {
		db, clearDb := mock.NewDatabase(new(statusRecord))
		defer clearDb()

		jobID := "job-abcd"
		jobType := job.TypeProfile
		currentTime := time.Now().In(time.UTC)
		statusFirst := &protocol.Status{
			ID:             "1",
			JobID:          jobID,
			JobType:        jobType,
			Status:         string(job.StateInProgress),
			Message:        "gathering metadata 1",
			EventTimestamp: currentTime,
		}
		statusSecond := &protocol.Status{
			ID:             "2",
			JobID:          jobID,
			JobType:        jobType,
			Status:         string(job.StateInProgress),
			Message:        "gathering metadata 2",
			EventTimestamp: currentTime.Add(10 * time.Second),
		}
		statusThird := &protocol.Status{
			ID:             "3",
			JobID:          jobID,
			JobType:        jobType,
			Status:         string(job.StateCompleted),
			Message:        "finished",
			EventTimestamp: currentTime.Add(20 * time.Second),
		}

		statusStore := NewStore(db, "status_records")
		statusStore.Store(statusFirst)
		statusStore.Store(statusSecond)
		statusStore.Store(statusThird)

		latestStatus, err := statusStore.GetLatestStatusByIDandType(jobID, jobType)

		assert.Nil(t, err)
		assert.Equal(t, statusThird, latestStatus)
	})
	t.Run("should return error when profile not found", func(t *testing.T) {
		db, clearDb := mock.NewDatabase(new(statusRecord))
		defer clearDb()

		jobID := "job-abcd"
		jobType := job.TypeProfile

		statusStore := NewStore(db, "status_records")

		latestStatus, err := statusStore.GetLatestStatusByIDandType(jobID, jobType)

		assert.Nil(t, latestStatus)
		assert.Equal(t, protocol.ErrStatusNotFound, err)
	})
	t.Run("should return error when type not found", func(t *testing.T) {
		db, clearDb := mock.NewDatabase(new(statusRecord))
		defer clearDb()

		jobID := "job-abcd"
		currentTime := time.Now().In(time.UTC)
		status := &protocol.Status{
			ID:             "1",
			JobID:          jobID,
			JobType:        job.TypeProfile,
			Status:         string(job.StateInProgress),
			Message:        "gathering metadata",
			EventTimestamp: currentTime,
		}

		statusStore := NewStore(db, "status_records")
		statusStore.Store(status)

		latestStatus, err := statusStore.GetLatestStatusByIDandType("job-1234", job.TypeAudit)

		assert.Nil(t, latestStatus)
		assert.Equal(t, protocol.ErrStatusNotFound, err)
	})
	t.Run("should return error when db query failed", func(t *testing.T) {
		db, clearDb := mock.NewDatabase(new(statusRecord))
		defer clearDb()

		statusStore := NewStore(db, "another_db")

		latestStatus, err := statusStore.GetLatestStatusByIDandType("job-1234", job.TypeAudit)

		assert.NotNil(t, err)
		assert.Nil(t, latestStatus)
	})
}

func TestStore_GetStatusLogByIDandType(t *testing.T) {
	t.Run("should return list of status given correct profileID and type", func(t *testing.T) {
		db, clearDb := mock.NewDatabase(new(statusRecord))
		defer clearDb()

		jobID := "job-abcd"
		jobType := job.TypeProfile
		currentTime := time.Now().In(time.UTC)
		statusFirst := &protocol.Status{
			ID:             "1",
			JobID:          jobID,
			JobType:        jobType,
			Status:         string(job.StateInProgress),
			Message:        "gathering metadata 1",
			EventTimestamp: currentTime,
		}
		statusSecond := &protocol.Status{
			ID:             "2",
			JobID:          jobID,
			JobType:        jobType,
			Status:         string(job.StateInProgress),
			Message:        "gathering metadata 2",
			EventTimestamp: currentTime.Add(10 * time.Second),
		}
		statusThird := &protocol.Status{
			ID:             "3",
			JobID:          jobID,
			JobType:        jobType,
			Status:         string(job.StateCompleted),
			Message:        "finished",
			EventTimestamp: currentTime.Add(20 * time.Second),
		}
		expectedStatus := []*protocol.Status{
			statusThird,
			statusSecond,
			statusFirst,
		}

		statusStore := NewStore(db, "status_records")
		statusStore.Store(statusFirst)
		statusStore.Store(statusSecond)
		statusStore.Store(statusThird)

		actualStatus, err := statusStore.GetStatusLogByIDandType(jobID, jobType)

		assert.Nil(t, err)
		assert.Equal(t, expectedStatus, actualStatus)
	})
	t.Run("should return error when profileID not found", func(t *testing.T) {
		db, clearDb := mock.NewDatabase(new(statusRecord))
		defer clearDb()

		jobType := job.TypeProfile
		currentTime := time.Now().In(time.UTC)
		status := &protocol.Status{
			ID:             "1",
			JobID:          "job-abcd",
			JobType:        jobType,
			Status:         string(job.StateInProgress),
			Message:        "gathering metadata",
			EventTimestamp: currentTime,
		}

		statusStore := NewStore(db, "status_records")
		statusStore.Store(status)

		latestStatus, err := statusStore.GetStatusLogByIDandType("job-1234", jobType)

		assert.Nil(t, latestStatus)
		assert.Equal(t, protocol.ErrStatusNotFound, err)
	})
	t.Run("should return error when type not found", func(t *testing.T) {
		db, clearDb := mock.NewDatabase(new(statusRecord))
		defer clearDb()

		jobID := "job-abcd"
		currentTime := time.Now().In(time.UTC)
		status := &protocol.Status{
			ID:             "1",
			JobID:          jobID,
			JobType:        job.TypeProfile,
			Status:         string(job.StateInProgress),
			Message:        "gathering metadata",
			EventTimestamp: currentTime,
		}

		statusStore := NewStore(db, "status_records")
		statusStore.Store(status)

		latestStatus, err := statusStore.GetStatusLogByIDandType("job-1234", job.TypeAudit)

		assert.Nil(t, latestStatus)
		assert.Equal(t, protocol.ErrStatusNotFound, err)
	})
	t.Run("should return error when db query failed", func(t *testing.T) {
		db, clearDb := mock.NewDatabase(new(statusRecord))
		defer clearDb()

		statusStore := NewStore(db, "another_db")

		latestStatus, err := statusStore.GetStatusLogByIDandType("job-1234", job.TypeAudit)

		assert.NotNil(t, err)
		assert.Nil(t, latestStatus)
	})
}
