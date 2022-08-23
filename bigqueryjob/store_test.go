package bigqueryjob

import (
	"github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestProfileBqStore(t *testing.T) {
	t.Run("Store", func(t *testing.T) {
		t.Run("should store profile and bq job id given correct input", func(t *testing.T) {
			bigqueryJob := &protocol.BigqueryJob{
				ID:        "1",
				ProfileID: "profile-id",
				BqID:      "bq-id",
				CreatedAt: time.Now().In(time.UTC),
			}

			db, clearDB := mock.NewDatabase(new(protocol.BigqueryJob))
			defer clearDB()

			profileBqStore := NewStore(db, "bigquery_jobs")

			err := profileBqStore.Store(bigqueryJob)

			var result protocol.BigqueryJob
			db.Find(&result)

			assert.Nil(t, err)
			assert.Equal(t, bigqueryJob, &result)
		})

		t.Run("should return error when db insert failed", func(t *testing.T) {
			bigqueryJob := &protocol.BigqueryJob{
				ID:        "1",
				ProfileID: "profile-id",
				BqID:      "bq-id",
				CreatedAt: time.Now().In(time.UTC),
			}

			db, clearDB := mock.NewDatabase(new(protocol.BigqueryJob))
			defer clearDB()

			profileBqStore := NewStore(db, "another_db")

			err := profileBqStore.Store(bigqueryJob)

			assert.Error(t, err)
		})
	})
}
