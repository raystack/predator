package protocol

import "time"

//BigqueryJob bigquery job information of a profile task
type BigqueryJob struct {
	ID        string
	ProfileID string

	//BqID is bigquery job ID provided by query execution
	BqID string

	CreatedAt time.Time
}

//BigqueryJobStore to store Bigquery job created by profile job
type BigqueryJobStore interface {
	Store(bigqueryJob *BigqueryJob) error
}
