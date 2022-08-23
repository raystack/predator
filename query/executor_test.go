package query

import (
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"context"
	"errors"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/odpf/predator/stats"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBigqueryExecutor(t *testing.T) {
	t.Run("Run", func(t *testing.T) {
		t.Run("should return result", func(t *testing.T) {
			profileID := "job-abcd"
			bqJobID := "bq-1234"

			profile := &job.Profile{
				ID:  profileID,
				URN: "a.b.c",
			}

			label := &protocol.Label{
				Project: "a",
				Dataset: "b",
				Table:   "c",
			}

			profileBQ := &protocol.BigqueryJob{
				ProfileID: profileID,
				BqID:      bqJobID,
			}

			queryConfig := bqiface.QueryConfig{
				QueryConfig: bigquery.QueryConfig{
					Priority: bigquery.InteractivePriority,
				},
			}

			modifiedQueryConfig := bqiface.QueryConfig{
				QueryConfig: bigquery.QueryConfig{
					Priority: bigquery.BatchPriority,
				},
			}

			queryStatistics := &bigquery.QueryStatistics{
				TotalBytesProcessed: 10,
				SlotMillis:          20,
			}

			jobStatus := &bigquery.JobStatus{
				Statistics: &bigquery.JobStatistics{
					Details: queryStatistics,
				},
			}

			queryType := job.StatisticalQuery
			queryStr := "select 1 as ct, \"2012-01-01\" as dt from x group by dt"
			bigqueryIteratorValues := []*map[string]bigquery.Value{
				{
					"ct": 1,
					"dt": civil.Date{
						Year:  2012,
						Month: 01,
						Day:   01,
					},
				},
				{
					"ct": 1,
					"dt": civil.Date{
						Year:  2012,
						Month: 01,
						Day:   01,
					},
				},
			}

			expected := []protocol.Row{
				{
					"ct": 1,
					"dt": civil.Date{
						Year:  2012,
						Month: 01,
						Day:   01,
					},
				},
				{
					"ct": 1,
					"dt": civil.Date{
						Year:  2012,
						Month: 01,
						Day:   01,
					},
				},
			}

			rowIterator := mock.NewIteratorStub(bigqueryIteratorValues)

			bqJob := &mock.JobMock{}
			bqJob.On("ID").Return(bqJobID)
			bqJob.On("Read", context.Background()).Return(rowIterator, nil)

			bqJob.On("Status", context.Background()).Return(jobStatus, nil)

			query := &mock.QueryMock{}
			query.On("QueryConfig").Return(queryConfig)
			query.On("SetQueryConfig", modifiedQueryConfig)
			query.On("JobIDConfig").Return(&bigquery.JobIDConfig{
				JobID: bqJobID,
			})
			query.On("Run", context.Background()).Return(bqJob, nil)

			client := &mock.BQClientMock{}

			bigqueryJobStore := mock.NewBigqueryJobStore()
			defer bigqueryJobStore.AssertExpectations(t)

			client.On("Query", queryStr).Return(query)
			bigqueryJobStore.On("Store", profileBQ).Return(nil)

			profileStore := mock.NewProfileStoreStub()

			statsClient := mock.NewDummyStats()
			defer statsClient.AssertExpectations(t)

			tags := []stats.KV{{K: "query_type", V: "metadata"}}
			statsClient.On("WithTags", tags).Return(statsClient)

			statsClientBuilder := mock.NewStatBuilder()
			defer statsClientBuilder.AssertExpectations(t)

			statsClientBuilder.On("WithURN", label).Return(statsClientBuilder)
			statsClientBuilder.On("Build").Return(statsClient, nil)

			queryExecutor := NewBigqueryExecutor(client, bigqueryJobStore, profileStore, statsClientBuilder)
			result, err := queryExecutor.Run(profile, queryStr, queryType)

			assert.Equal(t, expected, result)
			assert.Nil(t, err)
		})
		t.Run("should return error when query job failed", func(t *testing.T) {
			someError := errors.New("API error")
			profileID := "job-abcd"

			profile := &job.Profile{
				ID:  profileID,
				URN: "a.b.c",
			}

			label := &protocol.Label{
				Project: "a",
				Dataset: "b",
				Table:   "c",
			}

			queryConfig := bqiface.QueryConfig{
				QueryConfig: bigquery.QueryConfig{
					Priority: bigquery.InteractivePriority,
				},
			}

			modifiedQueryConfig := bqiface.QueryConfig{
				QueryConfig: bigquery.QueryConfig{
					Priority: bigquery.BatchPriority,
				},
			}

			queryType := job.StatisticalQuery
			queryStr := "select 1 as ct"

			client := &mock.BQClientMock{}
			defer client.AssertExpectations(t)

			bigqueryJobStore := mock.NewBigqueryJobStore()
			defer bigqueryJobStore.AssertExpectations(t)

			var bqJob *mock.JobMock
			query := &mock.QueryMock{}
			query.On("QueryConfig").Return(queryConfig)
			query.On("SetQueryConfig", modifiedQueryConfig)

			query.On("Run", context.Background()).Return(bqJob, someError)
			client.On("Query", queryStr).Return(query)

			profileStore := mock.NewProfileStoreStub()

			statsClient := mock.NewDummyStats()
			defer statsClient.AssertExpectations(t)

			tags := []stats.KV{{K: "query_type", V: "metadata"}}
			statsClient.On("WithTags", tags).Return(statsClient)

			statsClientBuilder := mock.NewStatBuilder()
			defer statsClientBuilder.AssertExpectations(t)

			statsClientBuilder.On("WithURN", label).Return(statsClientBuilder)
			statsClientBuilder.On("Build").Return(statsClient, nil)

			queryExecutor := NewBigqueryExecutor(client, bigqueryJobStore, profileStore, statsClientBuilder)
			result, err := queryExecutor.Run(profile, queryStr, queryType)

			assert.Error(t, err)
			assert.Nil(t, result)
		})
	})
}
