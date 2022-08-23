package query

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/odpf/predator/protocol/xlog"
	"github.com/odpf/predator/stats"
	"google.golang.org/api/iterator"
	"log"
	"os"
)

var logger = log.New(os.Stdout, "INFO: ", log.Lshortfile|log.LstdFlags)

//BigqueryExecutor to execute query in bigquery data warehouse
type BigqueryExecutor struct {
	client             bqiface.Client
	store              protocol.BigqueryJobStore
	profileStore       protocol.ProfileStore
	statsClientBuilder stats.ClientBuilder
}

//NewBigqueryExecutor constructor of BigqueryExecutor
func NewBigqueryExecutor(client bqiface.Client,
	store protocol.BigqueryJobStore,
	profileStore protocol.ProfileStore,
	statsFactory stats.ClientBuilder) *BigqueryExecutor {
	return &BigqueryExecutor{
		client:             client,
		store:              store,
		profileStore:       profileStore,
		statsClientBuilder: statsFactory,
	}
}

//Run executes query and log the bigquery job progress and information to a storage
func (qe *BigqueryExecutor) Run(profile *job.Profile, query string, queryType job.QueryType) ([]protocol.Row, error) {
	var jobID string

	label, err := protocol.ParseLabel(profile.URN)
	if err != nil {
		return nil, err
	}

	clientBuilder := qe.statsClientBuilder.WithURN(label)
	statsClient, err := clientBuilder.Build()
	if err != nil {
		return nil, err
	}

	switch queryType {
	case job.FieldLevelQuery:
		tag := stats.KV{K: "query_type", V: "column_metric"}
		statsClient = statsClient.WithTags(tag)
	case job.TableLevelQuery:
		tag := stats.KV{K: "query_type", V: "table_metric"}
		statsClient = statsClient.WithTags(tag)
	case job.StatisticalQuery:
		tag := stats.KV{K: "query_type", V: "metadata"}
		statsClient = statsClient.WithTags(tag)
	}

	defer func() {
		if err != nil {
			msg := xlog.Format(fmt.Errorf("bigquery job to fetch %s metrics failed %w", queryType.String(), err).Error(), xlog.NewValue("bq_job_id", jobID))

			profile.Message = msg
			if err = qe.profileStore.Update(profile); err != nil {
				e := fmt.Errorf("unable to write log message %w", err)
				logger.Println(e)
			}
			failedStatMetric := stats.Metric("profile.bigquery.job.failed.count")
			statsClient.Increment(failedStatMetric)
		}
		completedStatMetric := stats.Metric("profile.bigquery.job.completed.count")
		statsClient.Increment(completedStatMetric)
	}()

	q := qe.client.Query(query)
	queryConfig := q.QueryConfig()
	queryConfig.Priority = bigquery.BatchPriority
	q.SetQueryConfig(queryConfig)

	queryJob, err := q.Run(context.Background())
	if err != nil {
		return nil, err
	}

	createdStatMetric := stats.Metric("profile.bigquery.job.created.count")
	statsClient.Increment(createdStatMetric)

	jobID = queryJob.ID()

	msg := xlog.Format(fmt.Sprintf("started bigquery job to fetch %s metrics", queryType.String()), xlog.NewValue("bq_job_id", jobID), xlog.NewValue("profile_id", profile.ID))
	logger.Println(msg)

	profile.Message = msg
	if err = qe.profileStore.Update(profile); err != nil {
		return nil, fmt.Errorf("unable to write log message %w", err)
	}

	bigqueryJob := &protocol.BigqueryJob{
		ProfileID: profile.ID,
		BqID:      jobID,
	}
	err = qe.store.Store(bigqueryJob)
	if err != nil {
		return nil, err
	}

	it, err := queryJob.Read(context.Background())
	if err != nil {
		return nil, err
	}

	jobStatus, err := queryJob.Status(context.Background())
	if err != nil {
		return nil, err
	}

	queryStatistics := jobStatus.Statistics.Details.(*bigquery.QueryStatistics)

	bytesProcessedStat := stats.Metric("profile.bigquery.job.totalbytes")
	totalBytesProcessed := queryStatistics.TotalBytesProcessed
	statsClient.IncrementBy(bytesProcessedStat, totalBytesProcessed)

	slotMillisStat := stats.Metric("profile.bigquery.job.slotmillis")
	slotMillis := queryStatistics.SlotMillis
	statsClient.IncrementBy(slotMillisStat, slotMillis)

	jobDurationStat := stats.Metric("profile.bigquery.job.time")
	jobStatistics := jobStatus.Statistics
	start := jobStatistics.CreationTime
	end := jobStatistics.EndTime
	statsClient.DurationOf(jobDurationStat, start, end)

	msg = xlog.Format(fmt.Sprintf("bigquery job to fetch %s metrics finished", queryType.String()), xlog.NewValue("bq_job_id", jobID), xlog.NewValue("profile_id", profile.ID))
	logger.Println(msg)

	profile.Message = msg
	if err = qe.profileStore.Update(profile); err != nil {
		return nil, fmt.Errorf("unable to write log message %w", err)
	}

	var rows []protocol.Row
	for {
		var row map[string]bigquery.Value
		err := it.Next(&row)

		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, err
		}

		r := make(map[string]interface{})
		for k, v := range row {
			r[k] = v
		}

		rows = append(rows, r)
	}

	return rows, err
}
