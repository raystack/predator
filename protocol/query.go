package protocol

import "github.com/odpf/predator/protocol/job"

//Row is single table row the key of map is column name and the value is the cell value
type Row map[string]interface{}

//QueryExecutor that execute bigquery SQL query script return list of Row as result
type QueryExecutor interface {
	Run(profile *job.Profile, query string, queryType job.QueryType) ([]Row, error)
}
