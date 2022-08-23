package mock

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/stretchr/testify/mock"
	"google.golang.org/api/iterator"
)

type BQClientMock struct {
	mock.Mock
	bqiface.Client
}

func (cli *BQClientMock) Location() string {
	panic("not implemented")
}

func (cli *BQClientMock) SetLocation(string) {
	panic("not implemented")
}

func (cli *BQClientMock) Close() error {
	panic("not implemented")
}

func (cli *BQClientMock) Dataset(string) bqiface.Dataset {
	panic("not implemented")
}

func (cli *BQClientMock) DatasetInProject(project string, dataset string) bqiface.Dataset {
	panic("not implemented")
}

func (cli *BQClientMock) Datasets(context.Context) bqiface.DatasetIterator {
	panic("not implemented")
}

func (cli *BQClientMock) DatasetsInProject(context.Context, string) bqiface.DatasetIterator {
	panic("not implemented")
}

func (cli *BQClientMock) Query(query string) bqiface.Query {
	args := cli.Called(query)
	return args.Get(0).(bqiface.Query)
}

func (cli *BQClientMock) JobFromID(context.Context, string) (bqiface.Job, error) {
	panic("not implemented")
}

func (cli *BQClientMock) JobFromIDLocation(context.Context, string, string) (bqiface.Job, error) {
	panic("not implemented")
}

func (cli *BQClientMock) Jobs(context.Context) bqiface.JobIterator {
	panic("not implemented")
}

type QueryMock struct {
	mock.Mock
	bqiface.Query
}

func (q *QueryMock) QueryConfig() bqiface.QueryConfig {
	args := q.Called()
	return args.Get(0).(bqiface.QueryConfig)
}

func (q *QueryMock) JobIDConfig() *bigquery.JobIDConfig {
	args := q.Called()
	return args.Get(0).(*bigquery.JobIDConfig)
}

func (q *QueryMock) SetQueryConfig(queryConfig bqiface.QueryConfig) {
	q.Called(queryConfig)
}

func (q *QueryMock) Run(ctx context.Context) (bqiface.Job, error) {
	args := q.Called(ctx)
	return args.Get(0).(bqiface.Job), args.Error(1)
}

func (q *QueryMock) Read(ctx context.Context) (bqiface.RowIterator, error) {
	args := q.Called(ctx)
	return args.Get(0).(bqiface.RowIterator), args.Error(1)
}

type JobMock struct {
	mock.Mock
	bqiface.Job
}

func (j *JobMock) ID() string {
	return j.Called().String(0)
}

func (j *JobMock) Location() string {
	panic("not implemented")
}

func (j *JobMock) Config() (bigquery.JobConfig, error) {
	panic("not implemented")
}

func (j *JobMock) Status(ctx context.Context) (*bigquery.JobStatus, error) {
	args := j.Called(ctx)
	return args.Get(0).(*bigquery.JobStatus), args.Error(1)
}

func (j *JobMock) LastStatus() *bigquery.JobStatus {
	panic("not implemented")
}

func (j *JobMock) Cancel(_ context.Context) error {
	panic("not implemented")
}

func (j *JobMock) Wait(_ context.Context) (*bigquery.JobStatus, error) {
	panic("not implemented")
}

func (j *JobMock) Read(ctx context.Context) (bqiface.RowIterator, error) {
	args := j.Called(ctx)
	return args.Get(0).(bqiface.RowIterator), args.Error(1)
}

type RowIteratorMock struct {
	mock.Mock
	bqiface.RowIterator
}

func (ri *RowIteratorMock) SetStartIndex(uint64) {
	panic("not implemented")
}
func (ri *RowIteratorMock) Schema() bigquery.Schema {
	panic("not implemented")
}
func (ri *RowIteratorMock) TotalRows() uint64 {
	panic("not implemented")
}
func (ri *RowIteratorMock) Next(row interface{}) error {
	args := ri.Called(row)

	//initiate map
	rowPtr := row.(*map[string]bigquery.Value)
	if *rowPtr == nil {
		*rowPtr = map[string]bigquery.Value{}
	}
	rowValue := *rowPtr

	//modify external ref
	expected := args.Get(1).(map[string]bigquery.Value)
	for k, v := range expected {
		rowValue[k] = v
	}

	return args.Error(0)
}
func (ri *RowIteratorMock) PageInfo() *iterator.PageInfo {
	panic("not implemented")
}

type MultiRowIteratorMock struct {
	rows      []*map[string]bigquery.Value
	index     int
	rowLength int

	bqiface.RowIterator
}

//NewIteratorStub is stub for bigquery row iterator
func NewIteratorStub(rows []*map[string]bigquery.Value) *MultiRowIteratorMock {
	return &MultiRowIteratorMock{
		rows:      rows,
		rowLength: len(rows),
	}
}

func (m *MultiRowIteratorMock) SetStartIndex(u uint64) {
	panic("implement me")
}

func (m *MultiRowIteratorMock) Schema() bigquery.Schema {
	panic("implement me")
}

func (m *MultiRowIteratorMock) TotalRows() uint64 {
	return uint64(m.rowLength)
}

func (m *MultiRowIteratorMock) Next(row interface{}) error {

	if m.index >= m.rowLength {
		return iterator.Done
	}
	r := m.rows[m.index]

	rowPtr := row.(*map[string]bigquery.Value)
	if *rowPtr == nil {
		*rowPtr = make(map[string]bigquery.Value)
	}
	rowValue := *rowPtr

	for k, v := range *r {
		rowValue[k] = v
	}
	m.index++

	return nil
}

func (m *MultiRowIteratorMock) PageInfo() *iterator.PageInfo {
	panic("implement me")
}
