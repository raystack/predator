package metadata_test

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/stretchr/testify/mock"
)

type SpyConstraintStore struct {
	mock.Mock
}

func (spy *SpyConstraintStore) FetchConstraints(tableID string) ([]string, error) {
	args := spy.Called(tableID)
	return args.Get(0).([]string), args.Error(1)
}

type BqClientMock struct {
	mock.Mock
	bqiface.Client
}

func (cli *BqClientMock) Location() string {
	panic("not implemented")
}

func (cli *BqClientMock) SetLocation(string) {
	panic("not implemented")
}

func (cli *BqClientMock) Close() error {
	panic("not implemented")
}

func (cli *BqClientMock) Dataset(string) bqiface.Dataset {
	panic("not implemented")
}

func (cli *BqClientMock) DatasetInProject(project string, dataset string) bqiface.Dataset {
	return cli.Called(project, dataset).Get(0).(bqiface.Dataset)
}

func (cli *BqClientMock) Datasets(context.Context) bqiface.DatasetIterator {
	panic("not implemented")
}

func (cli *BqClientMock) DatasetsInProject(context.Context, string) bqiface.DatasetIterator {
	panic("not implemented")
}

func (cli *BqClientMock) Query(string) bqiface.Query {
	panic("not implemented")
}

func (cli *BqClientMock) JobFromID(context.Context, string) (bqiface.Job, error) {
	panic("not implemented")
}

func (cli *BqClientMock) JobFromIDLocation(context.Context, string, string) (bqiface.Job, error) {
	panic("not implemented")
}

func (cli *BqClientMock) Jobs(context.Context) bqiface.JobIterator {
	panic("not implemented")
}

type BqDatasetMock struct {
	mock.Mock
	bqiface.Dataset
}

func (ds *BqDatasetMock) ProjectID() string {
	panic("not implemented")
}

func (ds *BqDatasetMock) DatasetID() string {
	panic("not implemented")
}

func (ds *BqDatasetMock) Create(ctx context.Context, meta *bqiface.DatasetMetadata) error {
	return ds.Called(ctx, meta).Error(0)
}

func (ds *BqDatasetMock) Delete(context.Context) error {
	panic("not implemented")
}

func (ds *BqDatasetMock) DeleteWithContents(context.Context) error {
	panic("not implemented")
}

func (ds *BqDatasetMock) Metadata(ctx context.Context) (*bqiface.DatasetMetadata, error) {
	args := ds.Called(ctx)
	return args.Get(0).(*bqiface.DatasetMetadata), args.Error(1)
}

func (ds *BqDatasetMock) Update(context.Context, bqiface.DatasetMetadataToUpdate, string) (*bqiface.DatasetMetadata, error) {
	panic("not implemented")
}

func (ds *BqDatasetMock) Table(name string) bqiface.Table {
	return ds.Called(name).Get(0).(bqiface.Table)
}

func (ds *BqDatasetMock) Tables(context.Context) bqiface.TableIterator {
	panic("not implemented")
}

type BqTableMock struct {
	mock.Mock
	bqiface.Table
}

func (table *BqTableMock) CopierFrom(...bqiface.Table) bqiface.Copier {
	panic("not implemented")
}

func (table *BqTableMock) Create(ctx context.Context, meta *bigquery.TableMetadata) error {
	return table.Called(ctx, meta).Error(0)
}

func (table *BqTableMock) DatasetID() string {
	panic("not implemented")
}

func (table *BqTableMock) Delete(context.Context) error {
	panic("not implemented")
}

func (table *BqTableMock) ExtractorTo(dst *bigquery.GCSReference) bqiface.Extractor {
	panic("not implemented")
}

func (table *BqTableMock) FullyQualifiedName() string {
	panic("not implemented")
}

func (table *BqTableMock) LoaderFrom(bigquery.LoadSource) bqiface.Loader {
	panic("not implemented")
}

func (table *BqTableMock) Metadata(ctx context.Context) (*bigquery.TableMetadata, error) {
	args := table.Called(ctx)
	return args.Get(0).(*bigquery.TableMetadata), args.Error(1)
}

func (table *BqTableMock) ProjectID() string {
	panic("not implemented")
}

func (table *BqTableMock) Read(ctx context.Context) bqiface.RowIterator {
	panic("not implemented")
}

func (table *BqTableMock) TableID() string {
	panic("not implemented")
}

func (table *BqTableMock) Update(ctx context.Context, meta bigquery.TableMetadataToUpdate, etag string) (*bigquery.TableMetadata, error) {
	args := table.Called(ctx, meta, etag)
	return args.Get(0).(*bigquery.TableMetadata), args.Error(1)
}

func (table *BqTableMock) Uploader() bqiface.Uploader {
	panic("not implemented")
}
