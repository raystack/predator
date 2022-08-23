package metadata_test

import (
	"context"
	"testing"

	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/meta"
	"google.golang.org/api/googleapi"

	"cloud.google.com/go/bigquery"
	"github.com/odpf/predator/metadata"
	"github.com/stretchr/testify/assert"
)

func TestGetMetadata(t *testing.T) {
	suites := []struct {
		Description       string
		URN               string
		TableMeta         *bigquery.TableMetadata
		UniqueConstraints []string
		GetMetadataErr    error
		TableSpec         *meta.TableSpec
		ExpectedErr       error
	}{
		{
			Description: "should return metadata",
			URN:         "test-project.dataset.table",
			TableMeta: &bigquery.TableMetadata{
				Name: "table",
				TimePartitioning: &bigquery.TimePartitioning{
					Field: "field_1",
					Type:  bigquery.DayPartitioningType,
				},
				Schema: []*bigquery.FieldSchema{
					{
						Name:     "field_1",
						Repeated: false,
						Required: false,
						Type:     bigquery.StringFieldType,
					},
				},
				Labels: map[string]string{
					"key": "value",
				},
				RequirePartitionFilter: true,
			},
			UniqueConstraints: []string{"id"},
			TableSpec: &meta.TableSpec{
				ProjectName:            "test-project",
				DatasetName:            "dataset",
				TableName:              "table",
				PartitionField:         "field_1",
				RequirePartitionFilter: true,
				Labels: map[string]string{
					"key": "value",
				},
				Fields: []*meta.FieldSpec{
					{
						Name:      "field_1",
						FieldType: meta.FieldTypeString,
						Mode:      meta.ModeNullable,
						Level:     1,
					},
				},
				TimePartitioningType: meta.DayPartitioning,
			},
		},
		{
			Description: "should return ErrTableMetadataNotFound when object not found",
			URN:         "test-project.dataset.table",
			TableMeta:   &bigquery.TableMetadata{},
			GetMetadataErr: &googleapi.Error{
				Code: 404,
			},
			ExpectedErr: protocol.ErrTableMetadataNotFound,
		}, {
			Description: "should return metadata for table with repeated fields and tree level info",
			URN:         "test-project.dataset.table",
			TableMeta: &bigquery.TableMetadata{
				Name: "table",
				TimePartitioning: &bigquery.TimePartitioning{
					Field: "field_1",
					Type:  bigquery.DayPartitioningType,
				},
				Schema: []*bigquery.FieldSchema{
					{
						Name:     "field_1",
						Repeated: false,
						Required: false,
						Type:     bigquery.StringFieldType,
					},
					{
						Name:     "field_2",
						Repeated: true,
						Required: false,
						Type:     bigquery.RecordFieldType,
						Schema: []*bigquery.FieldSchema{
							{
								Name:     "field_2_child_1",
								Repeated: false,
								Required: false,
								Type:     bigquery.FloatFieldType,
							},
						},
					},
				},
				Labels: map[string]string{
					"key": "value",
				},
			},
			TableSpec: &meta.TableSpec{
				ProjectName:          "test-project",
				DatasetName:          "dataset",
				TableName:            "table",
				PartitionField:       "field_1",
				TimePartitioningType: meta.DayPartitioning,
				Labels: map[string]string{
					"key": "value",
				},
				Fields: func() []*meta.FieldSpec {
					parent := &meta.FieldSpec{
						Name:      "field_2",
						FieldType: meta.FieldTypeRecord,
						Mode:      meta.ModeRepeated,
						Level:     1,
					}

					child := &meta.FieldSpec{
						Name:      "field_2_child_1",
						FieldType: meta.FieldTypeFloat,
						Mode:      meta.ModeNullable,
						Fields:    nil,
						Parent:    parent,
						Level:     2,
					}

					parent.Fields = []*meta.FieldSpec{child}

					expectedFields := []*meta.FieldSpec{
						{
							Name:      "field_1",
							FieldType: meta.FieldTypeString,
							Mode:      meta.ModeNullable,
							Fields:    nil,
							Level:     1,
						},
						parent,
					}

					return expectedFields
				}(),
			},
		},
	}
	for _, test := range suites {
		t.Run(test.Description, func(t *testing.T) {
			// Create mocks
			bqClientMock := new(BqClientMock)
			defer bqClientMock.AssertExpectations(t)

			bqDatasetMock := new(BqDatasetMock)
			defer bqDatasetMock.AssertExpectations(t)

			bqTableMock := new(BqTableMock)
			defer bqTableMock.AssertExpectations(t)

			constraintStore := &SpyConstraintStore{}
			defer constraintStore.AssertExpectations(t)

			// Mock function calls
			bqClientMock.On("DatasetInProject", "test-project", "dataset").Return(bqDatasetMock)
			bqDatasetMock.On("Table", "table").Return(bqTableMock)
			bqTableMock.On("Metadata", context.Background()).Return(test.TableMeta, test.GetMetadataErr)

			store := metadata.NewStore(bqClientMock, constraintStore)
			actual, err := store.GetMetadata(test.URN)

			if test.ExpectedErr != nil {
				assert.Nil(t, actual)
				assert.Error(t, err)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, test.TableSpec, actual)
			}
		})
	}

	t.Run("GetUniqueConstraints", func(t *testing.T) {
		t.Run("should return unique constraints when table id found", func(t *testing.T) {
			urn := "test-project.dataset.table"
			uniqueConstraints := []string{"field1", "field2"}

			bqClientMock := new(BqClientMock)
			defer bqClientMock.AssertExpectations(t)

			constraintStore := &SpyConstraintStore{}
			defer constraintStore.AssertExpectations(t)
			constraintStore.On("FetchConstraints", urn).Return(uniqueConstraints, nil)

			store := metadata.NewStore(bqClientMock, constraintStore)
			actualUniqueConstraints, err := store.GetUniqueConstraints(urn)

			assert.Nil(t, err)
			assert.Equal(t, uniqueConstraints, actualUniqueConstraints)
		})

		t.Run("should return error when table id not found", func(t *testing.T) {
			urn := "test-project.dataset.table"

			bqClientMock := new(BqClientMock)
			defer bqClientMock.AssertExpectations(t)

			constraintStore := &SpyConstraintStore{}
			defer constraintStore.AssertExpectations(t)
			constraintStore.On("FetchConstraints", urn).Return([]string{}, protocol.ErrUniqueConstraintNotFound)

			store := metadata.NewStore(bqClientMock, constraintStore)
			actualUniqueConstraints, err := store.GetUniqueConstraints(urn)

			assert.Equal(t, []string{}, actualUniqueConstraints)
			assert.Equal(t, protocol.ErrUniqueConstraintNotFound, err)
		})
	})
}
