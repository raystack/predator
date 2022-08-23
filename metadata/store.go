package metadata

import (
	"context"
	"fmt"
	"github.com/odpf/predator/protocol/meta"
	"google.golang.org/api/googleapi"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/odpf/predator/protocol"
)

//Store store to get table metadata
type Store struct {
	bqClient        bqiface.Client
	constraintStore protocol.ConstraintStore
}

//NewStore is constructor of MetadataStore
func NewStore(client bqiface.Client, constraintStore protocol.ConstraintStore) *Store {
	return &Store{client, constraintStore}
}

//GetMetadata to get metadata
func (store *Store) GetMetadata(tableID string) (*meta.TableSpec, error) {
	urnsSegments := strings.Split(tableID, ".")

	if len(urnsSegments) != 3 {
		return nil, fmt.Errorf("wrong format of urn %s. expected ${project-id}.${dataset}.${table_name}", tableID)
	}

	projectName := urnsSegments[0]
	datasetName := urnsSegments[1]
	tableName := urnsSegments[2]

	tableMetadata, err := store.getTableMetadata(projectName, datasetName, tableName)
	if err != nil {
		return nil, err
	}

	tableSpec := &meta.TableSpec{
		ProjectName: projectName,
		DatasetName: datasetName,
		TableName:   tableName,
	}
	tableSpec.PartitionField = getPartitionField(tableMetadata.TimePartitioning)
	tableSpec.Labels = tableMetadata.Labels
	tableSpec.Fields = transformFields(tableMetadata.Schema)

	tableSpec.RequirePartitionFilter = tableMetadata.RequirePartitionFilter

	if tableMetadata.TimePartitioning != nil {
		timePartitioningType, err := convertTimePartitioningType(tableMetadata.TimePartitioning.Type)
		if err != nil {
			return nil, err
		}
		tableSpec.TimePartitioningType = timePartitioningType
	}

	return tableSpec, nil
}

//GetUniqueConstraints to fetch unique constraints
func (store *Store) GetUniqueConstraints(tableID string) ([]string, error) {
	return store.constraintStore.FetchConstraints(tableID)
}

func convertTimePartitioningType(_type bigquery.TimePartitioningType) (meta.TimePartitioning, error) {
	switch _type {
	case bigquery.DayPartitioningType:
		return meta.DayPartitioning, nil
	case bigquery.HourPartitioningType:
		return meta.HourPartitioning, nil
	case bigquery.MonthPartitioningType:
		return meta.MonthPartitioning, nil
	case bigquery.YearPartitioningType:
		return meta.YearPartitioning, nil
	default:
	}

	return "", fmt.Errorf("type unsupported %s", _type)
}

func (store *Store) getTableMetadata(projectName, datasetName, tableName string) (*bigquery.TableMetadata, error) {
	dataset := store.bqClient.DatasetInProject(projectName, datasetName)
	table := dataset.Table(tableName)

	tableMetadata, err := table.Metadata(context.Background())
	if e, ok := err.(*googleapi.Error); ok {
		if e.Code == 404 {
			return nil, protocol.ErrTableMetadataNotFound
		}
	}

	if err != nil {
		return nil, err
	}

	return tableMetadata, nil
}

func getPartitionField(timePartitioning *bigquery.TimePartitioning) string {
	if timePartitioning != nil {
		if len(timePartitioning.Field) == 0 {
			return meta.DefaultPartition
		}
		return timePartitioning.Field
	}
	return ""
}

func transformFields(schema bigquery.Schema) []*meta.FieldSpec {
	level := meta.RootLevel
	fieldSchemas := []*bigquery.FieldSchema(schema)
	var fieldSpecs []*meta.FieldSpec
	for _, fieldSchema := range fieldSchemas {
		fieldSpec := createFieldSpec(fieldSchema, nil, level)
		fieldSpecs = append(fieldSpecs, fieldSpec)
	}

	return fieldSpecs
}

func createFieldSpec(fieldSchema *bigquery.FieldSchema, parent *meta.FieldSpec, level int) *meta.FieldSpec {
	childs := []*bigquery.FieldSchema(fieldSchema.Schema)
	var fieldSpecs []*meta.FieldSpec

	current := &meta.FieldSpec{
		Name:      fieldSchema.Name,
		FieldType: fieldType(fieldSchema.Type),
		Mode:      fieldMode(fieldSchema.Repeated, fieldSchema.Required),
		Parent:    parent,
		Level:     level,
	}

	if len(childs) > 0 {
		for _, fieldSchema := range childs {
			fieldSpec := createFieldSpec(fieldSchema, current, level+1)
			fieldSpecs = append(fieldSpecs, fieldSpec)
		}
	}

	current.Fields = fieldSpecs
	return current
}

func fieldMode(repeated bool, required bool) meta.Mode {
	if repeated {
		return meta.ModeRepeated
	}
	if required {
		return meta.ModeRequired
	}
	return meta.ModeNullable
}

func fieldType(fieldType bigquery.FieldType) meta.FieldType {
	switch fieldType {
	case bigquery.BytesFieldType:
		return meta.FieldTypeBytes
	case bigquery.IntegerFieldType:
		return meta.FieldTypeInteger
	case bigquery.FloatFieldType:
		return meta.FieldTypeFloat
	case bigquery.BooleanFieldType:
		return meta.FieldTypeBoolean
	case bigquery.TimestampFieldType:
		return meta.FieldTypeTimestamp
	case bigquery.RecordFieldType:
		return meta.FieldTypeRecord
	case bigquery.DateFieldType:
		return meta.FieldTypeDate
	case bigquery.TimeFieldType:
		return meta.FieldTypeTime
	case bigquery.DateTimeFieldType:
		return meta.FieldTypeDateTime
	case bigquery.NumericFieldType:
		return meta.FieldTypeNumeric
	case bigquery.GeographyFieldType:
		return meta.FieldTypeGeography
	default:
		return meta.FieldTypeString
	}
}
