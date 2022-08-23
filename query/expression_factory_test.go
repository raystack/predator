package query

import (
	"github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol/meta"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSQLExpressionFactory_CreatePartitionExpression(t *testing.T) {
	suites := []struct {
		Description   string
		TableSpec     *meta.TableSpec
		URN           string
		Expression    string
		MetadataError error
		ExpectError   bool
	}{
		{
			Description: "should return expression with DATE field type as partition and DAY as time partitioning ",
			TableSpec: &meta.TableSpec{
				TimePartitioningType: meta.DayPartitioning,
				PartitionField:       "field_date",
				Fields: []*meta.FieldSpec{
					{
						Name:      "field_date",
						FieldType: meta.FieldTypeDate,
					},
				},
			},
			URN:        "project.dataset.table",
			Expression: "field_date",
		},
		{
			Description: "should return expression with DATE field type as partition and MONTH as time partitioning ",
			TableSpec: &meta.TableSpec{
				TimePartitioningType: meta.MonthPartitioning,
				PartitionField:       "field_date",
				Fields: []*meta.FieldSpec{
					{
						Name:      "field_date",
						FieldType: meta.FieldTypeDate,
					},
				},
			},
			URN:        "project.dataset.table",
			Expression: "DATE_TRUNC(field_date,MONTH)",
		},
		{
			Description: "should return expression with DATE field type as partition and YEAR as time partitioning ",
			TableSpec: &meta.TableSpec{
				TimePartitioningType: meta.YearPartitioning,
				PartitionField:       "field_date",
				Fields: []*meta.FieldSpec{
					{
						Name:      "field_date",
						FieldType: meta.FieldTypeDate,
					},
				},
			},
			URN:        "project.dataset.table",
			Expression: "DATE_TRUNC(field_date,YEAR)",
		},
		{
			Description: "should return expression with TIMESTAMP field type as partition and DAY as time partitioning ",
			TableSpec: &meta.TableSpec{
				TimePartitioningType: meta.DayPartitioning,
				PartitionField:       "field_timestamp",
				Fields: []*meta.FieldSpec{
					{
						Name:      "field_timestamp",
						FieldType: meta.FieldTypeTimestamp,
					},
				},
			},
			URN:        "project.dataset.table",
			Expression: "DATE(field_timestamp,\"UTC\")",
		},
		{
			Description: "should return expression with TIMESTAMP field type as partition and HOUR as time partitioning ",
			TableSpec: &meta.TableSpec{
				TimePartitioningType: meta.HourPartitioning,
				PartitionField:       "field_timestamp",
				Fields: []*meta.FieldSpec{
					{
						Name:      "field_timestamp",
						FieldType: meta.FieldTypeTimestamp,
					},
				},
			},
			URN:        "project.dataset.table",
			Expression: "TIMESTAMP_TRUNC(field_timestamp,HOUR,\"UTC\")",
		},
		{
			Description: "should return expression with TIMESTAMP field type as partition and MONTH as time partitioning ",
			TableSpec: &meta.TableSpec{
				TimePartitioningType: meta.MonthPartitioning,
				PartitionField:       "field_timestamp",
				Fields: []*meta.FieldSpec{
					{
						Name:      "field_timestamp",
						FieldType: meta.FieldTypeTimestamp,
					},
				},
			},
			URN:        "project.dataset.table",
			Expression: "DATE_TRUNC(DATE(field_timestamp,\"UTC\"),MONTH)",
		},
		{
			Description: "should return expression with TIMESTAMP field type as partition and YEAR as time partitioning ",
			TableSpec: &meta.TableSpec{
				TimePartitioningType: meta.YearPartitioning,
				PartitionField:       "field_timestamp",
				Fields: []*meta.FieldSpec{
					{
						Name:      "field_timestamp",
						FieldType: meta.FieldTypeTimestamp,
					},
				},
			},
			URN:        "project.dataset.table",
			Expression: "DATE_TRUNC(DATE(field_timestamp,\"UTC\"),YEAR)",
		},
		{
			Description: "should return error with INTEGER field type as partition ",
			TableSpec: &meta.TableSpec{
				PartitionField: "customer_id",
				Fields: []*meta.FieldSpec{
					{
						Name:      "customer_id",
						FieldType: meta.FieldTypeInteger,
					},
				},
			},
			URN:         "project.dataset.table",
			Expression:  "",
			ExpectError: true,
		},
	}

	for _, test := range suites {
		t.Run(test.Description, func(t *testing.T) {
			metadataStore := mock.NewMetadataStore()
			defer metadataStore.AssertExpectations(t)

			metadataStore.On("GetMetadata", test.URN).Return(test.TableSpec, test.MetadataError)

			factory := NewSQLExpressionFactory(metadataStore)
			expression, err := factory.CreatePartitionExpression(test.URN)

			if test.MetadataError != nil || test.ExpectError {
				assert.Empty(t, expression)
				assert.Error(t, err)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, test.Expression, expression)
			}
		})
	}
}
