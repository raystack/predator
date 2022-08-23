package query

import (
	"fmt"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/meta"
	"github.com/odpf/predator/protocol/query"
)

//SQLExpressionFactory create custom SQL expression
type SQLExpressionFactory struct {
	metadataStore protocol.MetadataStore
}

func NewSQLExpressionFactory(metadataStore protocol.MetadataStore) *SQLExpressionFactory {
	return &SQLExpressionFactory{metadataStore: metadataStore}
}

//CreatePartitionExpression create sql expression of __PARTITION__ macros that used on profile group
//only supported table with DATE and TIMESTAMP field partition or default _PARTITIONTIME partition field
//Here is the example of generated expression:
//When the data type is DATE
//
//* time partitioning DAY
// 	date_column
//
//* time partitioning MONTH
//  DATE_TRUNC(date_column, MONTH)
//
//* time partitioning YEAR
//  DATE_TRUNC(date_column, YEAR)
//
//* time partitioning HOUR is not supported
//
//
//When the data type is TIMESTAMP
//* time partitioning DAY
//	DATE(ts_column, "UTC")
//
//* timepartioning HOUR
//  TIMESTAMP_TRUNC(ts_column, HOUR, "UTC")
//
//* timepartitioning MONTH
//  DATE_TRUNC(DATE(ts_column, "UTC"),MONTH)
//
//* timepartitioning YEAR
//  DATE_TRUNC(DATE(ts_column, "UTC"),YEAR)
func (p *SQLExpressionFactory) CreatePartitionExpression(urn string) (string, error) {
	tableSpec, err := p.metadataStore.GetMetadata(urn)
	if err != nil {
		return "", err
	}

	partitionField := tableSpec.PartitionField

	var fieldSpec *meta.FieldSpec
	if partitionField != meta.DefaultPartition {
		fieldSpec, err = tableSpec.GetFieldSpecByID(partitionField)
		if err != nil {
			if err == meta.ErrFieldSpecNotFound {
				err = fmt.Errorf("field ID: %s is not found on table : %s ,%w", partitionField, tableSpec.TableID(), err)
			}
			return "", err
		}
	} else {
		fieldSpec = meta.IngestionTimeField
	}

	if fieldSpec.FieldType != meta.FieldTypeDate && fieldSpec.FieldType != meta.FieldTypeTimestamp {
		return "", fmt.Errorf("partition by field type of %s is not supported", fieldSpec.FieldType)
	}

	if fieldSpec.FieldType == meta.FieldTypeDate {
		switch tableSpec.TimePartitioningType {
		case meta.DayPartitioning:
			ptExpression := &query.FieldIdentifier{
				FieldID: fieldSpec.Name,
			}
			return ptExpression.Build()
		case meta.MonthPartitioning:
			ptExpression := &query.DateTrunc{
				Target: &query.FieldIdentifier{
					FieldID: fieldSpec.Name,
				},
				TruncateType: query.TruncTypeMonth,
			}
			return ptExpression.Build()
		case meta.YearPartitioning:
			ptExpression := &query.DateTrunc{
				Target: &query.FieldIdentifier{
					FieldID: fieldSpec.Name,
				},
				TruncateType: query.TruncTypeYear,
			}
			return ptExpression.Build()
		}
	}

	switch tableSpec.TimePartitioningType {
	case meta.DayPartitioning:
		ptExpression := &query.Date{
			TimestampExpr: &query.FieldIdentifier{
				FieldID: fieldSpec.Name,
			},
		}
		return ptExpression.Build()
	case meta.HourPartitioning:
		ptExpression := &query.TimestampTrunc{
			TimestampExpr: &query.FieldIdentifier{
				FieldID: fieldSpec.Name,
			},
			TruncateType: query.TruncTypeHour,
		}
		return ptExpression.Build()
	case meta.MonthPartitioning:
		ptExpression := &query.DateTrunc{
			Target: &query.Date{
				TimestampExpr: &query.FieldIdentifier{
					FieldID: fieldSpec.Name,
				},
			},
			TruncateType: query.TruncTypeMonth,
		}
		return ptExpression.Build()
	case meta.YearPartitioning:
		ptExpression := &query.DateTrunc{
			Target: &query.Date{
				TimestampExpr: &query.FieldIdentifier{
					FieldID: fieldSpec.Name,
				},
			},
			TruncateType: query.TruncTypeYear,
		}
		return ptExpression.Build()
	}

	return "", nil
}
