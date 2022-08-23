package protocol

import "errors"

var ErrPartitionExpressionIsNotSupported = errors.New("partition expression is not supported for this table")

//SQLExpressionFactory to generate SQL expression
type SQLExpressionFactory interface {
	CreatePartitionExpression(urn string) (string, error)
}
