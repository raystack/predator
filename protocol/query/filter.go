package query

import "fmt"

//FilterClause interface of types intended to do filter in where clause
type FilterClause interface {
	Build() string
	Equal(other FilterClause) bool
	implementFilter()
}

//DataType is type of data
type DataType string

const (
	//DataTypeTimestamp is timestamp datatype
	DataTypeTimestamp = "TIMESTAMP"
	//DataTypeDate is date datatype
	DataTypeDate = "DATE"
)

//PartitionFilter is filter for select data based on partition column
type PartitionFilter struct {
	DataType        DataType
	PartitionDate   string
	PartitionColumn string
}

func (pf *PartitionFilter) implementFilter() {}

//Build is a method to build sql expression of filtering based on partition
func (pf *PartitionFilter) Build() string {
	column := pf.PartitionColumn
	if pf.DataType == DataTypeTimestamp {
		column = fmt.Sprintf("DATE(%s)", pf.PartitionColumn)
	}
	return fmt.Sprintf("%s = '%s'", column, pf.PartitionDate)
}

//Equal implementation
func (pf *PartitionFilter) Equal(other FilterClause) bool {
	otherPf, ok := other.(*PartitionFilter)
	if !ok {
		return false
	}
	if *pf != *otherPf {
		return false
	}
	return true
}

//NoFilter is used to filter nothing on sql expression
type NoFilter struct {
}

func (nf *NoFilter) implementFilter() {}

//Build no filter clause
func (nf *NoFilter) Build() string {
	return "TRUE"
}

//Equal implementation
func (nf *NoFilter) Equal(other FilterClause) bool {
	_, ok := other.(*NoFilter)
	if !ok {
		return false
	}
	return true
}

//AllPartitionFilter is used to select all partition data
type AllPartitionFilter struct {
	PartitionColumn string
}

func (af *AllPartitionFilter) implementFilter() {}

//Build all partition filter clause
func (af *AllPartitionFilter) Build() string {
	return fmt.Sprintf("%s is not null", af.PartitionColumn)
}

//Equal implementation
func (af *AllPartitionFilter) Equal(other FilterClause) bool {
	otherAf, ok := other.(*AllPartitionFilter)
	if !ok {
		return false
	}
	if *af != *otherAf {
		return false
	}
	return true
}

//CustomFilterExpression filter by custom expression
type CustomFilterExpression struct {
	Expression string
}

func (c *CustomFilterExpression) Build() string {
	return c.Expression
}

func (c *CustomFilterExpression) Equal(other FilterClause) bool {
	o, ok := other.(*CustomFilterExpression)
	if !ok {
		return false
	}

	return c.Expression == o.Expression
}

func (c *CustomFilterExpression) implementFilter() {
}
