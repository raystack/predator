package query

import (
	"errors"
	"fmt"
	"strings"
)

const defaultExpressionSeparator = " , "

type SelectExpression struct {
	Expression string
	Alias      string
}

func (s *SelectExpression) Build() string {
	if s.Alias != "" {
		return fmt.Sprintf("%s AS %s", s.Expression, s.Alias)
	}
	return s.Expression
}

type SelectExpressionList []*SelectExpression

func (s SelectExpressionList) Build() string {
	var expressionString []string
	for _, exp := range []*SelectExpression(s) {
		s := exp.Build()
		expressionString = append(expressionString, s)
	}
	return strings.Join(expressionString, defaultExpressionSeparator)
}

var sqlTemplate = "SELECT %s FROM %s WHERE %s"

//Query is an SQL query
type Query struct {
	Expressions SelectExpressionList
	Metrics     MetricExpressionList
	From        *FromClause
	Where       FilterClause

	//GroupBy is optional
	GroupBy GroupByClause
}

//NewQuery is a function to build a sql to calculate metric
func (q *Query) String() string {
	fromExpression := q.From.Build()
	whereExpression := q.Where.Build()

	//sequence of select expression
	var expressions []string

	if len(q.Expressions) > 0 {
		selectExpression := q.Expressions.Build()
		expressions = append(expressions, selectExpression)
	}

	if len(q.Metrics) > 0 {
		metricsExpression, _ := q.Metrics.Build()
		expressions = append(expressions, metricsExpression)
	}

	queryExpression := strings.Join(expressions, defaultExpressionSeparator)

	sql := fmt.Sprintf(sqlTemplate, queryExpression, fromExpression, whereExpression)

	if q.GroupBy != nil {
		groupByExpression := q.GroupBy.Build()
		sql = fmt.Sprintf("%s %s", sql, groupByExpression)
	}

	return sql
}

//Merge is to merge two queries into one
//will result new Query that contains both of the queries metrics
//will return error when the Where and FromClause from both of the queries is different
func (q *Query) Merge(other *Query) (*Query, error) {

	if !q.Where.Equal(other.Where) {
		return nil, errors.New("unable to join query with different FilterClause")
	}

	if !q.From.Equal(other.From) {
		return nil, errors.New("unable to join query with different FromClause")
	}

	var combinedMetrics []*MetricExpression
	combinedMetrics = append(combinedMetrics, q.Metrics...)
	combinedMetrics = append(combinedMetrics, other.Metrics...)

	merged := &Query{
		Metrics: combinedMetrics,
		Where:   q.Where,
		From:    q.From,
	}

	return merged, nil
}
