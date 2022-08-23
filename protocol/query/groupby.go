package query

import "fmt"

//GroupByClause builder of group by expression
type GroupByClause interface {
	Build() string
	implementGroupBy()
}

var groupByTemplate = "GROUP BY %s"

//GroupByExpression is group by any kind of expression
// an Expression can be a column for example : GROUP BY created_date
// or can be an select expression for example : GROUP BY date(created_timestamp)
type GroupByExpression struct {
	Expression string
}

//Build build group by expression based on groupByTemplate template
func (g *GroupByExpression) Build() string {
	return fmt.Sprintf(groupByTemplate, g.Expression)
}

func (g *GroupByExpression) implementGroupBy() {
}

type NoGroupBy struct {
}

func (n *NoGroupBy) Build() string {
	return ""
}

func (n *NoGroupBy) implementGroupBy() {
}
