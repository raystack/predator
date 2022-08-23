package common

import (
	"github.com/odpf/predator/protocol/meta"
	"github.com/odpf/predator/protocol/query"
)

func GenerateFilterExpression(filter string, tableSpec *meta.TableSpec) query.FilterClause {
	var filterExpression query.FilterClause
	if filter != "" {
		filterExpression = &query.CustomFilterExpression{
			Expression: filter,
		}
	} else {
		if tableSpec.RequirePartitionFilter {
			filterExpression = &query.AllPartitionFilter{PartitionColumn: tableSpec.PartitionField}
		} else {
			filterExpression = &query.NoFilter{}
		}
	}
	return filterExpression
}

func GenerateGroupExpression(groupName string) query.GroupByClause {
	var groupByExpression query.GroupByClause
	if groupName != "" {
		groupByExpression = &query.GroupByExpression{Expression: groupName}
	}
	return groupByExpression
}

func GenerateSelectExpression(groupName string) []*query.SelectExpression {
	var selectExpressions []*query.SelectExpression
	if groupName != "" {
		exp := &query.SelectExpression{
			Expression: groupName,
			Alias:      GroupAlias,
		}
		selectExpressions = append(selectExpressions, exp)
	}
	return selectExpressions
}
