package query_test

import (
	"testing"

	"github.com/odpf/predator/protocol/query"
	"github.com/stretchr/testify/assert"
)

func TestQuery(t *testing.T) {
	t.Run("Query", func(t *testing.T) {
		t.Run("Build", func(t *testing.T) {
			t.Run("should generate sql one metric given one metric config ", func(t *testing.T) {
				q := &query.Query{
					Metrics: []*query.MetricExpression{
						query.NewMetricExpression("unnest1.field_child", "count_field_root_field_child", query.MetricTypeCount),
					},
					From: &query.FromClause{
						TableID: "project.dataset.table",
						UnnestClauses: []*query.Unnest{
							{
								ColumnName: "field_root",
								Alias:      "unnest1",
							},
						},
					},
					Where: &query.PartitionFilter{
						DataType:        query.DataTypeDate,
						PartitionDate:   "2019-01-01",
						PartitionColumn: "_PARTITIONDATE",
					}}
				sql := q.String()
				expected := "SELECT " +
					"count(unnest1.field_child) as count_field_root_field_child " +
					"FROM `project.dataset.table` , UNNEST(field_root) as unnest1 " +
					"WHERE _PARTITIONDATE = '2019-01-01'"
				assert.Equal(t, expected, sql)
			})
			t.Run("should generate sql one metric given more than one metric config ", func(t *testing.T) {
				q := &query.Query{
					Metrics: []*query.MetricExpression{
						query.NewMetricExpression("field1", "count_field1", query.MetricTypeCount),
						query.NewMetricExpression("field2", "nullcount_field2", query.MetricTypeNullCount),
					},
					From: &query.FromClause{
						TableID: "project.dataset.table",
					},
					Where: &query.PartitionFilter{
						DataType:        query.DataTypeDate,
						PartitionDate:   "2019-01-01",
						PartitionColumn: "_PARTITIONDATE",
					},
				}
				sql := q.String()

				expected := "SELECT " +
					"count(field1) as count_field1 , " +
					"countif(field2 is null) as nullcount_field2 " +
					"FROM `project.dataset.table` " +
					"WHERE _PARTITIONDATE = '2019-01-01'"
				assert.Equal(t, expected, sql)
			})
			t.Run("should generate sql one using select expression, custom group by and filter expression ", func(t *testing.T) {
				q := &query.Query{
					Expressions: []*query.SelectExpression{
						{
							Expression: "field_grouping",
							Alias:      "__group_value",
						},
					},
					Metrics: []*query.MetricExpression{
						query.NewMetricExpression("field1", "count_field1", query.MetricTypeCount),
					},
					From: &query.FromClause{
						TableID: "project.dataset.table",
					},
					Where: &query.CustomFilterExpression{
						Expression: "active = true",
					},
					GroupBy: &query.GroupByExpression{Expression: "field_grouping"},
				}
				sql := q.String()

				expected := "SELECT " +
					"field_grouping AS __group_value , " +
					"count(field1) as count_field1 " +
					"FROM `project.dataset.table` " +
					"WHERE active = true " +
					"GROUP BY field_grouping"
				assert.Equal(t, expected, sql)
			})
			t.Run("should generate sql with empty select expression ", func(t *testing.T) {
				var selExps []*query.SelectExpression
				q := &query.Query{
					Expressions: selExps,
					Metrics: []*query.MetricExpression{
						query.NewMetricExpression("unnest1.field_child", "count_field_root_field_child", query.MetricTypeCount),
					},
					From: &query.FromClause{
						TableID: "project.dataset.table",
						UnnestClauses: []*query.Unnest{
							{
								ColumnName: "field_root",
								Alias:      "unnest1",
							},
						},
					},
					Where: &query.PartitionFilter{
						DataType:        query.DataTypeDate,
						PartitionDate:   "2019-01-01",
						PartitionColumn: "_PARTITIONDATE",
					}}
				sql := q.String()
				expected := "SELECT " +
					"count(unnest1.field_child) as count_field_root_field_child " +
					"FROM `project.dataset.table` , UNNEST(field_root) as unnest1 " +
					"WHERE _PARTITIONDATE = '2019-01-01'"
				assert.Equal(t, expected, sql)
			})
		})
		t.Run("Merge", func(t *testing.T) {
			t.Run("should return merged query", func(t *testing.T) {
				q1 := &query.Query{
					Metrics: []*query.MetricExpression{
						query.NewMetricExpression("field1", "count_field1", query.MetricTypeCount),
					}, From: &query.FromClause{
						TableID: "project.dataset.table",
					}, Where: &query.PartitionFilter{
						DataType:        query.DataTypeDate,
						PartitionDate:   "2019-01-01",
						PartitionColumn: "_PARTITIONDATE",
					}}

				q2 := &query.Query{
					Metrics: []*query.MetricExpression{
						query.NewMetricExpression("field2", "nullcount_field2", query.MetricTypeNullCount),
					}, From: &query.FromClause{
						TableID: "project.dataset.table",
					}, Where: &query.PartitionFilter{
						DataType:        query.DataTypeDate,
						PartitionDate:   "2019-01-01",
						PartitionColumn: "_PARTITIONDATE",
					}}

				mergedQuery, _ := q1.Merge(q2)

				sql := mergedQuery.String()

				expected := "SELECT " +
					"count(field1) as count_field1 , " +
					"countif(field2 is null) as nullcount_field2 " +
					"FROM `project.dataset.table` " +
					"WHERE _PARTITIONDATE = '2019-01-01'"
				assert.Equal(t, expected, sql)
			})
			t.Run("should return error when FilterClause is Different ", func(t *testing.T) {
				q1 := &query.Query{
					Metrics: []*query.MetricExpression{
						query.NewMetricExpression("field1", "count_field1", query.MetricTypeCount),
					},
					From: &query.FromClause{
						TableID: "project.dataset.table",
					},
					Where: &query.PartitionFilter{
						DataType:        query.DataTypeDate,
						PartitionDate:   "2019-01-01",
						PartitionColumn: "_PARTITIONDATE",
					}}

				q2 := &query.Query{
					Metrics: []*query.MetricExpression{
						query.NewMetricExpression("field2", "nullcount_field2", query.MetricTypeNullCount),
					},
					From: &query.FromClause{
						TableID: "project.dataset.table",
					},
					Where: &query.PartitionFilter{
						DataType:        query.DataTypeDate,
						PartitionDate:   "2019-01-02",
						PartitionColumn: "_PARTITIONDATE",
					},
				}

				_, err := q1.Merge(q2)
				assert.NotNil(t, err)
			})
			t.Run("should return error when FromClause is Different ", func(t *testing.T) {
				q1 := &query.Query{
					Metrics: []*query.MetricExpression{
						query.NewMetricExpression("field1", "count_field1", query.MetricTypeCount),
					},
					From: &query.FromClause{
						TableID: "project.dataset.table1",
					},
					Where: &query.PartitionFilter{
						DataType:        query.DataTypeDate,
						PartitionDate:   "2019-01-01",
						PartitionColumn: "_PARTITIONDATE",
					},
				}

				q2 := &query.Query{
					Metrics: []*query.MetricExpression{
						query.NewMetricExpression("field2", "nullcount_field2", query.MetricTypeNullCount),
					},
					From: &query.FromClause{
						TableID: "project.dataset.table2",
					},
					Where: &query.PartitionFilter{
						DataType:        query.DataTypeDate,
						PartitionDate:   "2019-01-01",
						PartitionColumn: "_PARTITIONDATE",
					},
				}

				_, err := q1.Merge(q2)
				assert.NotNil(t, err)
			})
		})
	})
}

func TestSelectExpression(t *testing.T) {
	t.Run("should generate select expressions", func(t *testing.T) {
		expressions := []*query.SelectExpression{
			{
				Expression: "date(field_timestamp)",
			},
			{
				Expression: "field_grouping",
				Alias:      "__group_value",
			},
		}

		expected := "date(field_timestamp) , field_grouping AS __group_value"

		result := query.SelectExpressionList(expressions).Build()

		assert.Equal(t, expected, result)
	})
}
