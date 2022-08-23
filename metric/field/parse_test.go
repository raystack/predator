package field

import (
	"cloud.google.com/go/civil"
	"github.com/odpf/predator/metric/common"
	"github.com/odpf/predator/protocol/query"
	"testing"

	"github.com/odpf/predator/protocol/metric"

	"github.com/stretchr/testify/assert"
)

func TestParse(t *testing.T) {
	t.Run("ParseRowToMetrics", func(t *testing.T) {
		t.Run("should return invalidity metrics", func(t *testing.T) {
			condition1 := "field_1 <= 0"
			condition2 := "field_1 < field_2"

			pairs := []*common.SpecExpressionPair{
				{
					MetricSpec: &metric.Spec{
						Name:    metric.Count,
						FieldID: "field_1",
						TableID: "entity-1-project-1.dataset_a.table_x",
					},
					MetricExpression: &query.MetricExpression{
						Alias: "count_field_1_0",
					},
				},
				{
					MetricSpec: &metric.Spec{
						Name:      metric.InvalidCount,
						FieldID:   "field_1",
						TableID:   "entity-1-project-1.dataset_a.table_x",
						Condition: condition1,
					},
					MetricExpression: &query.MetricExpression{
						Alias: "invalidcount_field_1_1",
					},
				},
				{
					MetricSpec: &metric.Spec{
						Name:      metric.InvalidCount,
						FieldID:   "field_1",
						TableID:   "entity-1-project-1.dataset_a.table_x",
						Condition: condition2,
					},
					MetricExpression: &query.MetricExpression{
						Alias: "invalidcount_field_1_2",
					},
				},
			}

			queryResult := make(map[string]interface{})
			queryResult["count_field_1_0"] = int64(100)
			queryResult["invalidcount_field_1_1"] = int64(0)
			queryResult["invalidcount_field_1_2"] = int64(0)
			queryResult[common.GroupAlias] = civil.Date{
				Year:  2012,
				Month: 12,
				Day:   1,
			}

			expected := []*metric.Metric{
				{
					Type:       metric.Count,
					Category:   metric.Basic,
					Owner:      metric.Field,
					GroupValue: "2012-12-01",
					FieldID:    "field_1",
					Value:      float64(100),
				},
				{
					Type:       metric.InvalidCount,
					Category:   metric.Basic,
					Owner:      metric.Field,
					FieldID:    "field_1",
					GroupValue: "2012-12-01",
					Value:      float64(0),
					Condition:  condition1,
				},
				{
					Type:       metric.InvalidCount,
					Category:   metric.Basic,
					Owner:      metric.Field,
					FieldID:    "field_1",
					GroupValue: "2012-12-01",
					Value:      float64(0),
					Condition:  condition2,
				},
			}

			parser := &common.QueryResultParser{ParserMap: metricParserMap}

			actual, err := parser.Parse(queryResult, pairs)

			assert.ElementsMatch(t, expected, actual)
			assert.Nil(t, err)
		})
		t.Run("sum value with alias not found case", func(t *testing.T) {

			pairs := []*common.SpecExpressionPair{
				{
					MetricSpec: &metric.Spec{
						Name:    metric.Sum,
						FieldID: "field_2",
						TableID: "entity-1-project-1.dataset_a.table_x",
					},
					MetricExpression: &query.MetricExpression{
						Alias: "sum_field_2_0",
					},
				},
			}

			queryResult := make(map[string]interface{})

			parser := &common.QueryResultParser{ParserMap: metricParserMap}

			actual, err := parser.Parse(queryResult, pairs)
			assert.Equal(t, "sum value with alias sum_field_2_0, not found", err.Error())
			assert.Nil(t, actual)
		})
		t.Run("sum value with parsing error case", func(t *testing.T) {

			pairs := []*common.SpecExpressionPair{
				{
					MetricSpec: &metric.Spec{
						Name:    metric.Sum,
						FieldID: "field_2",
						TableID: "entity-1-project-1.dataset_a.table_x",
					},
					MetricExpression: &query.MetricExpression{
						Alias: "sum_field_2_0",
					},
				},
			}

			queryResult := make(map[string]interface{})
			queryResult["sum_field_2_0"] = "some_random_string"

			parser := &common.QueryResultParser{ParserMap: metricParserMap}

			actual, err := parser.Parse(queryResult, pairs)
			assert.Equal(t, "parse sum value to float64 with alias sum_field_2_0, failed", err.Error())
			assert.Nil(t, actual)
		})
		t.Run("should return sum value", func(t *testing.T) {

			pairs := []*common.SpecExpressionPair{
				{
					MetricSpec: &metric.Spec{
						Name:    metric.Sum,
						FieldID: "field_2",
						TableID: "entity-1-project-1.dataset_a.table_x",
					},
					MetricExpression: &query.MetricExpression{
						Alias: "sum_field_2_0",
					},
				},
			}

			queryResult := make(map[string]interface{})
			queryResult["sum_field_2_0"] = float64(100)

			expectedResponse := []*metric.Metric{
				{
					Type:     metric.Sum,
					Category: metric.Quality,
					Owner:    metric.Field,
					FieldID:  "field_2",
					Value:    float64(100),
				},
			}

			parser := &common.QueryResultParser{ParserMap: metricParserMap}

			actual, err := parser.Parse(queryResult, pairs)
			assert.ElementsMatch(t, expectedResponse, actual)
			assert.Nil(t, err)
		})
	})
}
