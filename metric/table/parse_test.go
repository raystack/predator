package table

import (
	"github.com/odpf/predator/protocol/query"
	"testing"

	"cloud.google.com/go/civil"
	"github.com/odpf/predator/metric/common"

	"github.com/odpf/predator/protocol/metric"

	"github.com/stretchr/testify/assert"
)

func TestParse(t *testing.T) {
	t.Run("parseRowToMetrics", func(t *testing.T) {
		t.Run("should parse table profiling result", func(t *testing.T) {
			queryResult := make(map[string]interface{})
			queryResult["count_0"] = int64(100)
			queryResult["uniquecount_1"] = int64(50)
			queryResult["invalidcount_2"] = int64(0)
			queryResult[common.GroupAlias] = civil.Date{
				Year:  2012,
				Month: 12,
				Day:   1,
			}

			condition := "sample_field1 - sample_field2 - sample_field3 - sample_field4 != 0"

			pairs := []*common.SpecExpressionPair{
				{
					MetricSpec: &metric.Spec{
						Name:    metric.Count,
						TableID: "sample-project.sample_dataset.sample_table",
						Owner:   metric.Table,
					},
					MetricExpression: &query.MetricExpression{
						Alias: "count_0",
					},
				},
				{
					MetricSpec: &metric.Spec{
						Name:    metric.UniqueCount,
						TableID: "sample-project.sample_dataset.sample_table",
						Owner:   metric.Table,
					},
					MetricExpression: &query.MetricExpression{
						Alias: "uniquecount_1",
					},
				},
				{
					MetricSpec: &metric.Spec{
						Name:      metric.InvalidCount,
						TableID:   "sample-project.sample_dataset.sample_table",
						Condition: condition,
						Owner:     metric.Table,
					},
					MetricExpression: &query.MetricExpression{
						Alias: "invalidcount_2",
					},
				},
			}

			expected := []*metric.Metric{
				{
					Type:       metric.Count,
					Category:   metric.Basic,
					Owner:      metric.Table,
					Value:      float64(100),
					GroupValue: "2012-12-01",
				},
				{
					Type:       metric.UniqueCount,
					Category:   metric.Basic,
					Owner:      metric.Table,
					Value:      float64(50),
					GroupValue: "2012-12-01",
				},
				{
					Type:       metric.InvalidCount,
					Category:   metric.Basic,
					Owner:      metric.Table,
					Value:      float64(0),
					Condition:  condition,
					GroupValue: "2012-12-01",
				},
			}

			parser := &common.QueryResultParser{ParserMap: metricParserMap}

			actual, err := parser.Parse(queryResult, pairs)

			assert.Nil(t, err)
			assert.ElementsMatch(t, expected, actual)
		})
	})
}
