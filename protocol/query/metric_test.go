package query_test

import (
	"github.com/odpf/predator/protocol/query"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMetricExpression(t *testing.T) {
	t.Run("MetricExpression", func(t *testing.T) {
		t.Run("Build", func(t *testing.T) {
			t.Run("should return count metric expression", func(t *testing.T) {
				columnName := "status"
				aliasName := "count_status"
				metric := query.NewMetricExpression(columnName, aliasName, query.MetricTypeCount)

				expected := "count(status) as count_status"

				metricScript, _ := metric.Build()
				assert.Equal(t, expected, metricScript)
			})
			t.Run("should return ErrorMetricTypeNotFound when given unsupported metric type", func(t *testing.T) {
				columnName := "status"
				aliasName := "average_status"
				metric := query.NewMetricExpression(columnName, aliasName, "average")

				_, err := metric.Build()
				assert.Equal(t, query.ErrorMetricTypeNotFound, err)
			})
			t.Run("should return null count metric expression", func(t *testing.T) {
				columnName := "status"
				aliasName := "nullcount_status"
				metric := query.NewMetricExpression(columnName, aliasName, query.MetricTypeNullCount)

				expected := "countif(status is null) as nullcount_status"

				metricScript, _ := metric.Build()
				assert.Equal(t, expected, metricScript)
			})
			t.Run("should return sum metric expression", func(t *testing.T) {
				columnName := "status"
				aliasName := "sum_status"
				metric := query.NewMetricExpression(columnName, aliasName, query.MetricTypeSum)

				expected := "sum(cast(status as float64)) as sum_status"

				metricScript, _ := metric.Build()
				assert.Equal(t, expected, metricScript)
			})
			t.Run("should return count metric expression for repeated fields", func(t *testing.T) {
				columnName := "status"
				aliasName := "count_status"
				metric := query.NewMetricExpression(columnName, aliasName, query.MetricTypeCountForRepeated)

				expected := "countif(array_length(status)>0) as count_status"

				metricScript, _ := metric.Build()
				assert.Equal(t, expected, metricScript)
			})
			t.Run("should return count null metric expression for repeated fields", func(t *testing.T) {
				columnName := "status"
				aliasName := "nullcount_status"
				metric := query.NewMetricExpression(columnName, aliasName, query.MetricTypeNullCountForRepeated)

				expected := "countif(array_length(status)=0) as nullcount_status"

				metricScript, _ := metric.Build()
				assert.Equal(t, expected, metricScript)
			})
		})
	})
	t.Run("MetricExpressionList", func(t *testing.T) {
		t.Run("Build", func(t *testing.T) {
			t.Run("should return metric expression", func(t *testing.T) {
				metricExpressions := []*query.MetricExpression{
					query.NewMetricExpression("status", "count_status", query.MetricTypeCount),
					query.NewMetricExpression("status", "nullcount_status", query.MetricTypeNullCount),
				}

				expected := "count(status) as count_status , countif(status is null) as nullcount_status"

				metricScript, err := query.MetricExpressionList(metricExpressions).Build()

				assert.Nil(t, err)
				assert.Equal(t, expected, metricScript)
			})
			t.Run("should return error when unsupported metric on the list", func(t *testing.T) {
				metricExpressions := []*query.MetricExpression{
					query.NewMetricExpression("status", "count_status", query.MetricTypeCount),
					query.NewMetricExpression("status", "average_status", query.MetricType("AVERAGE")),
				}
				metricScript, err := query.MetricExpressionList(metricExpressions).Build()

				assert.Empty(t, metricScript)
				assert.Error(t, err)
			})
		})
	})
}
