package query

import (
	"errors"
	"fmt"
	"github.com/odpf/predator/protocol/meta"
	"github.com/odpf/predator/protocol/metric"
	"strings"
)

//MetricType is type of metric
type MetricType string

const (
	//MetricTypeCount is metric type of count
	MetricTypeCount = "COUNT"
	//MetricTypeNullCount is metric type of null count
	MetricTypeNullCount = "NULLCOUNT"
	//MetricTypeSum is metric type of sum
	MetricTypeSum = "SUM"
	//MetricTypeCountForRepeated is metric type of count
	MetricTypeCountForRepeated = "COUNTFORREPEATED"
	//MetricTypeNullCountForRepeated is metric type of count
	MetricTypeNullCountForRepeated = "NULLCOUNTFORREPEATED"
	//MetricTypeUniqueCount is metric type of unique count
	MetricTypeUniqueCount = "UNIQUECOUNT"
	//MetricTypeInvalidCount is metric type of invalid count
	MetricTypeInvalidCount = "INVALIDCOUNT"
)

//ParseMetricType to parse metric name to metric type
func ParseMetricType(_type metric.Type, mode meta.Mode) MetricType {
	var metricType MetricType
	if _type == metric.Count && mode == meta.ModeRepeated {
		metricType = MetricTypeCountForRepeated
	} else if _type == metric.Count && (mode == meta.ModeNullable || mode == meta.ModeRequired) {
		metricType = MetricTypeCount
	} else if _type == metric.NullCount && mode == meta.ModeRepeated {
		metricType = MetricTypeNullCountForRepeated
	} else if _type == metric.NullCount && (mode == meta.ModeNullable || mode == meta.ModeRequired) {
		metricType = MetricTypeNullCount
	} else if _type == metric.Sum {
		metricType = MetricTypeSum
	} else if _type == metric.UniqueCount {
		metricType = MetricTypeUniqueCount
	} else if _type == metric.InvalidCount {
		metricType = MetricTypeInvalidCount
	}
	return metricType
}

var metricTypeExpressionTemplate = map[MetricType]string{
	MetricTypeCount:                "count(%s) as %s",
	MetricTypeNullCount:            "countif(%s is null) as %s",
	MetricTypeSum:                  "sum(cast(%s as float64)) as %s",
	MetricTypeCountForRepeated:     "countif(array_length(%s)>0) as %s",
	MetricTypeNullCountForRepeated: "countif(array_length(%s)=0) as %s",
	MetricTypeUniqueCount:          "count(distinct %s) as %s",
	MetricTypeInvalidCount:         "countif(%s) as %s",
}

//ErrorMetricTypeNotFound is error when  metric type is undefined, or not found from the list
var ErrorMetricTypeNotFound = errors.New("error : Metric type is not found")

//MetricExpression is definition type of calculation, and the alias produced of from a column
type MetricExpression struct {
	Arg        string
	Alias      string
	MetricType MetricType
}

//Build is process of constructing script from metric definition
func (m *MetricExpression) Build() (string, error) {
	metricTemplate, ok := metricTypeExpressionTemplate[m.MetricType]
	if !ok {
		return "", ErrorMetricTypeNotFound
	}
	return fmt.Sprintf(metricTemplate, m.Arg, m.Alias), nil
}

//NewMetricExpression is constructor for metric expression
func NewMetricExpression(arg string, alias string, metricType MetricType) *MetricExpression {
	return &MetricExpression{
		Arg:        arg,
		Alias:      alias,
		MetricType: metricType,
	}
}

//MetricExpressionList list of metric expression
type MetricExpressionList []*MetricExpression

//Build build each metric expression and join them
func (m MetricExpressionList) Build() (string, error) {
	var metricList []string
	for _, me := range []*MetricExpression(m) {
		metricExpression, err := me.Build()
		if err != nil {
			return "", err
		}
		metricList = append(metricList, metricExpression)
	}
	return strings.Join(metricList, defaultExpressionSeparator), nil
}
