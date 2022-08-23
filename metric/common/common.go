package common

import (
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"encoding/base64"
	"fmt"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/metric"
	"github.com/odpf/predator/protocol/query"
	"math/big"
	"reflect"
	"strconv"
	"time"
)

var typeOfByteSlice = reflect.TypeOf([]byte{})
var typeOfTimestamp = reflect.TypeOf(time.Time{})
var typeOfDate = reflect.TypeOf(civil.Date{})
var typeOfDatetime = reflect.TypeOf(civil.DateTime{})
var typeOfTime = reflect.TypeOf(civil.Time{})
var typeOfNumeric = reflect.TypeOf(&big.Rat{})

//ConvertValueToString convert bigquery compatible types to string
func ConvertValueToString(val interface{}) (string, error) {

	v := reflect.ValueOf(val)
	switch v.Type() {
	case typeOfByteSlice:
		return base64.StdEncoding.EncodeToString(v.Bytes()), nil
	case typeOfTime:
		return bigquery.CivilTimeString(val.(civil.Time)), nil
	case typeOfDate:
		return val.(civil.Date).String(), nil
	case typeOfDatetime:
		return bigquery.CivilDateTimeString(val.(civil.DateTime)), nil
	case typeOfTimestamp:
		t := val.(time.Time)
		return strconv.FormatInt(int64(time.Nanosecond)*t.UnixNano()/int64(time.Millisecond), 10), nil
	case typeOfNumeric:
		r := val.(*big.Rat)
		return bigquery.NumericString(r), nil
	}

	switch v.Kind() {
	case reflect.String:
		return v.String(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(v.Int(), 10), nil
	case reflect.Float32, reflect.Float64:
		return fmt.Sprintf("%.9f", v.Float()), nil
	case reflect.Bool:
		return strconv.FormatBool(v.Bool()), nil
	default:
		return "", fmt.Errorf("unhandled kind %s", v.Kind())
	}
}

const GroupAlias = "__group_value"

type SpecExpressionPair struct {
	MetricSpec       *metric.Spec
	MetricExpression *query.MetricExpression
}

type RowParserType = func(map[string]interface{}, string, *metric.Spec) (*metric.Metric, error)

type QueryResultParser struct {
	ParserMap map[metric.Type]RowParserType
}

func (q *QueryResultParser) Parse(row protocol.Row, metricPairs []*SpecExpressionPair) ([]*metric.Metric, error) {
	var metrics []*metric.Metric

	for _, pair := range metricPairs {
		metricSpec := pair.MetricSpec
		metricExpression := pair.MetricExpression

		parser, ok := q.ParserMap[metricSpec.Name]
		if !ok {
			return nil, fmt.Errorf("unsupported metric type: %s", metricSpec.Name.String())
		}
		fieldMetric, err := parser(row, metricExpression.Alias, metricSpec)
		if err != nil {
			return nil, err
		}
		metrics = append(metrics, fieldMetric)
	}

	groupValue, useGroup := row[GroupAlias]
	if useGroup {
		for _, m := range metrics {
			gv, err := ConvertValueToString(groupValue)
			if err != nil {
				return nil, fmt.Errorf(" group value is invalid %w", err)
			}
			m.GroupValue = gv
		}
	}

	return metrics, nil
}
