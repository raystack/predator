package table

import (
	"fmt"
	"strings"

	"github.com/odpf/predator/metric/common"
	"github.com/odpf/predator/protocol/job"
	"github.com/odpf/predator/protocol/meta"
	"github.com/odpf/predator/protocol/metric"

	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/query"
)

//Profiler as a model of table profiler
type Profiler struct {
	queryExecutor     protocol.QueryExecutor
	metadataStore     protocol.MetadataStore
	queryResultParser *common.QueryResultParser
}

//New as a constructor of table profiler
func New(queryExecutor protocol.QueryExecutor, metadataStore protocol.MetadataStore) *Profiler {
	return &Profiler{
		queryExecutor:     queryExecutor,
		metadataStore:     metadataStore,
		queryResultParser: &common.QueryResultParser{ParserMap: metricParserMap},
	}
}

//ProfileFullScan to do full scan table profiling
func (t *Profiler) Profile(entry protocol.Entry, profile *job.Profile, metricSpecs []*metric.Spec) ([]*metric.Metric, error) {
	tableSpec, err := t.metadataStore.GetMetadata(profile.URN)
	if err != nil {
		return nil, err
	}

	metricPairs, err := t.prepareMetrics(tableSpec, metricSpecs)
	if err != nil {
		return nil, err
	}
	var metricExpressions []*query.MetricExpression
	for _, pair := range metricPairs {
		metricExpressions = append(metricExpressions, pair.MetricExpression)
	}

	fromExpression := &query.FromClause{
		TableID: profile.URN,
	}

	filterExpression := common.GenerateFilterExpression(profile.Filter, tableSpec)
	groupByExpression := common.GenerateGroupExpression(profile.GroupName)
	selectExpressions := common.GenerateSelectExpression(profile.GroupName)

	q := &query.Query{
		Expressions: selectExpressions,
		Metrics:     metricExpressions,
		From:        fromExpression,
		Where:       filterExpression,
		GroupBy:     groupByExpression,
	}

	queryString := q.String()

	result, err := t.queryExecutor.Run(profile, queryString, job.TableLevelQuery)
	if err != nil {
		return nil, err
	}

	var metrics []*metric.Metric
	for _, row := range result {
		groupMetrics, err := t.queryResultParser.Parse(row, metricPairs)
		if err != nil {
			return nil, err
		}

		metrics = append(metrics, groupMetrics...)
	}

	return metrics, nil
}

func (t *Profiler) prepareMetrics(tableSpec *meta.TableSpec, metricSpecs []*metric.Spec) ([]*common.SpecExpressionPair, error) {
	var pairs []*common.SpecExpressionPair

	for i, metricSpec := range metricSpecs {
		var m *query.MetricExpression
		alias := createAlias(metricSpec.Name.String(), i)
		switch metricSpec.Name {

		case metric.Count:
			m = query.NewMetricExpression("1", alias, query.MetricTypeCount)

		case metric.UniqueCount:
			var err error
			uniqueKey, err := t.createUniqueKey(tableSpec, metricSpec)
			if err != nil {
				return nil, err
			}
			m = query.NewMetricExpression(uniqueKey, alias, query.MetricTypeUniqueCount)

		case metric.InvalidCount:
			m = query.NewMetricExpression(metricSpec.Condition, alias, query.MetricTypeInvalidCount)

		default:
			return nil, fmt.Errorf("unsupported metric type %s", metricSpec.Name)
		}

		pair := &common.SpecExpressionPair{MetricExpression: m, MetricSpec: metricSpec}
		pairs = append(pairs, pair)
	}

	return pairs, nil
}

func createAlias(metricName string, index int) string {
	return fmt.Sprintf("%s_%d", metricName, index)
}

func (t *Profiler) tryFetchFromMetadataStore(tableID string) ([]string, error) {
	return t.metadataStore.GetUniqueConstraints(tableID)
}

func (t *Profiler) createUniqueKey(tableSpec *meta.TableSpec, metricSpec *metric.Spec) (string, error) {
	var uniqueKeys []string
	var err error

	uniqueKeysRaw, ok := metricSpec.Metadata[metric.UniqueFields]
	if ok {
		uniqueKeys, ok = uniqueKeysRaw.([]string)
		if !ok {
			return "", fmt.Errorf("invalid unique fields format")
		}
	} else {
		uniqueKeys, err = t.tryFetchFromMetadataStore(tableSpec.TableID())
		if err != nil {
			return "", err
		}
	}

	var arg string
	if len(uniqueKeys) == 1 {
		arg = uniqueKeys[0]
	} else if len(uniqueKeys) > 1 {
		var fieldExpressions []string
		for _, fieldID := range uniqueKeys {
			castExpression, err := castToString(tableSpec, fieldID)
			if err != nil {
				return "", err
			}
			ifNullExpression := fmt.Sprintf("IFNULL(%s,'null')", castExpression)
			fieldExpressions = append(fieldExpressions, ifNullExpression)
		}
		expressionString := strings.Join(fieldExpressions, ",'|',")
		arg = fmt.Sprintf("CONCAT(%s)", expressionString)
	} else {
		return "", fmt.Errorf("expected list unique constraint column, but no column has been set")
	}

	return arg, nil
}

func castToString(tableSpec *meta.TableSpec, fieldID string) (string, error) {
	fieldSpec, err := tableSpec.GetFieldSpecByID(fieldID)
	if err != nil {
		if err == meta.ErrFieldSpecNotFound {
			err = fmt.Errorf("field ID: %s is not found on table : %s ,%w", fieldID, tableSpec.TableID(), err)
		}
		return "", err
	}
	if fieldSpec.FieldType == meta.FieldTypeBytes {
		return fmt.Sprintf("TO_BASE64(%s)", fieldID), nil
	}
	return fmt.Sprintf("CAST(%s AS STRING)", fieldID), nil
}
