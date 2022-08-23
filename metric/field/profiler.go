package field

import (
	"fmt"
	"sort"
	"strings"

	"github.com/odpf/predator/metric/common"
	"github.com/odpf/predator/protocol/job"

	"github.com/odpf/predator/protocol/meta"
	"github.com/odpf/predator/protocol/metric"

	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/query"
)

//Profiler as a struct for field profiler
type Profiler struct {
	queryExecutor     protocol.QueryExecutor
	metadataStore     protocol.MetadataStore
	queryResultParser *common.QueryResultParser
}

//New to construct field profiler
func New(queryExecutor protocol.QueryExecutor, metadataStore protocol.MetadataStore) *Profiler {
	return &Profiler{
		queryExecutor:     queryExecutor,
		metadataStore:     metadataStore,
		queryResultParser: &common.QueryResultParser{ParserMap: metricParserMap},
	}
}

//Profile as an implementation to profile
func (f *Profiler) Profile(entry protocol.Entry, profile *job.Profile, metricSpecs []*metric.Spec) ([]*metric.Metric, error) {
	tableSpec, err := f.metadataStore.GetMetadata(profile.URN)
	if err != nil {
		return nil, err
	}

	metricSpecsGroup, err := groupMetricSpecsByBranch(tableSpec, metricSpecs)
	if err != nil {
		return nil, err
	}

	var branches []*meta.FieldSpec
	for fieldSpec := range metricSpecsGroup {
		branches = append(branches, fieldSpec)
	}

	sort.Sort(meta.ByFieldName(branches))

	var metrics []*metric.Metric
	for _, branch := range branches {
		ms := metricSpecsGroup[branch]

		results, err := f.profileFieldGroup(branch, profile, tableSpec, ms)
		if err != nil {
			return nil, err
		}

		metrics = append(metrics, results...)
	}
	return metrics, nil
}

//groupMetricSpecsByBranch ByClosestRepeatedAncestor
func groupMetricSpecsByBranch(tableSpec *meta.TableSpec, metricSpecs []*metric.Spec) (map[*meta.FieldSpec][]*metric.Spec, error) {
	metricSpecsGroup := make(map[*meta.FieldSpec][]*metric.Spec)

	for _, metricSpec := range metricSpecs {
		fieldSpec, err := tableSpec.GetFieldSpecByID(metricSpec.FieldID)
		if err != nil {
			if err == meta.ErrFieldSpecNotFound {
				err = fmt.Errorf("field ID: %s is not found on table : %s ,%w", metricSpec.FieldID, tableSpec.TableID(), err)
			}
			return nil, err
		}
		pathToRoot := fieldSpec.FromRootPath()
		branch := getNearestBranch(pathToRoot)
		metricSpecsGroup[branch] = append(metricSpecsGroup[branch], metricSpec)
	}

	return metricSpecsGroup, nil
}

func (f *Profiler) profileFieldGroup(branch *meta.FieldSpec, profile *job.Profile, tableSpec *meta.TableSpec, metricSpecs []*metric.Spec) ([]*metric.Metric, error) {
	metricExpressionsPairs, err := prepareMetricsForQuery(tableSpec, metricSpecs)
	if err != nil {
		return nil, err
	}

	var metricExpressions []*query.MetricExpression
	for _, pair := range metricExpressionsPairs {
		metricExpressions = append(metricExpressions, pair.MetricExpression)
	}

	fromExpression := generateFromExpression(branch, tableSpec)
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

	sql := q.String()

	result, err := f.queryExecutor.Run(profile, sql, job.FieldLevelQuery)
	if err != nil {
		return nil, err
	}

	var metrics []*metric.Metric
	for _, r := range result {
		groupMetrics, err := f.queryResultParser.Parse(r, metricExpressionsPairs)
		if err != nil {
			return nil, err
		}

		metrics = append(metrics, groupMetrics...)
	}

	return metrics, nil
}

func generateFromExpression(branch *meta.FieldSpec, spec *meta.TableSpec) *query.FromClause {
	var branchToRoot []*meta.FieldSpec
	if branch != nil {
		branchToRoot = append(branch.FromRootPath(), branch)
	}

	var unnests []*query.Unnest
	unnests = generateUnnest(branchToRoot)

	fromClause := &query.FromClause{
		TableID:       spec.TableID(),
		UnnestClauses: unnests,
	}
	return fromClause
}

func prepareMetricsForQuery(tableSpec *meta.TableSpec, metricSpecs []*metric.Spec) ([]*common.SpecExpressionPair, error) {
	var pairs []*common.SpecExpressionPair

	for i, metricSpec := range metricSpecs {
		fieldSpec, err := tableSpec.GetFieldSpecByID(metricSpec.FieldID)
		if err != nil {
			if err == meta.ErrFieldSpecNotFound {
				err = fmt.Errorf("field ID: %s is not found on table : %s ,%w", metricSpec.FieldID, tableSpec.TableID(), err)
			}
			return nil, err
		}

		alias := getAlias(fieldSpec.Name, metricSpec.Name, i)

		var arg string
		if metricSpec.Name != metric.InvalidCount {
			arg = getUnnestedColumnName(fieldSpec)
		} else {
			arg = metricSpec.Condition
		}
		metricTemplateType := query.ParseMetricType(metricSpec.Name, fieldSpec.Mode)

		m := query.NewMetricExpression(arg, alias, metricTemplateType)

		pair := &common.SpecExpressionPair{
			MetricSpec:       metricSpec,
			MetricExpression: m,
		}
		pairs = append(pairs, pair)
	}

	return pairs, nil
}

func getAlias(fieldName string, metricType metric.Type, index int) string {
	return fmt.Sprintf("%s_%s_%d", metricType.String(), fieldName, index)
}

func getUnnestedColumnName(fieldSpec *meta.FieldSpec) string {
	pathToRoot := fieldSpec.FromRootPath()
	if fieldSpec.Mode == meta.ModeRepeated {
		if len(pathToRoot) > 0 {
			return getColumnNameByLineage(fieldSpec, pathToRoot)
		}
		return fmt.Sprintf("`%s`", fieldSpec.Name)
	}
	return getColumnNameByLineage(fieldSpec, pathToRoot)
}

//given fieldSpec [e], with parents [a,b,c,d], with [c] is repeated column
//will return level2.`d`.`e`
func getColumnNameByLineage(fieldSpec *meta.FieldSpec, fullParents []*meta.FieldSpec) string {
	//[c,d]
	leafToBranch := pathToNearestBranch(fullParents)

	//[`d`,level1]
	var namespaces []string
	for _, fs := range leafToBranch {
		var namespace string
		if fs.Mode == meta.ModeRepeated {
			namespace = fmt.Sprintf("level%d", fs.Level)
		} else {
			namespace = fmt.Sprintf("`%s`", fs.Name)
		}

		namespaces = append(namespaces, namespace)
	}

	//[level1,`d`,`e`]
	namespaces = append(namespaces, fmt.Sprintf("`%s`", fieldSpec.Name))

	//level1.`d`.`e`
	return strings.Join(namespaces, ".")
}

//pathToNearestBranch return array of *meta.FieldSpec until closest repeated field sorted from the root ancestry
//given field e with ancestors [a,b,c,d]
//with [a] as root
//and [c] as repeated column
//will returns c
func getNearestBranch(fullParents []*meta.FieldSpec) *meta.FieldSpec {
	//[c,d]
	toNearestBranch := pathToNearestBranch(fullParents)

	if len(toNearestBranch) > 0 {
		nearestBranch := toNearestBranch[0]
		return nearestBranch
	}

	return nil
}

//pathToNearestBranch return array of *meta.FieldSpec until closest repeated field sorted from the root ancestry
//given parents [a,b,c,d]
//with [a] as root
//and [c] as repeated column
//will returns [c,d]
func pathToNearestBranch(fullParents []*meta.FieldSpec) []*meta.FieldSpec {
	var parents []*meta.FieldSpec
	for i := len(fullParents) - 1; i >= 0; i-- {
		parents = append(parents, fullParents[i])
		if fullParents[i].Mode == meta.ModeRepeated {
			break
		}
	}

	var result []*meta.FieldSpec
	for i := len(parents) - 1; i >= 0; i-- {
		result = append(result, parents[i])
	}

	return result
}

//generateUnnest generate of unnest for lists of lineage
func generateUnnest(lineage []*meta.FieldSpec) []*query.Unnest {
	if len(lineage) == 0 {
		return nil
	}

	var unnestList []*query.Unnest

	for i, fieldSpec := range lineage {
		currentLineage := lineage[:i]
		if fieldSpec.Mode == meta.ModeRepeated {
			unnest := createUnnest(fieldSpec, currentLineage)
			unnestList = append(unnestList, unnest)
		}
	}

	return unnestList
}

//createUnnest to create unnest element to reach a nested field
func createUnnest(fieldSpec *meta.FieldSpec, ancestors []*meta.FieldSpec) *query.Unnest {

	columnName := getColumnNameByLineage(fieldSpec, ancestors)
	alias := getUnnestAlias(fieldSpec)

	return &query.Unnest{
		ColumnName: columnName,
		Alias:      alias,
	}
}

//level of unnest start from 1
func getUnnestAlias(fieldSpec *meta.FieldSpec) string {
	return fmt.Sprintf("level%d", fieldSpec.Level)
}
