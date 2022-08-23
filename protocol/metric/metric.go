package metric

import (
	"sort"
	"time"
)

//Type is type of metric
type Type string

func (t Type) String() string {
	return string(t)
}

const (
	//NullnessPct is nullnes percentage
	NullnessPct Type = "nullness_pct"
	//DuplicationPct is duplication percentage
	DuplicationPct Type = "duplication_pct"
	//TrendInconsistencyPct is trend inconsistency percentage
	TrendInconsistencyPct Type = "trend_inconsistency_pct"
	//RowCount is row count and is not a percentage
	RowCount Type = "row_count"
	//InvalidPct is invalid percentage
	InvalidPct Type = "invalid_pct"
)

const (
	//NullCount is null count metric
	NullCount Type = "nullcount"
	//Count is count metric
	Count Type = "count"

	//UniqueCount is unique count metric
	UniqueCount Type = "uniquecount"
	//Sum is sum metric
	Sum Type = "sum"
	//InvalidCount is invalid count metric
	InvalidCount Type = "invalidcount"
)

var (
	//TypesBasicMetric metric in basic metric category
	TypesBasicMetric = []Type{NullCount, Count, UniqueCount, Sum, InvalidCount}

	typeCategoryMap = map[Type]Category{
		NullCount:             Basic,
		UniqueCount:           Basic,
		Count:                 Basic,
		InvalidCount:          Basic,
		Sum:                   Quality,
		NullnessPct:           Quality,
		DuplicationPct:        Quality,
		TrendInconsistencyPct: Quality,
		RowCount:              Quality,
		InvalidPct:            Quality,
	}
	//TypesDataQuality is metric types in data quality category
	TypesDataQuality = []Type{NullnessPct, DuplicationPct, TrendInconsistencyPct, RowCount, InvalidPct}

	//TypeAll is all of metric types
	TypeAll = append(TypesDataQuality, TypesBasicMetric...)
)

//GetCategory to get Category of a metric Type
func GetCategory(metricType Type) Category {
	return typeCategoryMap[metricType]
}

//Category is business perspective of metric
type Category string

const (
	//Basic is category for metric belong to basic metric
	Basic Category = "basic"

	//Quality is category for metric belong to data quality metric
	Quality = "quality"
)

//Owner type the metric
type Owner string

const (
	//Table table as owner of metric
	Table Owner = "table"
	//Field field or column as owner of metric
	Field Owner = "field"
)

//Metric information about statistical measurement of a table resource
type Metric struct {
	ID       string
	FieldID  string
	Type     Type
	Category Category
	Owner    Owner

	//Partition will be deprecated, please do not use this
	Partition string

	Metadata map[string]interface{}

	GroupValue string
	Value      float64
	Condition  string
	Timestamp  time.Time
}

const (
	//UniqueFields is metadata needed to form unique count metric
	UniqueFields = "uniquefields"
)

//New create Metric
func New(fieldID string,
	_type Type,
	category Category,
	value float64,
	timestamp time.Time) *Metric {

	owner := Field
	if fieldID == "" {
		owner = Table
	}

	return &Metric{
		FieldID:   fieldID,
		Type:      _type,
		Category:  category,
		Owner:     owner,
		Value:     value,
		Timestamp: timestamp,
	}
}

type matcher interface {
	match(metric *Metric) bool
}

type idMatcher struct {
	ID string
}

func (i idMatcher) match(metric *Metric) bool {
	return metric.ID == i.ID
}

type fieldIDMatcher struct {
	FieldID string
}

func (f fieldIDMatcher) match(metric *Metric) bool {
	return f.FieldID == metric.FieldID
}

type typeMatcher struct {
	Type Type
}

func (t typeMatcher) match(metric *Metric) bool {
	return t.Type == metric.Type
}

type categoryMatcher struct {
	Category Category
}

func (c categoryMatcher) match(metric *Metric) bool {
	return c.Category == metric.Category
}

type ownerMatcher struct {
	Owner Owner
}

func (o ownerMatcher) match(metric *Metric) bool {
	return o.Owner == metric.Owner
}

type partitionMatcher struct {
	Partition string
}

func (p partitionMatcher) match(metric *Metric) bool {
	return p.Partition == metric.Partition
}

//Finder to find metric with specific criteria
type Finder struct {
	metrics  []*Metric
	matchers []matcher
}

func (f *Finder) WithID(ID string) *Finder {
	i := idMatcher{
		ID: ID,
	}
	f.matchers = append(f.matchers, i)
	return f
}

func (f *Finder) WithFieldID(fieldID string) *Finder {
	i := fieldIDMatcher{
		FieldID: fieldID,
	}
	f.matchers = append(f.matchers, i)
	return f
}

func (f *Finder) WithType(Type Type) *Finder {
	t := typeMatcher{
		Type: Type,
	}
	f.matchers = append(f.matchers, t)
	return f
}

func (f *Finder) WithOwner(owner Owner) *Finder {
	t := ownerMatcher{
		Owner: owner,
	}
	f.matchers = append(f.matchers, t)
	return f
}

func (f *Finder) WithCategory(cat Category) *Finder {
	t := categoryMatcher{
		Category: cat,
	}
	f.matchers = append(f.matchers, t)
	return f
}

func (f *Finder) WithPartition(partition string) *Finder {
	p := partitionMatcher{
		Partition: partition,
	}
	f.matchers = append(f.matchers, p)
	return f
}

type conditionMatcher struct {
	Condition string
}

func (c conditionMatcher) match(metric *Metric) bool {
	return metric.Condition == c.Condition
}

func (f *Finder) WithCondition(condition string) *Finder {
	m := &conditionMatcher{
		Condition: condition,
	}
	f.matchers = append(f.matchers, m)
	return f
}

//NewFinder create Finder
//need list of metrics
func NewFinder(metrics []*Metric) *Finder {
	return &Finder{
		metrics: metrics,
	}
}

func (f *Finder) check(metric *Metric) bool {
	for _, m := range f.matchers {
		if !m.match(metric) {
			return false
		}
	}
	return true
}

//Find find all metrics matches criteria
//return empty slices when no metric found
func (f *Finder) Find() []*Metric {
	var matches []*Metric
	for _, target := range f.metrics {
		if f.check(target) {
			matches = append(matches, target)
		}
	}
	return matches
}

//FindOne find one metric matches criteria
//Return first found metric
//Might return nil when not found
func (f *Finder) FindOne() *Metric {
	for _, target := range f.metrics {
		if f.check(target) {
			return target
		}
	}
	return nil
}

type Group []*Metric

func (g Group) ByPartition() []Group {
	partitionMetricsMap := make(map[string][]*Metric)

	for _, metric := range g {
		partitionMetricsMap[metric.Partition] = append(partitionMetricsMap[metric.Partition], metric)
	}

	var partitions []string
	for k := range partitionMetricsMap {
		partitions = append(partitions, k)
	}

	sort.Strings(partitions)

	var groups []Group
	for _, p := range partitions {
		m := partitionMetricsMap[p]
		groups = append(groups, m)
	}

	return groups
}

func (g Group) ByPartitionAsMap() map[string][]*Metric {
	partitionMetricsMap := make(map[string][]*Metric)

	for _, metric := range g {
		partitionMetricsMap[metric.Partition] = append(partitionMetricsMap[metric.Partition], metric)
	}
	return partitionMetricsMap
}

func (g Group) ByGroupValue() []Group {
	groupValueMetricsMap := make(map[string][]*Metric)

	for _, metric := range g {
		groupValueMetricsMap[metric.GroupValue] = append(groupValueMetricsMap[metric.GroupValue], metric)
	}

	var groupValues []string
	for k := range groupValueMetricsMap {
		groupValues = append(groupValues, k)
	}

	sort.Strings(groupValues)

	var groups []Group
	for _, p := range groupValues {
		m := groupValueMetricsMap[p]
		groups = append(groups, m)
	}

	return groups
}
