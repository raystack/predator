package metric

import (
	"fmt"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/meta"
	"github.com/odpf/predator/protocol/metric"
	"github.com/odpf/predator/protocol/xlog"
)

type BasicMetricSpecGenerator struct {
	toleranceStore protocol.ToleranceStore
	metadataStore  protocol.MetadataStore
}

func NewBasicMetricSpecGenerator(toleranceStore protocol.ToleranceStore, metadataStore protocol.MetadataStore) protocol.MetricSpecGenerator {
	return &BasicMetricSpecGenerator{toleranceStore: toleranceStore, metadataStore: metadataStore}
}

func (b *BasicMetricSpecGenerator) GenerateMetricSpec(urn string) ([]*metric.Spec, error) {
	toleranceSpec, err := b.toleranceStore.GetByTableID(urn)
	if err != nil {
		e := fmt.Errorf("failed to try to get toleranceSpec for table %s ,%w", urn, err)
		logger.Println(e)
		return nil, e
	}

	tableSpec, err := b.metadataStore.GetMetadata(urn)
	if err != nil {
		e := fmt.Errorf("failed to try to get metadata for table %s ,%w", urn, err)
		logger.Println(e)
		return nil, e
	}

	return b.Generate(tableSpec, toleranceSpec.Tolerances)
}

func (b *BasicMetricSpecGenerator) Generate(tableSpec *meta.TableSpec, tolerances []*protocol.Tolerance) ([]*metric.Spec, error) {
	var specs []*metric.Spec

	tableSpecs, err := b.generateTableMetricSpecs(tolerances)
	if err != nil {
		return nil, err
	}
	specs = append(specs, tableSpecs...)

	fieldSpecs := b.generateFieldMetricSpecs(tableSpec, tolerances)
	specs = append(specs, fieldSpecs...)

	var upstreamMetricSpecs []*metric.Spec
	upstreamMetricSpecs = b.generateUpstreamMetrics(tableSpec, specs, upstreamMetricSpecs)

	return append(specs, upstreamMetricSpecs...), nil
}

//generateUpstreamMetrics generate any metric that required for the computation of a table metric or field metric
func (b *BasicMetricSpecGenerator) generateUpstreamMetrics(tableSpec *meta.TableSpec, specs []*metric.Spec, upstreamMetricSpecs []*metric.Spec) []*metric.Spec {
	if len(specs) > 0 {
		countMetricSpec := &metric.Spec{
			Name:    metric.Count,
			TableID: tableSpec.TableID(),
			Owner:   metric.Table,
		}
		upstreamMetricSpecs = append(upstreamMetricSpecs, countMetricSpec)
	}
	return upstreamMetricSpecs
}

func (b *BasicMetricSpecGenerator) generateTableMetricSpecs(tolerances []*protocol.Tolerance) ([]*metric.Spec, error) {
	var specs []*metric.Spec

	for _, tolerance := range tolerances {
		if tolerance.FieldID != "" {
			continue
		}
		if tolerance.MetricName == metric.DuplicationPct {
			uniqueCountMetric, err := generateUniqueCountMetric(tolerance)
			if err != nil {
				return nil, err
			}
			specs = append(specs, uniqueCountMetric)
		}
		if tolerance.MetricName == metric.InvalidPct {
			specs = append(specs, generateInvalidCountMetric(tolerance))
		}
	}

	return specs, nil
}

func (b *BasicMetricSpecGenerator) generateFieldMetricSpecs(tableSpec *meta.TableSpec, tolerances []*protocol.Tolerance) []*metric.Spec {
	var specs []*metric.Spec
	countMetricExistMap := make(map[string]bool)

	for _, tolerance := range tolerances {
		if tolerance.FieldID == "" {
			continue
		}
		if !countMetricExistMap[tolerance.FieldID] {
			specs = append(specs, generateCountMetric(tolerance))
			countMetricExistMap[tolerance.FieldID] = true
		}
		if tolerance.MetricName == metric.InvalidPct {
			specs = append(specs, generateInvalidCountMetric(tolerance))
		}
		if tolerance.MetricName == metric.NullnessPct {
			specs = append(specs, generateNullCountMetric(tolerance))
		}
		if tolerance.MetricName == metric.Sum {
			var sumSpecs = generateSumMetric(tableSpec, tolerance)
			if sumSpecs != nil {
				specs = append(specs, sumSpecs)
			}
		}
	}

	return specs
}

func generateCountMetric(tolerance *protocol.Tolerance) *metric.Spec {
	return &metric.Spec{
		Name:    metric.Count,
		TableID: tolerance.TableURN,
		FieldID: tolerance.FieldID,
		Owner:   getOwner(tolerance),
	}
}

func generateSumMetric(tableSpec *meta.TableSpec, tolerance *protocol.Tolerance) *metric.Spec {
	var fieldsSpec []*meta.FieldSpec = tableSpec.Fields
	var isValidField = false
	for i := range fieldsSpec {
		var field = fieldsSpec[i]
		if field.Name == tolerance.FieldID && field.FieldType.IsNumeric() {
			isValidField = true
			break
		}
	}
	println(isValidField)
	if !isValidField {
		xlog.Info("Unable to calculate sum metric for non numeric field " + tolerance.FieldID)
		return nil
	}
	return &metric.Spec{
		Name:    metric.Sum,
		TableID: tolerance.TableURN,
		FieldID: tolerance.FieldID,
		Owner:   getOwner(tolerance),
	}
}

func generateNullCountMetric(tolerance *protocol.Tolerance) *metric.Spec {
	return &metric.Spec{
		Name:    metric.NullCount,
		TableID: tolerance.TableURN,
		FieldID: tolerance.FieldID,
		Owner:   metric.Field,
	}
}

func generateInvalidCountMetric(tolerance *protocol.Tolerance) *metric.Spec {
	return &metric.Spec{
		Name:      metric.InvalidCount,
		TableID:   tolerance.TableURN,
		FieldID:   tolerance.FieldID,
		Condition: tolerance.Condition,
		Owner:     getOwner(tolerance),
	}
}

func generateUniqueCountMetric(tolerance *protocol.Tolerance) (*metric.Spec, error) {
	uniqueCountMetric := &metric.Spec{
		Name:    metric.UniqueCount,
		TableID: tolerance.TableURN,
		Owner:   metric.Table,
	}

	// currently Predator supports fetching unique constraints from spec and unique constraint store
	// when unique constraint store is deprecated, a validation to check if unique fields are specified in spec is needed
	uniqueFieldsRaw, ok := tolerance.Metadata[metric.UniqueFields]
	if ok {
		uniqueFields, ok := uniqueFieldsRaw.([]string)
		if !ok {
			return nil, fmt.Errorf("invalid unique fields type in %s tolerance spec", tolerance.TableURN)
		}
		uniqueCountMetric.Metadata = map[string]interface{}{
			metric.UniqueFields: uniqueFields,
		}
	}

	return uniqueCountMetric, nil
}

type QualityMetricSpecGenerator struct {
	metadataStore  protocol.MetadataStore
	toleranceStore protocol.ToleranceStore
}

func NewQualityMetricSpecGenerator(metadataStore protocol.MetadataStore, toleranceStore protocol.ToleranceStore) *QualityMetricSpecGenerator {
	return &QualityMetricSpecGenerator{metadataStore: metadataStore, toleranceStore: toleranceStore}
}

func (q *QualityMetricSpecGenerator) Generate(tableSpec *meta.TableSpec, tolerances []*protocol.Tolerance) ([]*metric.Spec, error) {
	metricSpecs := generateMetricSpec(tolerances)
	return metricSpecs, nil
}

func (q *QualityMetricSpecGenerator) GenerateMetricSpec(urn string) ([]*metric.Spec, error) {
	toleranceSpec, err := q.toleranceStore.GetByTableID(urn)
	if err != nil {
		e := fmt.Errorf("failed to try to get toleranceSpec for table %s ,%w", urn, err)
		logger.Println(e)
		return nil, e
	}

	tableSpec, err := q.metadataStore.GetMetadata(urn)
	if err != nil {
		e := fmt.Errorf("failed to try to get metadata for table %s ,%w", urn, err)
		logger.Println(e)
		return nil, e
	}

	return q.Generate(tableSpec, toleranceSpec.Tolerances)
}

func generateMetricSpec(tolerances []*protocol.Tolerance) []*metric.Spec {
	var specs []*metric.Spec

	tableSpecs := generateTableMetricSpecs(tolerances)
	specs = append(specs, tableSpecs...)

	fieldSpecs := generateFieldMetricSpecs(tolerances)
	specs = append(specs, fieldSpecs...)

	return specs
}

func generateTableMetricSpecs(tolerances []*protocol.Tolerance) []*metric.Spec {
	var specs []*metric.Spec

	for _, tolerance := range tolerances {
		if tolerance.FieldID != "" {
			continue
		}
		if tolerance.MetricName == metric.DuplicationPct {
			specs = append(specs, generateDuplicationPctMetric(tolerance))
		}
		if tolerance.MetricName == metric.RowCount {
			specs = append(specs, generateRowCountMetric(tolerance))
		}
		if tolerance.MetricName == metric.InvalidPct {
			specs = append(specs, generateInvalidityPctMetric(tolerance))
		}
	}

	return specs
}

func generateFieldMetricSpecs(tolerances []*protocol.Tolerance) []*metric.Spec {
	var specs []*metric.Spec

	for _, tolerance := range tolerances {
		if tolerance.FieldID == "" {
			continue
		}
		if tolerance.MetricName == metric.NullnessPct {
			specs = append(specs, generateNullnessPctMetric(tolerance))
		}
		if tolerance.MetricName == metric.TrendInconsistencyPct {
			specs = append(specs, generateTrendInconsistencyMetric(tolerance))
		}
		if tolerance.MetricName == metric.InvalidPct {
			specs = append(specs, generateInvalidityPctMetric(tolerance))
		}
	}
	return specs
}

func generateDuplicationPctMetric(tolerance *protocol.Tolerance) *metric.Spec {
	return &metric.Spec{
		Name:    metric.DuplicationPct,
		TableID: tolerance.TableURN,
		Owner:   metric.Table,
	}
}

func generateRowCountMetric(tolerance *protocol.Tolerance) *metric.Spec {
	return &metric.Spec{
		Name:    metric.RowCount,
		TableID: tolerance.TableURN,
		Owner:   metric.Table,
	}
}

func generateNullnessPctMetric(tolerance *protocol.Tolerance) *metric.Spec {
	return &metric.Spec{
		Name:    metric.NullnessPct,
		TableID: tolerance.TableURN,
		FieldID: tolerance.FieldID,
		Owner:   metric.Field,
	}
}

func generateTrendInconsistencyMetric(tolerance *protocol.Tolerance) *metric.Spec {
	return &metric.Spec{
		Name:     metric.TrendInconsistencyPct,
		TableID:  tolerance.TableURN,
		FieldID:  tolerance.FieldID,
		Optional: true,
		Owner:    metric.Field,
	}
}

func generateInvalidityPctMetric(tolerance *protocol.Tolerance) *metric.Spec {
	return &metric.Spec{
		Name:      metric.InvalidPct,
		TableID:   tolerance.TableURN,
		FieldID:   tolerance.FieldID,
		Condition: tolerance.Condition,
		Owner:     getOwner(tolerance),
	}
}

func getOwner(tolerance *protocol.Tolerance) metric.Owner {
	if tolerance.FieldID == "" {
		return metric.Table
	}
	return metric.Field
}
