package tolerance

import (
	"errors"
	"fmt"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/meta"
	"github.com/odpf/predator/protocol/metric"
	"gopkg.in/yaml.v2"
	"sort"
	"time"
)

const (
	flatSpecType specType = iota
	compactSpecType
)

type specType int

//FlatSpec is predator tolerance spec presented as list  json/yaml structure
type FlatSpec []*Tolerance

//Tolerance is format of tolerance configuration
type Tolerance struct {
	ID             string
	TableID        string
	FieldID        string
	MetricName     metric.Type
	ToleranceRules RulesMap
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func (t *Tolerance) ToTolerance() *protocol.Tolerance {
	toleranceRules := t.ToleranceRules.ToArray()

	return &protocol.Tolerance{
		ID:             t.ID,
		TableURN:       t.TableID,
		FieldID:        t.FieldID,
		MetricName:     t.MetricName,
		ToleranceRules: toleranceRules,
		CreatedAt:      t.CreatedAt,
		UpdatedAt:      t.UpdatedAt,
	}
}

//CompactSpec compact tolerance spec is structured
// json/yaml schema with less redundant fields
type CompactSpec struct {
	TableID      string
	TableMetrics []*MetricSpec
	Fields       []*Field
}

type MetricSpec struct {
	MetricName metric.Type
	Condition  string
	Metadata   map[string]interface{}
	Tolerance  RulesMap
}

type Metadata struct {
	//UniqueFields are metadata that required to profile unique count
	UniqueFields []string
}

type Field struct {
	FieldID      string
	FieldMetrics []*MetricSpec
}

//RulesMap tolerance rules as map
type RulesMap map[string]float64

func NewRulesMap(rules []protocol.ToleranceRule) RulesMap {
	rulesMap := make(map[string]float64)
	for _, rule := range rules {
		rulesMap[rule.Comparator.String()] = rule.Value
	}
	return rulesMap
}

func (r RulesMap) ToArray() []protocol.ToleranceRule {
	var keys []string
	for k := range r {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	var toleranceRules []protocol.ToleranceRule

	for _, comparator := range keys {
		value := r[comparator]
		toleranceRule := protocol.ToleranceRule{
			Comparator: protocol.Comparator(comparator),
			Value:      value,
		}
		toleranceRules = append(toleranceRules, toleranceRule)
	}
	return toleranceRules
}

func createParser(sType specType) Parser {
	if sType == flatSpecType {
		return &FlatSpecParser{}
	}
	return &CompactSpecParser{}
}

//Parser is parser and serializer of tolerance spec
type Parser interface {
	Parse(content []byte) (*protocol.ToleranceSpec, error)
	Serialise(*protocol.ToleranceSpec) ([]byte, error)
}

//FlatSpecParser is parser for FlatSpec format
type FlatSpecParser struct {
}

func (s *FlatSpecParser) Serialise(tolerances *protocol.ToleranceSpec) (content []byte, err error) {
	return nil, errors.New("unsupported serialization")
}

func (s *FlatSpecParser) Parse(content []byte) (*protocol.ToleranceSpec, error) {
	var gcsTolerances FlatSpec
	err := yaml.UnmarshalStrict(content, &gcsTolerances)
	if err != nil {
		return nil, err
	}

	var tolerances []*protocol.Tolerance

	for _, tolerance := range gcsTolerances {
		t := tolerance.ToTolerance()
		tolerances = append(tolerances, t)
	}

	if len(tolerances) == 0 {
		return nil, errors.New("unable to serialise spec with flat format" +
			", no tolerance found, need to specifiy at least one metric")
	}

	sample := tolerances[0]
	return &protocol.ToleranceSpec{
		URN:        sample.TableURN,
		Tolerances: tolerances,
	}, nil
}

//CompactSpecParser is parser for CompactSpec
type CompactSpecParser struct {
}

func (s *CompactSpecParser) Serialise(toleranceSpec *protocol.ToleranceSpec) (content []byte, err error) {

	spec := &CompactSpec{
		TableID:      toleranceSpec.URN,
		TableMetrics: nil,
		Fields:       nil,
	}

	var tableMetrics []*MetricSpec
	for _, tol := range toleranceSpec.Tolerances {
		own := metric.Table
		if tol.FieldID != "" {
			own = metric.Field
		}

		if own == metric.Table {
			metadata := make(map[string]interface{})
			if uniqueKeysRaw, ok := tol.Metadata[metric.UniqueFields]; ok {
				if uniqueKeys, ok := uniqueKeysRaw.([]string); ok {
					metadata[metric.UniqueFields] = uniqueKeys
				}
			}
			ms := &MetricSpec{
				MetricName: tol.MetricName,
				Condition:  tol.Condition,
				Metadata:   metadata,
				Tolerance:  NewRulesMap(tol.ToleranceRules),
			}
			tableMetrics = append(tableMetrics, ms)
		}
	}

	fieldsMap := make(map[string][]*MetricSpec)
	for _, tol := range toleranceSpec.Tolerances {
		own := metric.Table
		if tol.FieldID != "" {
			own = metric.Field
		}

		if own == metric.Field {
			ms := &MetricSpec{
				MetricName: tol.MetricName,
				Condition:  tol.Condition,
				Tolerance:  NewRulesMap(tol.ToleranceRules),
			}

			fieldsMap[tol.FieldID] = append(fieldsMap[tol.FieldID], ms)
		}
	}

	var keys []string
	for key := range fieldsMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var fields []*Field
	for _, fieldID := range keys {
		specs := fieldsMap[fieldID]
		f := &Field{
			FieldID:      fieldID,
			FieldMetrics: specs,
		}
		fields = append(fields, f)
	}

	spec.TableMetrics = tableMetrics
	spec.Fields = fields
	return yaml.Marshal(spec)
}

func (s *CompactSpecParser) Parse(content []byte) (*protocol.ToleranceSpec, error) {
	var storedSpec *CompactSpec
	err := yaml.UnmarshalStrict(content, &storedSpec)
	if err != nil {
		return nil, err
	}

	var tolerances []*protocol.Tolerance
	tableLevelTolerances := prepareTableLevelTolerances(storedSpec.TableID, storedSpec.TableMetrics)
	fieldLevelTolerances := prepareFieldLevelTolerances(storedSpec.TableID, storedSpec.Fields)
	tolerances = append(tolerances, tableLevelTolerances...)
	tolerances = append(tolerances, fieldLevelTolerances...)

	return &protocol.ToleranceSpec{
		URN:        storedSpec.TableID,
		Tolerances: tolerances,
	}, nil
}

func prepareTableLevelTolerances(tableURN string, gcsTableMetrics []*MetricSpec) []*protocol.Tolerance {
	var tolerances []*protocol.Tolerance
	for _, tableMetric := range gcsTableMetrics {
		var metadata map[string]interface{}
		if tableMetric.MetricName == metric.DuplicationPct {
			var uniqueKeys []string
			if uniqueKeysRaw, ok := tableMetric.Metadata[metric.UniqueFields]; ok {
				for _, key := range uniqueKeysRaw.([]interface{}) {
					uniqueKeys = append(uniqueKeys, key.(string))
				}
				metadata = map[string]interface{}{
					metric.UniqueFields: uniqueKeys,
				}
			}
		}

		toleranceRules := tableMetric.Tolerance.ToArray()
		tolerance := &protocol.Tolerance{
			TableURN:       tableURN,
			MetricName:     tableMetric.MetricName,
			Condition:      tableMetric.Condition,
			Metadata:       metadata,
			ToleranceRules: toleranceRules,
		}
		tolerances = append(tolerances, tolerance)
	}
	return tolerances
}

func prepareFieldLevelTolerances(tableURN string, fields []*Field) []*protocol.Tolerance {
	var tolerances []*protocol.Tolerance
	for _, field := range fields {
		for _, fieldMetric := range field.FieldMetrics {
			toleranceRules := fieldMetric.Tolerance.ToArray()
			tolerance := &protocol.Tolerance{
				TableURN:       tableURN,
				FieldID:        field.FieldID,
				MetricName:     fieldMetric.MetricName,
				Condition:      fieldMetric.Condition,
				ToleranceRules: toleranceRules,
			}
			tolerances = append(tolerances, tolerance)
		}
	}
	return tolerances
}

//SmartParser parser that automatically Parse yaml that using either CompactSpec or FlatSpec
type SmartParser struct {
}

func NewSmartParser() *SmartParser {
	return &SmartParser{}
}

func (s *SmartParser) Parse(content []byte) (*protocol.ToleranceSpec, error) {
	sType := inspectSpecType(content)
	parser := createParser(sType)
	return parser.Parse(content)
}

func (s *SmartParser) Serialise(tolerances *protocol.ToleranceSpec) ([]byte, error) {
	parser := createParser(compactSpecType)
	return parser.Serialise(tolerances)
}

type specHeader struct {
	TableID string
}

func inspectSpecType(content []byte) specType {
	var header specHeader
	err := yaml.Unmarshal(content, &header)
	if err != nil {
		return flatSpecType
	}
	return compactSpecType
}

type SpecValidator struct {
	metadataStore protocol.MetadataStore
}

//NewSpecValidator create spec validator
func NewSpecValidator(metadataStore protocol.MetadataStore) *SpecValidator {
	return &SpecValidator{metadataStore: metadataStore}
}

func (d *SpecValidator) Validate(spec *protocol.ToleranceSpec) error {
	tableSpec, err := d.metadataStore.GetMetadata(spec.URN)
	if err != nil {
		if err == protocol.ErrTableMetadataNotFound {
			err = fmt.Errorf("error validating: %s ,%w", spec.URN, err)
			err = &protocol.ErrSpecInvalid{Errors: []error{err}, URN: spec.URN}
		}
		return err
	}

	var fieldErrors []error
	for _, tolerance := range spec.Tolerances {
		if tolerance.FieldID != "" {
			_, err = tableSpec.GetFieldSpecByID(tolerance.FieldID)
			if err != nil {
				if err == meta.ErrFieldSpecNotFound {
					err = fmt.Errorf("field ID: %s is not found on table : %s ,%w", tolerance.FieldID, spec.URN, err)
					fieldErrors = append(fieldErrors, err)
				} else {
					return err
				}
			}
		}

		if metric.GetCategory(tolerance.MetricName) != metric.Quality {
			err = fmt.Errorf("metric : %s is not supported", tolerance.MetricName)
			fieldErrors = append(fieldErrors, err)
		}

		if tolerance.MetricName == metric.TrendInconsistencyPct {
			err = fmt.Errorf("metric : %s is not supported", metric.TrendInconsistencyPct.String())
			fieldErrors = append(fieldErrors, err)
		}

		if tolerance.MetricName != metric.InvalidPct {
			if len(tolerance.Condition) > 0 {
				err = fmt.Errorf("[condition] is not supported for %s metric in %s fieldid", tolerance.MetricName, tolerance.FieldID)
				fieldErrors = append(fieldErrors, err)
			}
		}

		// TODO: Add validation of unique fields when unique constraint store is deprecated
	}

	if len(fieldErrors) > 0 {
		err = &protocol.ErrSpecInvalid{Errors: fieldErrors, URN: spec.URN}
		return err
	}

	return nil
}
