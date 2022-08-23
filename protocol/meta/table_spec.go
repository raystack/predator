package meta

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

// Mode type represent Mode of a Field
type Mode string

// FieldType represent Type of a Field
type FieldType string

func (f FieldType) String() string {
	return string(f)
}

const (
	ModeNullable Mode = "NULLABLE"
	ModeRequired Mode = "REQUIRED"
	ModeRepeated Mode = "REPEATED"

	DefaultPartition string = "_PARTITIONTIME"

	FieldTypeString    FieldType = "STRING"
	FieldTypeBytes     FieldType = "BYTES"
	FieldTypeInteger   FieldType = "INTEGER"
	FieldTypeFloat     FieldType = "FLOAT"
	FieldTypeBoolean   FieldType = "BOOLEAN"
	FieldTypeTimestamp FieldType = "TIMESTAMP"
	FieldTypeRecord    FieldType = "RECORD"
	FieldTypeDate      FieldType = "DATE"
	FieldTypeTime      FieldType = "TIME"
	FieldTypeDateTime  FieldType = "DATETIME"
	FieldTypeNumeric   FieldType = "NUMERIC"
	FieldTypeGeography FieldType = "GEOGRAPHY"
	FieldTypeUnknown   FieldType = "UNKNOWN"
)

// TimePartitioning defines the partition interval
type TimePartitioning string

const (
	// DayPartitioning  day-based interval for time partitioning.
	DayPartitioning TimePartitioning = "DAY"

	// HourPartitioning uses an hour-based interval for time partitioning.
	HourPartitioning TimePartitioning = "HOUR"

	// MonthPartitioning uses a month-based interval for time partitioning.
	MonthPartitioning TimePartitioning = "MONTH"

	// YearPartitioning uses a year-based interval for time partitioning.
	YearPartitioning TimePartitioning = "YEAR"
)

//IsNumeric is field type categorized as numeric
func (f FieldType) IsNumeric() bool {
	return f == FieldTypeInteger || f == FieldTypeNumeric || f == FieldTypeFloat
}

//RootLevel is depth level of field spec of the root fields
const RootLevel int = 1

//FieldSpec struct to represent Field information detail
type FieldSpec struct {
	Name      string
	FieldType FieldType
	Mode      Mode
	//Level start from 1 , use RootLevel
	Level  int
	Parent *FieldSpec
	Fields []*FieldSpec
}

//Partition is fully qualified Partition of field
func (f *FieldSpec) ID() string {
	var lineage []string

	var current *FieldSpec
	current = f
	for {
		if current == nil {
			break
		}

		lineage = append([]string{current.Name}, lineage...)
		current = current.Parent
	}

	return strings.Join(lineage, ".")
}

//FromRootPath return path as array of *FieldSpec sorted by root to leaf
func (f *FieldSpec) FromRootPath() []*FieldSpec {
	var path []*FieldSpec

	var current *FieldSpec
	current = f
	for {
		if current == nil {
			break
		}

		path = append([]*FieldSpec{current}, path...)
		current = current.Parent
	}

	if len(path) > 0 {
		return path[:len(path)-1]
	}

	return nil
}

type ByFieldName []*FieldSpec

func (b ByFieldName) Len() int {
	return len(b)
}

func (b ByFieldName) Less(i, j int) bool {

	var iName string
	iField := b[i]
	if iField != nil {
		iName = iField.Name
	}

	var jName string
	jField := b[j]
	if jField != nil {
		jName = jField.Name
	}

	return iName < jName
}

func (b ByFieldName) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

var IngestionTimeField = &FieldSpec{
	Name:      DefaultPartition,
	FieldType: FieldTypeTimestamp,
	Mode:      ModeNullable,
	Level:     0,
}

//TableSpec struct to represent Table information detail
type TableSpec struct {
	ProjectName string
	DatasetName string
	TableName   string
	//PartitionField is field that be used for partition
	//non partitioned table the value of should be empty string ""
	PartitionField         string
	RequirePartitionFilter bool

	//TimePartitioningType type of time partitioning interval
	TimePartitioningType TimePartitioning

	Labels map[string]string
	Fields []*FieldSpec

	//dictionary with fieldID as key and FieldSpec as value
	//to be able to effieciently search FieldSpec information
	m         sync.Mutex
	fieldsMap map[string]*FieldSpec
}

//IsPartitioned is resource partitioned
func (t *TableSpec) IsPartitioned() bool {
	return t.PartitionField != ""
}

//TableID is fully qualified table name
func (t *TableSpec) TableID() string {
	return fmt.Sprintf("%s.%s.%s", t.ProjectName, t.DatasetName, t.TableName)
}

//FieldsFlatten return fields flattened in array
func (t *TableSpec) FieldsFlatten() []*FieldSpec {
	var fields []*FieldSpec

	for _, f := range t.Fields {
		rf := getRecursive(f)
		fields = append(fields, rf...)
	}

	return fields
}

func getRecursive(p *FieldSpec) []*FieldSpec {
	var fields []*FieldSpec
	fields = append(fields, p)

	for _, f := range p.Fields {
		cf := getRecursive(f)
		fields = append(fields, cf...)
	}
	return fields
}

//GetFieldSpecByID to get fieldSpec
func (t *TableSpec) GetFieldSpecByID(fieldID string) (*FieldSpec, error) {
	if len(t.Fields) == 0 {
		return nil, ErrFieldSpecNotFound
	}

	t.m.Lock()
	if t.fieldsMap == nil {
		t.fieldsMap = t.generateFieldMap()
	}
	t.m.Unlock()

	field, ok := t.fieldsMap[fieldID]
	if !ok {
		return nil, ErrFieldSpecNotFound
	}

	return field, nil
}

func (t *TableSpec) generateFieldMap() map[string]*FieldSpec {
	fm := make(map[string]*FieldSpec)
	for _, f := range t.FieldsFlatten() {
		id := f.ID()
		fm[id] = f
	}
	return fm
}

//FieldFinder to group list of fieldspec
type FieldFinder []*FieldSpec

//GetFieldTypeByFieldName to return field type using field name
func (ff FieldFinder) GetFieldTypeByFieldName(fieldName string) (FieldType, error) {
	for _, field := range []*FieldSpec(ff) {
		if field.Name == fieldName {
			return field.FieldType, nil
		}
	}
	return FieldTypeUnknown, ErrFieldSpecNotFound
}

var (
	//ErrTableSpecNotFound thrown when trying to access metadata using MetadataCache
	ErrTableSpecNotFound = errors.New("metadata not found")
	//ErrFieldSpecNotFound fieldspec specified not found
	ErrFieldSpecNotFound = errors.New("fieldspec not found")
)
