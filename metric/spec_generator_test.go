package metric

import (
	"errors"
	"fmt"
	"github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/meta"
	"github.com/odpf/predator/protocol/metric"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMetricSpecGenerator(t *testing.T) {
	projectName := "project"
	datasetName := "dataset"
	tableName := "table"
	tableID := "project.dataset.table"
	lessThanOrEqZeroRule := protocol.ToleranceRule{
		Comparator: protocol.ComparatorLessThanEq,
		Value:      0.0,
	}
	uniqueFields := []string{"unique_field"}
	duplicationTolerance := &protocol.Tolerance{
		TableURN:       tableID,
		MetricName:     metric.DuplicationPct,
		ToleranceRules: []protocol.ToleranceRule{lessThanOrEqZeroRule},
		Metadata: map[string]interface{}{
			metric.UniqueFields: uniqueFields,
		},
	}
	tableCondition := "field1 - field2 - field3 != 0"
	tableInvalidityTolerance := &protocol.Tolerance{
		TableURN:       tableID,
		MetricName:     metric.InvalidPct,
		Condition:      tableCondition,
		ToleranceRules: []protocol.ToleranceRule{lessThanOrEqZeroRule},
	}

	t.Run("BasicMetricSpecGenerator", func(t *testing.T) {
		t.Run("GenerateMetricSpec", func(t *testing.T) {
			t.Run("should return metric spec", func(t *testing.T) {
				tableSpec := &meta.TableSpec{
					ProjectName: projectName,
					DatasetName: datasetName,
					TableName:   tableName,
					Fields:      []*meta.FieldSpec{},
				}
				tolerances := []*protocol.Tolerance{duplicationTolerance}
				toleranceSpec := &protocol.ToleranceSpec{
					URN:        tableSpec.TableID(),
					Tolerances: tolerances,
				}

				metadataStore := mock.NewMetadataStore()
				metadataStore.On("GetMetadata", tableSpec.TableID()).Return(tableSpec, nil)
				defer metadataStore.AssertExpectations(t)

				toleranceStore := mock.NewToleranceStore()
				toleranceStore.On("GetByTableID", tableSpec.TableID()).Return(toleranceSpec, nil)
				defer toleranceStore.AssertExpectations(t)

				expectedSpecs := []*metric.Spec{
					{
						TableID: tableID,
						Name:    metric.UniqueCount,
						Owner:   metric.Table,
						Metadata: map[string]interface{}{
							metric.UniqueFields: uniqueFields,
						},
					},
					{
						TableID: tableID,
						Name:    metric.Count,
						Owner:   metric.Table,
					},
				}

				gen := BasicMetricSpecGenerator{
					metadataStore:  metadataStore,
					toleranceStore: toleranceStore,
				}
				actualSpecs, err := gen.GenerateMetricSpec(tableSpec.TableID())

				assert.Nil(t, err)
				assert.Equal(t, expectedSpecs, actualSpecs)

			})
			t.Run("should return error when failed to get metadata", func(t *testing.T) {
				tableID := "sample-project.sample_dataset.sample_table"
				tolerances := []*protocol.Tolerance{duplicationTolerance}
				toleranceSpec := &protocol.ToleranceSpec{
					URN:        tableID,
					Tolerances: tolerances,
				}
				apiErr := errors.New("API error")

				toleranceStore := mock.NewToleranceStore()
				toleranceStore.On("GetByTableID", tableID).Return(toleranceSpec, nil)
				defer toleranceStore.AssertExpectations(t)

				metadataStore := mock.NewMetadataStore()
				metadataStore.On("GetMetadata", tableID).Return(&meta.TableSpec{}, apiErr)
				defer metadataStore.AssertExpectations(t)

				expectedErr := fmt.Errorf("failed to try to get metadata for table %s ,%w", tableID, apiErr)

				gen := BasicMetricSpecGenerator{
					metadataStore:  metadataStore,
					toleranceStore: toleranceStore,
				}
				result, err := gen.GenerateMetricSpec(tableID)

				assert.Nil(t, result)
				assert.Equal(t, expectedErr, err)
			})
			t.Run("should return error when failed to get tolerance", func(t *testing.T) {
				tableID := "sample-project.sample_dataset.sample_table"
				toleranceSpec := &protocol.ToleranceSpec{
					URN: tableID,
				}
				apiErr := errors.New("API error")

				toleranceStore := mock.NewToleranceStore()
				toleranceStore.On("GetByTableID", tableID).Return(toleranceSpec, apiErr)
				defer toleranceStore.AssertExpectations(t)

				metadataStore := mock.NewMetadataStore()
				defer metadataStore.AssertExpectations(t)

				expectedErr := fmt.Errorf("failed to try to get toleranceSpec for table %s ,%w", tableID, apiErr)

				gen := BasicMetricSpecGenerator{
					metadataStore:  metadataStore,
					toleranceStore: toleranceStore,
				}
				result, err := gen.GenerateMetricSpec(tableID)

				assert.Nil(t, result)
				assert.Equal(t, expectedErr, err)
			})
		})
		t.Run("Generate", func(t *testing.T) {
			t.Run("should return all table and upstream metric spec", func(t *testing.T) {
				tableSpec := &meta.TableSpec{
					ProjectName:    projectName,
					DatasetName:    datasetName,
					TableName:      tableName,
					PartitionField: "",
				}
				tableID := fmt.Sprintf("%s.%s.%s", tableSpec.ProjectName, tableSpec.DatasetName, tableSpec.TableName)
				tolerances := []*protocol.Tolerance{
					duplicationTolerance,
					tableInvalidityTolerance,
				}

				expectedSpecs := []*metric.Spec{
					{
						TableID: tableID,
						Name:    metric.UniqueCount,
						Owner:   metric.Table,
						Metadata: map[string]interface{}{
							metric.UniqueFields: uniqueFields,
						},
					},
					{
						TableID:   tableID,
						Name:      metric.InvalidCount,
						Condition: tableCondition,
						Owner:     metric.Table,
					},
					{
						TableID: tableID,
						Name:    metric.Count,
						Owner:   metric.Table,
					},
				}

				gms := &BasicMetricSpecGenerator{}
				actualSpecs, err := gms.Generate(tableSpec, tolerances)

				assert.Equal(t, expectedSpecs, actualSpecs)
				assert.Nil(t, err)
			})
		})
		t.Run("generateFieldMetricSpecs", func(t *testing.T) {
			t.Run("should return field metric", func(t *testing.T) {
				tableSpec := &meta.TableSpec{
					ProjectName:    projectName,
					DatasetName:    datasetName,
					TableName:      tableName,
					PartitionField: "",
					Fields: []*meta.FieldSpec{
						{
							Name:      "field1",
							FieldType: meta.FieldTypeString,
							Mode:      meta.ModeNullable,
							Parent:    nil,
							Level:     1,
						},
						{
							Name:      "field2",
							FieldType: meta.FieldTypeNumeric,
							Mode:      meta.ModeNullable,
							Parent:    nil,
							Level:     1,
						},
					},
				}
				tolerances := []*protocol.Tolerance{
					{
						TableURN:   tableSpec.TableID(),
						MetricName: metric.NullnessPct,
						FieldID:    "field1",
					},
					{
						TableURN:   tableSpec.TableID(),
						MetricName: metric.Sum,
						FieldID:    "field2",
					},
				}

				expectedSpecs := []*metric.Spec{
					{
						FieldID: "field1",
						TableID: tableID,
						Name:    metric.Count,
						Owner:   metric.Field,
					},
					{
						FieldID: "field1",
						TableID: tableID,
						Name:    metric.NullCount,
						Owner:   metric.Field,
					},
					{
						FieldID: "field2",
						TableID: tableID,
						Name:    metric.Count,
						Owner:   metric.Field,
					},
					{
						FieldID: "field2",
						TableID: tableID,
						Name:    metric.Sum,
						Owner:   metric.Field,
					},
				}

				gms := &BasicMetricSpecGenerator{}
				result := gms.generateFieldMetricSpecs(tableSpec, tolerances)
				assert.Equal(t, expectedSpecs, result)
			})
			t.Run("should return metric specs for child fields", func(t *testing.T) {
				tableSpec := &meta.TableSpec{
					ProjectName:    projectName,
					DatasetName:    datasetName,
					TableName:      tableName,
					PartitionField: "",
					Fields: func() []*meta.FieldSpec {
						field3 := &meta.FieldSpec{
							Name:      "field3",
							FieldType: meta.FieldTypeRecord,
							Mode:      meta.ModeRepeated,
							Parent:    nil,
							Level:     1,
						}
						field4 := &meta.FieldSpec{
							Name:      "field4",
							FieldType: meta.FieldTypeString,
							Mode:      meta.ModeNullable,
							Parent:    field3,
							Level:     2,
						}
						field5 := &meta.FieldSpec{
							Name:      "field5",
							FieldType: meta.FieldTypeString,
							Mode:      meta.ModeNullable,
							Parent:    field3,
							Level:     2,
						}
						field3.Fields = []*meta.FieldSpec{field4, field5}
						s := []*meta.FieldSpec{
							field3,
						}
						return s
					}(),
				}
				tolerances := []*protocol.Tolerance{
					{
						TableURN:   tableSpec.TableID(),
						MetricName: metric.NullnessPct,
						FieldID:    "field3",
					},
					{
						TableURN:   tableSpec.TableID(),
						MetricName: metric.NullnessPct,
						FieldID:    "field3.field4",
					},
					{
						TableURN:   tableSpec.TableID(),
						MetricName: metric.NullnessPct,
						FieldID:    "field3.field5",
					},
				}

				expectedSpecs := []*metric.Spec{
					{
						FieldID: "field3",
						TableID: tableID,
						Name:    metric.Count,
						Owner:   metric.Field,
					},
					{
						FieldID: "field3",
						TableID: tableID,
						Name:    metric.NullCount,
						Owner:   metric.Field,
					},
					{
						FieldID: "field3.field4",
						TableID: tableID,
						Name:    metric.Count,
						Owner:   metric.Field,
					},
					{
						FieldID: "field3.field4",
						TableID: tableID,
						Name:    metric.NullCount,
						Owner:   metric.Field,
					},
					{
						FieldID: "field3.field5",
						TableID: tableID,
						Name:    metric.Count,
						Owner:   metric.Field,
					},
					{
						FieldID: "field3.field5",
						TableID: tableID,
						Name:    metric.NullCount,
						Owner:   metric.Field,
					},
				}

				gms := &BasicMetricSpecGenerator{}
				actualSpecs := gms.generateFieldMetricSpecs(tableSpec, tolerances)

				assert.Equal(t, expectedSpecs, actualSpecs)
			})
			t.Run("should return invalidcount if available in tolerance", func(t *testing.T) {
				tableSpec := &meta.TableSpec{
					ProjectName:    projectName,
					DatasetName:    datasetName,
					TableName:      tableName,
					PartitionField: "",
					Fields: []*meta.FieldSpec{
						{
							Name:      "field6",
							FieldType: meta.FieldTypeInteger,
							Mode:      meta.ModeNullable,
							Parent:    nil,
							Level:     1,
						},
					},
				}

				tableID := fmt.Sprintf("%s.%s.%s", tableSpec.ProjectName, tableSpec.DatasetName, tableSpec.TableName)
				condition := "field6 <= 0"
				tolerances := []*protocol.Tolerance{
					{
						TableURN:       tableID,
						FieldID:        "field6",
						MetricName:     metric.InvalidPct,
						Condition:      condition,
						ToleranceRules: []protocol.ToleranceRule{lessThanOrEqZeroRule},
					},
					{
						TableURN:       tableID,
						FieldID:        "field6",
						MetricName:     metric.NullnessPct,
						ToleranceRules: []protocol.ToleranceRule{lessThanOrEqZeroRule},
					},
				}

				expectedSpecs := []*metric.Spec{
					{
						FieldID: "field6",
						TableID: tableID,
						Name:    metric.Count,
						Owner:   metric.Field,
					},
					{
						FieldID:   "field6",
						TableID:   tableID,
						Name:      metric.InvalidCount,
						Condition: condition,
						Owner:     metric.Field,
					},
					{
						FieldID: "field6",
						TableID: tableID,
						Name:    metric.NullCount,
						Owner:   metric.Field,
					},
				}

				gms := &BasicMetricSpecGenerator{}
				actualSpecs := gms.generateFieldMetricSpecs(tableSpec, tolerances)

				assert.Equal(t, expectedSpecs, actualSpecs)
			})
			t.Run("shouldn't return sum result if field in non-numeric", func(t *testing.T) {
				tableSpec := &meta.TableSpec{
					ProjectName:    projectName,
					DatasetName:    datasetName,
					TableName:      tableName,
					PartitionField: "",
					Fields: []*meta.FieldSpec{
						{
							Name:      "customer_name",
							FieldType: meta.FieldTypeString,
							Mode:      meta.ModeNullable,
							Parent:    nil,
							Level:     1,
						},
					},
				}

				tableID := fmt.Sprintf("%s.%s.%s", tableSpec.ProjectName, tableSpec.DatasetName, tableSpec.TableName)
				tolerances := []*protocol.Tolerance{
					{
						TableURN:       tableID,
						FieldID:        "customer_name",
						MetricName:     metric.Sum,
						ToleranceRules: []protocol.ToleranceRule{lessThanOrEqZeroRule},
					},
				}

				expectedSpecs := []*metric.Spec{
					{
						FieldID: "customer_name",
						TableID: tableID,
						Name:    metric.Count,
						Owner:   metric.Field,
					},
				}

				gms := &BasicMetricSpecGenerator{}
				actualSpecs := gms.generateFieldMetricSpecs(tableSpec, tolerances)
				assert.Equal(t, expectedSpecs, actualSpecs)
			})
			t.Run("should return metrics of columns if only available in tolerance", func(t *testing.T) {
				tableSpec := &meta.TableSpec{
					ProjectName:    projectName,
					DatasetName:    datasetName,
					TableName:      tableName,
					PartitionField: "",
					Fields: []*meta.FieldSpec{
						{
							Name:      "field6",
							FieldType: meta.FieldTypeInteger,
							Mode:      meta.ModeNullable,
							Parent:    nil,
							Level:     1,
						},
						{
							Name:      "field7",
							FieldType: meta.FieldTypeInteger,
							Mode:      meta.ModeNullable,
							Parent:    nil,
							Level:     1,
						},
					},
				}
				tableID := fmt.Sprintf("%s.%s.%s", tableSpec.ProjectName, tableSpec.DatasetName, tableSpec.TableName)
				condition := "field6 <= 0"
				tolerances := []*protocol.Tolerance{
					{
						TableURN:       tableID,
						FieldID:        "field6",
						MetricName:     metric.InvalidPct,
						Condition:      condition,
						ToleranceRules: []protocol.ToleranceRule{lessThanOrEqZeroRule},
					},
					{
						TableURN:       tableID,
						FieldID:        "field6",
						MetricName:     metric.NullnessPct,
						ToleranceRules: []protocol.ToleranceRule{lessThanOrEqZeroRule},
					},
				}

				expectedSpecs := []*metric.Spec{
					{
						FieldID: "field6",
						TableID: tableID,
						Name:    metric.Count,
						Owner:   metric.Field,
					},
					{
						FieldID:   "field6",
						TableID:   tableID,
						Name:      metric.InvalidCount,
						Condition: condition,
						Owner:     metric.Field,
					},
					{
						FieldID: "field6",
						TableID: tableID,
						Name:    metric.NullCount,
						Owner:   metric.Field,
					},
				}

				gms := &BasicMetricSpecGenerator{}
				actualSpecs := gms.generateFieldMetricSpecs(tableSpec, tolerances)

				assert.Equal(t, expectedSpecs, actualSpecs)
			})
		})
		t.Run("generateTableMetricSpecs", func(t *testing.T) {
			t.Run("should return table metrics all metric that exist in tolerance", func(t *testing.T) {
				tableSpec := &meta.TableSpec{
					ProjectName:    projectName,
					DatasetName:    datasetName,
					TableName:      tableName,
					PartitionField: "",
					Fields:         []*meta.FieldSpec{},
				}
				tableID := fmt.Sprintf("%s.%s.%s", tableSpec.ProjectName, tableSpec.DatasetName, tableSpec.TableName)
				tolerances := []*protocol.Tolerance{
					{
						TableURN:       tableID,
						MetricName:     metric.InvalidPct,
						Condition:      tableCondition,
						ToleranceRules: []protocol.ToleranceRule{lessThanOrEqZeroRule},
					},
				}

				expectedSpecs := []*metric.Spec{
					{
						TableID:   tableID,
						Name:      metric.InvalidCount,
						Condition: tableCondition,
						Owner:     metric.Table,
					},
				}

				gms := &BasicMetricSpecGenerator{}
				actualSpecs, err := gms.generateTableMetricSpecs(tolerances)

				assert.Equal(t, expectedSpecs, actualSpecs)
				assert.Nil(t, err)
			})
			t.Run("should return table metrics all metric that exist in tolerance", func(t *testing.T) {
				uniqueFields := []string{"field1", "field2"}
				tableSpec := &meta.TableSpec{
					ProjectName:    projectName,
					DatasetName:    datasetName,
					TableName:      tableName,
					PartitionField: "",
					Fields:         []*meta.FieldSpec{},
				}
				tableID := fmt.Sprintf("%s.%s.%s", tableSpec.ProjectName, tableSpec.DatasetName, tableSpec.TableName)
				tolerances := []*protocol.Tolerance{
					{
						TableURN:       tableID,
						MetricName:     metric.InvalidPct,
						Condition:      tableCondition,
						ToleranceRules: []protocol.ToleranceRule{lessThanOrEqZeroRule},
					},
					{
						TableURN:       tableID,
						MetricName:     metric.DuplicationPct,
						ToleranceRules: []protocol.ToleranceRule{lessThanOrEqZeroRule},
						Metadata: map[string]interface{}{
							metric.UniqueFields: uniqueFields,
						},
					},
				}

				expectedSpecs := []*metric.Spec{
					{
						TableID:   tableID,
						Name:      metric.InvalidCount,
						Condition: tableCondition,
						Owner:     metric.Table,
					},
					{
						TableID: tableID,
						Name:    metric.UniqueCount,
						Owner:   metric.Table,
						Metadata: map[string]interface{}{
							metric.UniqueFields: uniqueFields,
						},
					},
				}

				gms := &BasicMetricSpecGenerator{}
				actualSpecs, err := gms.generateTableMetricSpecs(tolerances)

				assert.Equal(t, expectedSpecs, actualSpecs)
				assert.Nil(t, err)
			})
		})
	})
	t.Run("QualityMetricSpecGenerator", func(t *testing.T) {
		t.Run("GenerateMetricSpec", func(t *testing.T) {
			t.Run("should return metric spec", func(t *testing.T) {
				tableSpec := &meta.TableSpec{
					ProjectName: projectName,
					DatasetName: datasetName,
					TableName:   tableName,
					Fields:      []*meta.FieldSpec{},
				}
				condition := "field1 - field2 - field3 != 0"
				toleranceSpec := &protocol.ToleranceSpec{
					URN: tableSpec.TableID(),
					Tolerances: []*protocol.Tolerance{
						duplicationTolerance,
						{
							TableURN:   tableSpec.TableID(),
							MetricName: metric.RowCount,
						},
						{
							TableURN:   tableSpec.TableID(),
							MetricName: metric.InvalidPct,
							Condition:  condition,
						},
					},
				}

				metadataStore := mock.NewMetadataStore()
				metadataStore.On("GetMetadata", tableSpec.TableID()).Return(tableSpec, nil)
				defer metadataStore.AssertExpectations(t)

				toleranceStore := mock.NewToleranceStore()
				toleranceStore.On("GetByTableID", tableSpec.TableID()).Return(toleranceSpec, nil)
				defer toleranceStore.AssertExpectations(t)

				expectedSpecs := []*metric.Spec{
					{
						TableID: tableID,
						Name:    metric.DuplicationPct,
						Owner:   metric.Table,
					},
					{
						TableID: tableID,
						Name:    metric.RowCount,
						Owner:   metric.Table,
					},
					{
						TableID:   tableID,
						Name:      metric.InvalidPct,
						Condition: condition,
						Owner:     metric.Table,
					},
				}

				gen := QualityMetricSpecGenerator{
					metadataStore:  metadataStore,
					toleranceStore: toleranceStore,
				}
				actualSpecs, err := gen.GenerateMetricSpec(tableSpec.TableID())

				assert.Nil(t, err)
				assert.Equal(t, expectedSpecs, actualSpecs)

			})
			t.Run("should return error when failed to get metadata", func(t *testing.T) {
				tableID := "sample-project.sample_dataset.sample_table"
				toleranceSpec := &protocol.ToleranceSpec{
					URN: tableID,
				}
				apiErr := errors.New("API error")

				toleranceStore := mock.NewToleranceStore()
				toleranceStore.On("GetByTableID", tableID).Return(toleranceSpec, nil)
				defer toleranceStore.AssertExpectations(t)

				metadataStore := mock.NewMetadataStore()
				metadataStore.On("GetMetadata", tableID).Return(&meta.TableSpec{}, apiErr)
				defer metadataStore.AssertExpectations(t)

				expectedErr := fmt.Errorf("failed to try to get metadata for table %s ,%w", tableID, apiErr)

				gen := QualityMetricSpecGenerator{
					metadataStore:  metadataStore,
					toleranceStore: toleranceStore,
				}
				actualSpecs, err := gen.GenerateMetricSpec(tableID)

				assert.Nil(t, actualSpecs)
				assert.Equal(t, expectedErr, err)
			})
			t.Run("should return error when failed to get tolerance", func(t *testing.T) {
				tableID := "sample-project.sample_dataset.sample_table"
				toleranceSpec := &protocol.ToleranceSpec{URN: tableID}
				apiErr := errors.New("API error")

				toleranceStore := mock.NewToleranceStore()
				toleranceStore.On("GetByTableID", tableID).Return(toleranceSpec, apiErr)
				defer toleranceStore.AssertExpectations(t)

				metadataStore := mock.NewMetadataStore()
				defer metadataStore.AssertExpectations(t)

				expectedErr := fmt.Errorf("failed to try to get toleranceSpec for table %s ,%w", tableID, apiErr)

				gen := QualityMetricSpecGenerator{
					metadataStore:  metadataStore,
					toleranceStore: toleranceStore,
				}
				actualSpecs, err := gen.GenerateMetricSpec(tableID)

				assert.Nil(t, actualSpecs)
				assert.Equal(t, expectedErr, err)
			})
		})
		t.Run("generateFieldMetricSpec", func(t *testing.T) {
			t.Run("should generate field metric spec", func(t *testing.T) {
				tableSpec := &meta.TableSpec{
					ProjectName: projectName,
					DatasetName: datasetName,
					TableName:   tableName,
					Fields: []*meta.FieldSpec{
						{
							Name:      "field1",
							FieldType: meta.FieldTypeString,
							Mode:      meta.ModeNullable,
							Parent:    nil,
							Level:     meta.RootLevel,
						},
					},
				}
				condition := "field1 != nil"
				tolerances := []*protocol.Tolerance{
					{
						TableURN:   tableSpec.TableID(),
						MetricName: metric.InvalidPct,
						FieldID:    "field1",
						Condition:  condition,
					},
					{
						TableURN:   tableSpec.TableID(),
						MetricName: metric.TrendInconsistencyPct,
						FieldID:    "field1",
					},
					{
						TableURN:   tableSpec.TableID(),
						MetricName: metric.NullnessPct,
						FieldID:    "field1",
					},
				}

				expectedSpecs := []*metric.Spec{
					{
						FieldID:   "field1",
						TableID:   tableID,
						Name:      metric.InvalidPct,
						Condition: condition,
						Owner:     metric.Field,
					},
					{
						FieldID:  "field1",
						TableID:  tableID,
						Name:     metric.TrendInconsistencyPct,
						Optional: true,
						Owner:    metric.Field,
					},
					{
						FieldID: "field1",
						TableID: tableID,
						Name:    metric.NullnessPct,
						Owner:   metric.Field,
					},
				}

				actualSpecs := generateFieldMetricSpecs(tolerances)

				assert.Equal(t, expectedSpecs, actualSpecs)
			})
			t.Run("should generate field metric spec if only specified in tolerance", func(t *testing.T) {
				tableSpec := &meta.TableSpec{
					ProjectName: projectName,
					DatasetName: datasetName,
					TableName:   tableName,
					Fields: []*meta.FieldSpec{
						{
							Name:      "field1",
							FieldType: meta.FieldTypeString,
							Mode:      meta.ModeNullable,
							Parent:    nil,
							Level:     1,
						},
					},
				}

				tolerances := []*protocol.Tolerance{
					{
						TableURN:   tableSpec.TableID(),
						MetricName: metric.TrendInconsistencyPct,
						FieldID:    "field1",
					},
				}

				expectedSpecs := []*metric.Spec{
					{
						FieldID:  "field1",
						TableID:  tableID,
						Name:     metric.TrendInconsistencyPct,
						Optional: true,
						Owner:    metric.Field,
					},
				}

				actualSpecs := generateFieldMetricSpecs(tolerances)

				assert.Equal(t, expectedSpecs, actualSpecs)
			})
		})
	})
}
