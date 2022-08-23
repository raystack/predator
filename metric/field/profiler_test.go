package field

import (
	"errors"
	"fmt"
	"github.com/odpf/predator/metric/common"
	"github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/odpf/predator/protocol/meta"
	"github.com/odpf/predator/protocol/metric"
	"github.com/odpf/predator/protocol/query"
	"github.com/stretchr/testify/assert"
	testifyMock "github.com/stretchr/testify/mock"
	"strings"
	"testing"
)

func TestFieldProfiler(t *testing.T) {
	fieldRootA := &meta.FieldSpec{
		Name:      "field_root_a",
		FieldType: meta.FieldTypeRecord,
		Mode:      meta.ModeRepeated,
		Parent:    nil,
		Level:     meta.RootLevel,
	}

	fieldChildA := &meta.FieldSpec{
		Name:      "field_root_a_child_a",
		FieldType: meta.FieldTypeRecord,
		Mode:      meta.ModeRepeated,
		Parent:    fieldRootA,
		Level:     2,
	}

	fieldGrandChildA := &meta.FieldSpec{
		Name:      "field_root_a_grandchild_a",
		FieldType: meta.FieldTypeInteger,
		Mode:      meta.ModeNullable,
		Parent:    fieldChildA,
		Level:     3,
	}

	fieldRootA.Fields = []*meta.FieldSpec{fieldChildA}
	fieldChildA.Fields = []*meta.FieldSpec{fieldGrandChildA}

	fieldRootB := &meta.FieldSpec{
		Name:      "field_root_b",
		FieldType: meta.FieldTypeString,
		Mode:      meta.ModeNullable,
		Parent:    nil,
		Level:     meta.RootLevel,
	}

	fieldRootC := &meta.FieldSpec{
		Name:      "field_root_c",
		FieldType: meta.FieldTypeString,
		Mode:      meta.ModeNullable,
		Parent:    nil,
		Level:     meta.RootLevel,
	}

	fieldRootD := &meta.FieldSpec{
		Name:      "field_root_d",
		FieldType: meta.FieldTypeRecord,
		Mode:      meta.ModeRepeated,
		Parent:    nil,
		Level:     meta.RootLevel,
	}

	fieldRootDChildB := &meta.FieldSpec{
		Name:      "field_root_d_child_b",
		FieldType: meta.FieldTypeString,
		Mode:      meta.ModeNullable,
		Parent:    fieldRootD,
		Level:     2,
	}

	fieldRootDChildC := &meta.FieldSpec{
		Name:      "field_root_d_child_c",
		FieldType: meta.FieldTypeString,
		Mode:      meta.ModeNullable,
		Parent:    fieldRootD,
		Level:     2,
	}

	fieldRootD.Fields = []*meta.FieldSpec{fieldRootDChildB, fieldRootDChildC}

	entry := protocol.NewEntry()

	t.Run("groupMetricSpecsByBranch", func(t *testing.T) {
		t.Run("should group metric specs by closest repeated ancestor field", func(t *testing.T) {
			tableSpec := &meta.TableSpec{
				Fields: []*meta.FieldSpec{
					fieldRootD,
					fieldRootA,
					fieldRootC,
				},
			}

			metricSpecs := []*metric.Spec{
				{
					FieldID: "field_root_c",
					Name:    metric.NullCount,
				},
				{
					FieldID: "field_root_a",
					Name:    metric.NullCount,
				},
				{
					FieldID: "field_root_a.field_root_a_child_a",
					Name:    metric.NullCount,
				},
				{
					FieldID: "field_root_a.field_root_a_child_a.field_root_a_grandchild_a",
					Name:    metric.NullCount,
				},
				{
					FieldID: "field_root_d",
					Name:    metric.NullCount,
				},
				{
					FieldID: "field_root_d.field_root_d_child_b",
					Name:    metric.NullCount,
				},
				{
					FieldID: "field_root_d.field_root_d_child_c",
					Name:    metric.NullCount,
				},
			}

			expected := map[*meta.FieldSpec][]*metric.Spec{
				fieldRootD: {
					{
						FieldID: "field_root_d.field_root_d_child_b",
						Name:    metric.NullCount,
					},
					{
						FieldID: "field_root_d.field_root_d_child_c",
						Name:    metric.NullCount,
					},
				},
				nil: {
					{
						FieldID: "field_root_c",
						Name:    metric.NullCount,
					},
					{
						FieldID: "field_root_a",
						Name:    metric.NullCount,
					},
					{
						FieldID: "field_root_d",
						Name:    metric.NullCount,
					},
				},
				fieldRootA: {
					{
						FieldID: "field_root_a.field_root_a_child_a",
						Name:    metric.NullCount,
					},
				},
				fieldChildA: {
					{
						FieldID: "field_root_a.field_root_a_child_a.field_root_a_grandchild_a",
						Name:    metric.NullCount,
					},
				},
			}
			groups, err := groupMetricSpecsByBranch(tableSpec, metricSpecs)
			assert.Nil(t, err)

			assert.Equal(t, expected, groups)
		})
	})
	t.Run("createUnnest", func(t *testing.T) {
		t.Run("should return Unnest", func(t *testing.T) {
			result := createUnnest(fieldChildA, []*meta.FieldSpec{fieldRootA})

			expected := &query.Unnest{
				ColumnName: "level1.`field_root_a_child_a`",
				Alias:      "level2",
			}

			assert.Equal(t, expected, result)

		})
		t.Run("should return Unnest column name that namespace only contains closest repeated parent", func(t *testing.T) {
			parents := []*meta.FieldSpec{fieldRootA, fieldChildA}
			result := createUnnest(fieldGrandChildA, parents)

			expected := &query.Unnest{
				ColumnName: "level2.`field_root_a_grandchild_a`",
				Alias:      "level3",
			}

			assert.Equal(t, expected, result)

		})
	})
	t.Run("generateUnnest", func(t *testing.T) {
		t.Run("should return list of unnest", func(t *testing.T) {
			parents := []*meta.FieldSpec{fieldRootA, fieldChildA}
			result := generateUnnest(parents)

			expected := []*query.Unnest{
				{
					ColumnName: "`field_root_a`",
					Alias:      "level1",
				},
				{
					ColumnName: "level1.`field_root_a_child_a`",
					Alias:      "level2",
				},
			}

			assert.Equal(t, expected, result)
		})

		t.Run("should return list of unnest only from repeated column", func(t *testing.T) {
			parents := []*meta.FieldSpec{fieldRootA, fieldChildA, fieldGrandChildA}
			result := generateUnnest(parents)

			expected := []*query.Unnest{
				{
					ColumnName: "`field_root_a`",
					Alias:      "level1",
				},
				{
					ColumnName: "level1.`field_root_a_child_a`",
					Alias:      "level2",
				},
			}
			assert.Equal(t, expected, result)
		})
	})
	t.Run("getUnnestedColumnName", func(t *testing.T) {
		t.Run("should return alias name given repeated field spec", func(t *testing.T) {
			result := getUnnestedColumnName(fieldRootA)
			assert.Equal(t, "`field_root_a`", result)
		})
		t.Run("should return alias name given non repeated field spec", func(t *testing.T) {
			sampleField := &meta.FieldSpec{
				Name:      "sample_field",
				FieldType: meta.FieldTypeString,
				Mode:      meta.ModeNullable,
				Parent:    nil,
				Level:     1,
			}

			result := getUnnestedColumnName(sampleField)
			assert.Equal(t, "`sample_field`", result)
		})
		t.Run("should return alias name given non repeated field spec but has repeated parent", func(t *testing.T) {
			result := getUnnestedColumnName(fieldGrandChildA)
			assert.Equal(t, "level2.`field_root_a_grandchild_a`", result)
		})
		t.Run("should return alias name given non repeated field spec but has repeated parent", func(t *testing.T) {
			parent := &meta.FieldSpec{
				Name:      "parent_field",
				FieldType: meta.FieldTypeRecord,
				Mode:      meta.ModeRepeated,
				Parent:    nil,
				Level:     meta.RootLevel,
			}

			parent2 := &meta.FieldSpec{
				Name:      "parent2_field",
				FieldType: meta.FieldTypeRecord,
				Mode:      meta.ModeNullable,
				Parent:    parent,
				Level:     2,
			}

			repeatedField := &meta.FieldSpec{
				Name:      "repeated_field",
				FieldType: meta.FieldTypeString,
				Mode:      meta.ModeRepeated,
				Parent:    parent2,
				Level:     3,
			}

			parent.Fields = []*meta.FieldSpec{parent2}
			parent2.Fields = []*meta.FieldSpec{repeatedField}

			result := getUnnestedColumnName(repeatedField)
			assert.Equal(t, "level1.`parent2_field`.`repeated_field`", result)
		})
	})
	t.Run("getAlias", func(t *testing.T) {
		t.Run("should return alias metric name", func(t *testing.T) {
			result := getAlias(fieldRootA.Name, metric.Count, 0)
			assert.Equal(t, "count_field_root_a_0", result)
		})
	})
	t.Run("Profile", func(t *testing.T) {
		profile := &job.Profile{
			Filter:    "active = true",
			GroupName: "`field_grouping`",
			URN:       "sample-project.sample_dataset.sample_table",
		}

		suites := []struct {
			Profile     *job.Profile
			Description string
			Spec        *meta.TableSpec
			Queries     [][]string
			MetricSpecs []*metric.Spec
		}{
			{
				Profile:     profile,
				Description: "should use custom filter expression",
				Spec: &meta.TableSpec{
					ProjectName: "sample-project",
					DatasetName: "sample_dataset",
					TableName:   "sample_table",
					Labels:      map[string]string{"key": "value"},
					Fields:      []*meta.FieldSpec{fieldRootB},
				},
				Queries: [][]string{
					{
						"SELECT `field_grouping` AS __group_value , count(`field_root_b`) as count_field_root_b_0 , countif(`field_root_b` is null) as nullcount_field_root_b_1",
						"FROM `sample-project.sample_dataset.sample_table`",
						"WHERE active = true",
						"GROUP BY `field_grouping`",
					},
				},
				MetricSpecs: []*metric.Spec{
					{
						Name:    metric.Count,
						FieldID: "field_root_b",
						TableID: "sample-project.sample_dataset.sample_table",
					},
					{
						Name:    metric.NullCount,
						FieldID: "field_root_b",
						TableID: "sample-project.sample_dataset.sample_table",
					},
				},
			},
			{
				Profile: &job.Profile{
					GroupName: "`field_grouping`",
					URN:       "sample-project.sample_dataset.sample_table",
				},
				Description: "should use no filter when filter is empty",
				Spec: &meta.TableSpec{
					ProjectName: "sample-project",
					DatasetName: "sample_dataset",
					TableName:   "sample_table",
					Labels:      map[string]string{"key": "value"},
					Fields:      []*meta.FieldSpec{fieldRootB},
				},
				Queries: [][]string{
					{
						"SELECT `field_grouping` AS __group_value , count(`field_root_b`) as count_field_root_b_0 , countif(`field_root_b` is null) as nullcount_field_root_b_1",
						"FROM `sample-project.sample_dataset.sample_table`",
						"WHERE TRUE",
						"GROUP BY `field_grouping`",
					},
				},
				MetricSpecs: []*metric.Spec{
					{
						Name:    metric.Count,
						FieldID: "field_root_b",
						TableID: "sample-project.sample_dataset.sample_table",
					},
					{
						Name:    metric.NullCount,
						FieldID: "field_root_b",
						TableID: "sample-project.sample_dataset.sample_table",
					},
				},
			},
			{
				Profile: &job.Profile{
					URN: "sample-project.sample_dataset.sample_table",
				},
				Description: "should use no group by when group name is empty",
				Spec: &meta.TableSpec{
					ProjectName: "sample-project",
					DatasetName: "sample_dataset",
					TableName:   "sample_table",
					Labels:      map[string]string{"key": "value"},
					Fields:      []*meta.FieldSpec{fieldRootB},
				},
				Queries: [][]string{
					{
						"SELECT count(`field_root_b`) as count_field_root_b_0 , countif(`field_root_b` is null) as nullcount_field_root_b_1",
						"FROM `sample-project.sample_dataset.sample_table`",
						"WHERE TRUE",
					},
				},
				MetricSpecs: []*metric.Spec{
					{
						Name:    metric.Count,
						FieldID: "field_root_b",
						TableID: "sample-project.sample_dataset.sample_table",
					},
					{
						Name:    metric.NullCount,
						FieldID: "field_root_b",
						TableID: "sample-project.sample_dataset.sample_table",
					},
				},
			},
			{
				Profile:     profile,
				Description: "should only profile columns available in metric specs",
				Spec: &meta.TableSpec{
					ProjectName:    "sample-project",
					DatasetName:    "sample_dataset",
					TableName:      "sample_table",
					PartitionField: "",
					Labels:         map[string]string{"key": "value"},
					Fields:         []*meta.FieldSpec{fieldRootB, fieldRootA},
				},
				Queries: [][]string{
					{
						"SELECT `field_grouping` AS __group_value , count(`field_root_b`) as count_field_root_b_0 , countif(`field_root_b` is null) as nullcount_field_root_b_1",
						"FROM `sample-project.sample_dataset.sample_table`",
						"WHERE active = true",
						"GROUP BY `field_grouping`",
					},
				},
				MetricSpecs: []*metric.Spec{
					{
						Name:    metric.Count,
						FieldID: "field_root_b",
						TableID: "sample-project.sample_dataset.sample_table",
					},
					{
						Name:    metric.NullCount,
						FieldID: "field_root_b",
						TableID: "sample-project.sample_dataset.sample_table",
					},
				},
			},
			{
				Profile:     profile,
				Description: "should handle repeated string field",
				Spec: &meta.TableSpec{
					ProjectName:    "sample-project",
					DatasetName:    "sample_dataset",
					TableName:      "sample_table",
					PartitionField: "_partitiontime",
					Labels:         map[string]string{"key": "value"},
					Fields: []*meta.FieldSpec{{
						Name:      "repeated_field",
						FieldType: meta.FieldTypeString,
						Mode:      meta.ModeRepeated,
						Parent:    nil,
						Level:     1,
					}},
				},
				Queries: [][]string{
					{
						"SELECT `field_grouping` AS __group_value , countif(array_length(`repeated_field`)>0) as count_repeated_field_0 , countif(array_length(`repeated_field`)=0) as nullcount_repeated_field_1",
						"FROM `sample-project.sample_dataset.sample_table`",
						"WHERE active = true",
						"GROUP BY `field_grouping`",
					},
				},
				MetricSpecs: []*metric.Spec{
					{
						Name:    metric.Count,
						FieldID: "repeated_field",
						TableID: "sample-project.sample_dataset.sample_table",
					},
					{
						Name:    metric.NullCount,
						FieldID: "repeated_field",
						TableID: "sample-project.sample_dataset.sample_table",
					},
				},
			},
			{
				Profile:     profile,
				Description: "should handle repeated string field with parent",
				Spec: &meta.TableSpec{
					ProjectName: "sample-project",
					DatasetName: "sample_dataset",
					TableName:   "sample_table",
					Labels:      map[string]string{"key": "value"},
					Fields: func() []*meta.FieldSpec {
						parent := &meta.FieldSpec{
							Name:      "parent_field",
							FieldType: meta.FieldTypeRecord,
							Mode:      meta.ModeRepeated,
							Parent:    nil,
							Level:     1,
						}
						parent.Fields = []*meta.FieldSpec{
							{
								Name:      "repeated_field",
								FieldType: meta.FieldTypeString,
								Mode:      meta.ModeRepeated,
								Parent:    parent,
								Level:     2,
							},
						}

						return []*meta.FieldSpec{parent}
					}(),
				},
				Queries: [][]string{
					{
						"SELECT `field_grouping` AS __group_value , countif(array_length(`parent_field`)>0) as count_parent_field_0 , countif(array_length(`parent_field`)=0) as nullcount_parent_field_1",
						"FROM `sample-project.sample_dataset.sample_table`",
						"WHERE active = true",
						"GROUP BY `field_grouping`",
					},
					{
						"SELECT `field_grouping` AS __group_value , countif(array_length(level1.`repeated_field`)>0) as count_repeated_field_0 , countif(array_length(level1.`repeated_field`)=0) as nullcount_repeated_field_1",
						"FROM `sample-project.sample_dataset.sample_table` , UNNEST(`parent_field`) as level1",
						"WHERE active = true",
						"GROUP BY `field_grouping`",
					},
				},
				MetricSpecs: []*metric.Spec{
					{
						Name:    metric.Count,
						FieldID: "parent_field",
						TableID: "sample-project.sample_dataset.sample_table",
					},
					{
						Name:    metric.NullCount,
						FieldID: "parent_field",
						TableID: "sample-project.sample_dataset.sample_table",
					},
					{
						Name:    metric.Count,
						FieldID: "parent_field.repeated_field",
						TableID: "sample-project.sample_dataset.sample_table",
					},
					{
						Name:    metric.NullCount,
						FieldID: "parent_field.repeated_field",
						TableID: "sample-project.sample_dataset.sample_table",
					},
				},
			},
			{
				Profile:     profile,
				Description: "should handle repeated string field with two parents",
				Spec: &meta.TableSpec{
					ProjectName:    "sample-project",
					DatasetName:    "sample_dataset",
					TableName:      "sample_table",
					PartitionField: "_partitiontime",
					Labels:         map[string]string{"key": "value"},
					Fields: func() []*meta.FieldSpec {
						parent := &meta.FieldSpec{
							Name:      "parent_field",
							FieldType: meta.FieldTypeRecord,
							Mode:      meta.ModeRepeated,
							Parent:    nil,
							Level:     1,
						}
						parent2 := &meta.FieldSpec{
							Name:      "parent2_field",
							FieldType: meta.FieldTypeRecord,
							Mode:      meta.ModeRepeated,
							Parent:    parent,
							Level:     2,
						}

						parent.Fields = []*meta.FieldSpec{parent2}
						parent2.Fields = []*meta.FieldSpec{
							{
								Name:      "repeated_field",
								FieldType: meta.FieldTypeString,
								Mode:      meta.ModeRepeated,
								Parent:    parent2,
								Level:     3,
							},
						}

						return []*meta.FieldSpec{parent}
					}(),
				},
				Queries: [][]string{
					{
						"SELECT `field_grouping` AS __group_value , countif(array_length(`parent_field`)>0) as count_parent_field_0 , countif(array_length(`parent_field`)=0) as nullcount_parent_field_1",
						"FROM `sample-project.sample_dataset.sample_table`",
						"WHERE active = true",
						"GROUP BY `field_grouping`",
					},
					{
						"SELECT `field_grouping` AS __group_value , countif(array_length(level2.`repeated_field`)>0) as count_repeated_field_0 , countif(array_length(level2.`repeated_field`)=0) as nullcount_repeated_field_1",
						"FROM `sample-project.sample_dataset.sample_table` , UNNEST(`parent_field`) as level1 , UNNEST(level1.`parent2_field`) as level2",
						"WHERE active = true",
						"GROUP BY `field_grouping`",
					},
					{
						"SELECT `field_grouping` AS __group_value , countif(array_length(level1.`parent2_field`)>0) as count_parent2_field_0 , countif(array_length(level1.`parent2_field`)=0) as nullcount_parent2_field_1",
						"FROM `sample-project.sample_dataset.sample_table` , UNNEST(`parent_field`) as level1",
						"WHERE active = true",
						"GROUP BY `field_grouping`",
					},
				},
				MetricSpecs: []*metric.Spec{
					{
						Name:    metric.Count,
						FieldID: "parent_field",
						TableID: "sample-project.sample_dataset.sample_table",
					},
					{
						Name:    metric.NullCount,
						FieldID: "parent_field",
						TableID: "sample-project.sample_dataset.sample_table",
					},
					{
						Name:    metric.Count,
						FieldID: "parent_field.parent2_field",
						TableID: "sample-project.sample_dataset.sample_table",
					},
					{
						Name:    metric.NullCount,
						FieldID: "parent_field.parent2_field",
						TableID: "sample-project.sample_dataset.sample_table",
					},
					{
						Name:    metric.Count,
						FieldID: "parent_field.parent2_field.repeated_field",
						TableID: "sample-project.sample_dataset.sample_table",
					},
					{
						Name:    metric.NullCount,
						FieldID: "parent_field.parent2_field.repeated_field",
						TableID: "sample-project.sample_dataset.sample_table",
					},
				},
			},
			{
				Profile:     profile,
				Description: "should handle repeated string field with two parents with one parent not repeated",
				Spec: &meta.TableSpec{
					ProjectName:    "sample-project",
					DatasetName:    "sample_dataset",
					TableName:      "sample_table",
					PartitionField: "_partitiontime",
					Labels:         map[string]string{"key": "value"},
					Fields: func() []*meta.FieldSpec {
						parent := &meta.FieldSpec{
							Name:      "parent_field",
							FieldType: meta.FieldTypeRecord,
							Mode:      meta.ModeRepeated,
							Parent:    nil,
							Level:     meta.RootLevel,
						}
						parent2 := &meta.FieldSpec{
							Name:      "parent2_field",
							FieldType: meta.FieldTypeRecord,
							Mode:      meta.ModeNullable,
							Parent:    parent,
							Level:     2,
						}

						parent.Fields = []*meta.FieldSpec{parent2}
						parent2.Fields = []*meta.FieldSpec{
							{
								Name:      "repeated_field",
								FieldType: meta.FieldTypeString,
								Mode:      meta.ModeRepeated,
								Parent:    parent2,
								Level:     3,
							},
						}

						return []*meta.FieldSpec{parent}
					}(),
				},
				Queries: [][]string{
					{
						"SELECT `field_grouping` AS __group_value , countif(array_length(`parent_field`)>0) as count_parent_field_0 , countif(array_length(`parent_field`)=0) as nullcount_parent_field_1",
						"FROM `sample-project.sample_dataset.sample_table`",
						"WHERE active = true",
						"GROUP BY `field_grouping`",
					},
					{
						"SELECT `field_grouping` AS __group_value , count(level1.`parent2_field`) as count_parent2_field_0 , countif(level1.`parent2_field` is null) as nullcount_parent2_field_1",
						", countif(array_length(level1.`parent2_field`.`repeated_field`)>0) as count_repeated_field_2 , countif(array_length(level1.`parent2_field`.`repeated_field`)=0) as nullcount_repeated_field_3",
						"FROM `sample-project.sample_dataset.sample_table` , UNNEST(`parent_field`) as level1",
						"WHERE active = true",
						"GROUP BY `field_grouping`",
					},
				},
				MetricSpecs: []*metric.Spec{
					{
						Name:    metric.Count,
						FieldID: "parent_field",
						TableID: "sample-project.sample_dataset.sample_table",
					},
					{
						Name:    metric.NullCount,
						FieldID: "parent_field",
						TableID: "sample-project.sample_dataset.sample_table",
					},
					{
						Name:    metric.Count,
						FieldID: "parent_field.parent2_field",
						TableID: "sample-project.sample_dataset.sample_table",
					},
					{
						Name:    metric.NullCount,
						FieldID: "parent_field.parent2_field",
						TableID: "sample-project.sample_dataset.sample_table",
					},
					{
						Name:    metric.Count,
						FieldID: "parent_field.parent2_field.repeated_field",
						TableID: "sample-project.sample_dataset.sample_table",
					},
					{
						Name:    metric.NullCount,
						FieldID: "parent_field.parent2_field.repeated_field",
						TableID: "sample-project.sample_dataset.sample_table",
					},
				},
			},
			{
				Profile:     profile,
				Description: "should profile field with condition",
				Spec: &meta.TableSpec{
					ProjectName:    "sample-project",
					DatasetName:    "sample_dataset",
					TableName:      "sample_table",
					PartitionField: "_partitiontime",
					Labels:         map[string]string{"key": "value"},
					Fields: []*meta.FieldSpec{{
						Name:      "field_numeric",
						FieldType: meta.FieldTypeFloat,
						Mode:      meta.ModeNullable,
						Parent:    nil,
						Level:     1,
					}},
				},
				Queries: [][]string{
					{
						"SELECT `field_grouping` AS __group_value , count(`field_numeric`) as count_field_numeric_0 , countif(`field_numeric` is null) as nullcount_field_numeric_1 , sum(cast(`field_numeric` as float64)) as sum_field_numeric_2 , countif(`field_numeric` <= 0) as invalidcount_field_numeric_3",
						"FROM `sample-project.sample_dataset.sample_table`",
						"WHERE active = true",
						"GROUP BY `field_grouping`",
					},
				},
				MetricSpecs: []*metric.Spec{
					{
						Name:    metric.Count,
						FieldID: "field_numeric",
						TableID: "sample-project.sample_dataset.sample_table",
					},
					{
						Name:    metric.NullCount,
						FieldID: "field_numeric",
						TableID: "sample-project.sample_dataset.sample_table",
					},
					{
						Name:    metric.Sum,
						FieldID: "field_numeric",
						TableID: "sample-project.sample_dataset.sample_table",
					},
					{
						Name:      metric.InvalidCount,
						FieldID:   "field_numeric",
						TableID:   "sample-project.sample_dataset.sample_table",
						Condition: "`field_numeric` <= 0",
					},
				},
			},
		}
		for _, test := range suites {
			t.Run(test.Description, func(t *testing.T) {
				metadataStore := mock.NewMetadataStore()
				defer metadataStore.AssertExpectations(t)

				queryExecutor := mock.NewQueryExecutor()
				defer queryExecutor.AssertExpectations(t)

				var rows []protocol.Row

				queryExecutor.On("Run", testifyMock.Anything, testifyMock.AnythingOfType("string"), job.FieldLevelQuery).Return(rows, nil)
				metadataStore.On("GetMetadata", test.Spec.TableID()).Return(test.Spec, nil)

				profiler := New(queryExecutor, metadataStore)
				profiler.Profile(entry, test.Profile, test.MetricSpecs)

				for i, call := range queryExecutor.Calls {
					sql := strings.Join(test.Queries[i], " ")
					fmt.Println(sql)
					assert.Equal(t, sql, call.Arguments[1].(string))
				}

				assert.Equal(t, len(test.Queries), len(queryExecutor.Calls))
			})
		}
		t.Run("should return metrics", func(t *testing.T) {
			profile := &job.Profile{
				Filter:    "active = true",
				GroupName: "field_grouping",
				URN:       "sample-project.sample_dataset.sample_table",
			}

			orderDurationMinute := &meta.FieldSpec{
				Name:      "order_duration_minute",
				FieldType: meta.FieldTypeInteger,
				Mode:      meta.ModeNullable,
				Parent:    nil,
				Level:     1,
			}

			spec := &meta.TableSpec{
				ProjectName:    "sample-project",
				DatasetName:    "sample_dataset",
				TableName:      "sample_table",
				PartitionField: "_partitiontime",
				Labels:         map[string]string{"key": "value"},
				Fields:         []*meta.FieldSpec{fieldRootC, fieldRootB, orderDurationMinute},
			}
			recordCountFieldRootC := 295
			recordNullCountFieldRootC := 5
			recordCountFieldRootB := 300
			recordNullCountFieldRootB := 0

			recordCountOrderDurationMinute := 295
			recordNullCountOrderDurationMinute := 5

			metricSpecs := []*metric.Spec{
				{
					Name:     metric.Count,
					FieldID:  fieldRootC.ID(),
					TableID:  spec.TableID(),
					Owner:    metric.Field,
					Optional: false,
				},
				{
					Name:     metric.NullCount,
					FieldID:  fieldRootC.ID(),
					TableID:  spec.TableID(),
					Owner:    metric.Field,
					Optional: false,
				},
				{
					Name:     metric.Count,
					FieldID:  fieldRootB.ID(),
					TableID:  spec.TableID(),
					Owner:    metric.Field,
					Optional: false,
				},
				{
					Name:     metric.NullCount,
					FieldID:  fieldRootB.ID(),
					TableID:  spec.TableID(),
					Owner:    metric.Field,
					Optional: false,
				},
				{
					Name:     metric.Count,
					FieldID:  orderDurationMinute.ID(),
					TableID:  spec.TableID(),
					Owner:    metric.Field,
					Optional: false,
				},
				{
					Name:     metric.NullCount,
					FieldID:  orderDurationMinute.ID(),
					TableID:  spec.TableID(),
					Owner:    metric.Field,
					Optional: false,
				},
			}

			rows := []protocol.Row{
				{
					"count_field_root_c_0":              int64(recordCountFieldRootC),
					"nullcount_field_root_c_1":          int64(recordNullCountFieldRootC),
					"count_field_root_b_2":              int64(recordCountFieldRootB),
					"nullcount_field_root_b_3":          int64(recordNullCountFieldRootB),
					"count_order_duration_minute_4":     int64(recordCountOrderDurationMinute),
					"nullcount_order_duration_minute_5": int64(recordNullCountOrderDurationMinute),
					common.GroupAlias:                   "ID",
				},
			}

			expected := []*metric.Metric{
				{
					FieldID:    fieldRootC.ID(),
					Type:       metric.Count,
					Category:   metric.Basic,
					Owner:      metric.Field,
					Value:      float64(recordCountFieldRootC),
					GroupValue: "ID",
				},
				{
					FieldID:    fieldRootC.ID(),
					Type:       metric.NullCount,
					Category:   metric.Basic,
					Owner:      metric.Field,
					Value:      float64(recordNullCountFieldRootC),
					GroupValue: "ID",
				},
				{
					FieldID:    fieldRootB.ID(),
					Type:       metric.Count,
					Category:   metric.Basic,
					Owner:      metric.Field,
					Value:      float64(recordCountFieldRootB),
					GroupValue: "ID",
				},
				{
					FieldID:    fieldRootB.ID(),
					Type:       metric.NullCount,
					Category:   metric.Basic,
					Owner:      metric.Field,
					Value:      float64(recordNullCountFieldRootB),
					GroupValue: "ID",
				},
				{
					FieldID:    orderDurationMinute.ID(),
					Type:       metric.Count,
					Category:   metric.Basic,
					Owner:      metric.Field,
					Value:      float64(recordCountOrderDurationMinute),
					GroupValue: "ID",
				},
				{
					FieldID:    orderDurationMinute.ID(),
					Type:       metric.NullCount,
					Category:   metric.Basic,
					Owner:      metric.Field,
					Value:      float64(recordNullCountOrderDurationMinute),
					GroupValue: "ID",
				},
			}

			metadataStore := mock.NewMetadataStore()
			defer metadataStore.AssertExpectations(t)

			queryExecutor := mock.NewQueryExecutor()
			defer queryExecutor.AssertExpectations(t)

			metadataStore.On("GetMetadata", profile.URN).Return(spec, nil)

			queryExecutor.On("Run", testifyMock.Anything, testifyMock.AnythingOfType("string"), job.FieldLevelQuery).Return(rows, nil)

			profiler := New(queryExecutor, metadataStore)
			fieldMetrics, err := profiler.Profile(entry, profile, metricSpecs)

			assert.Equal(t, expected, fieldMetrics)
			assert.Nil(t, err)
		})
		t.Run("should return Field Metrics with nested fields", func(t *testing.T) {
			profile := &job.Profile{
				Filter:    "active = true",
				GroupName: "`field_grouping`",
				URN:       "sample-project.sample_dataset.sample_table",
			}

			spec := &meta.TableSpec{
				ProjectName:    "sample-project",
				DatasetName:    "sample_dataset",
				TableName:      "sample_table",
				PartitionField: "_partitiontime",
				Labels:         map[string]string{"key": "value"},
				Fields:         []*meta.FieldSpec{fieldRootB, fieldRootA},
			}
			recordCountFieldRootB := 300
			recordNullCountFieldRootB := 0
			recordCountFieldRootA := 400
			recordNullCountFieldRootA := 0
			recordCountFieldChildA := 500
			recordNullCountFieldChildA := 0
			recordCountFieldGrandChildA := 600
			recordNullCountFieldGrandChildA := 0

			metricSpecs := []*metric.Spec{
				{
					Name:     metric.Count,
					FieldID:  fieldRootB.ID(),
					TableID:  spec.TableID(),
					Owner:    metric.Field,
					Optional: false,
				},
				{
					Name:     metric.NullCount,
					FieldID:  fieldRootB.ID(),
					TableID:  spec.TableID(),
					Owner:    metric.Field,
					Optional: false,
				},
				{
					Name:     metric.Count,
					FieldID:  fieldRootA.ID(),
					TableID:  spec.TableID(),
					Owner:    metric.Field,
					Optional: false,
				},
				{
					Name:     metric.NullCount,
					FieldID:  fieldRootA.ID(),
					TableID:  spec.TableID(),
					Owner:    metric.Field,
					Optional: false,
				},
				{
					Name:     metric.Count,
					FieldID:  fieldChildA.ID(),
					TableID:  spec.TableID(),
					Owner:    metric.Field,
					Optional: false,
				},
				{
					Name:     metric.NullCount,
					FieldID:  fieldChildA.ID(),
					TableID:  spec.TableID(),
					Owner:    metric.Field,
					Optional: false,
				},
				{
					Name:     metric.Count,
					FieldID:  fieldGrandChildA.ID(),
					TableID:  spec.TableID(),
					Owner:    metric.Field,
					Optional: false,
				},
				{
					Name:     metric.NullCount,
					FieldID:  fieldGrandChildA.ID(),
					TableID:  spec.TableID(),
					Owner:    metric.Field,
					Optional: false,
				},
			}

			metadataStore := mock.NewMetadataStore()
			defer metadataStore.AssertExpectations(t)

			queryExecutor := mock.NewQueryExecutor()
			defer queryExecutor.AssertExpectations(t)

			rowsGroup1 := []protocol.Row{
				{
					"count_field_root_b_0":     int64(recordCountFieldRootB),
					"nullcount_field_root_b_1": int64(recordNullCountFieldRootB),
					"count_field_root_a_2":     int64(recordCountFieldRootA),
					"nullcount_field_root_a_3": int64(recordNullCountFieldRootA),
					common.GroupAlias:          "ID",
				},
			}

			rowsGroup2 := []protocol.Row{
				{
					"count_field_root_a_child_a_0":     int64(recordCountFieldChildA),
					"nullcount_field_root_a_child_a_1": int64(recordNullCountFieldChildA),
					common.GroupAlias:                  "ID",
				},
			}
			rowsGroup3 := []protocol.Row{
				{
					"count_field_root_a_grandchild_a_0":     int64(recordCountFieldGrandChildA),
					"nullcount_field_root_a_grandchild_a_1": int64(recordNullCountFieldGrandChildA),
					common.GroupAlias:                       "ID",
				},
			}

			expected := []*metric.Metric{
				{
					FieldID:    fieldRootB.ID(),
					Type:       metric.Count,
					Category:   metric.Basic,
					Owner:      metric.Field,
					Value:      float64(recordCountFieldRootB),
					GroupValue: "ID",
				},
				{
					FieldID:    fieldRootB.ID(),
					Type:       metric.NullCount,
					Category:   metric.Basic,
					Owner:      metric.Field,
					Value:      float64(recordNullCountFieldRootB),
					GroupValue: "ID",
				},
				{
					FieldID:    fieldRootA.ID(),
					Type:       metric.Count,
					Category:   metric.Basic,
					Owner:      metric.Field,
					Value:      float64(recordCountFieldRootA),
					GroupValue: "ID",
				},
				{
					FieldID:    fieldRootA.ID(),
					Type:       metric.NullCount,
					Category:   metric.Basic,
					Owner:      metric.Field,
					Value:      float64(recordNullCountFieldRootA),
					GroupValue: "ID",
				},
				{
					FieldID:    fieldChildA.ID(),
					Type:       metric.Count,
					Category:   metric.Basic,
					Owner:      metric.Field,
					Value:      float64(recordCountFieldChildA),
					GroupValue: "ID",
				},
				{
					FieldID:    fieldChildA.ID(),
					Type:       metric.NullCount,
					Category:   metric.Basic,
					Owner:      metric.Field,
					Value:      float64(recordNullCountFieldChildA),
					GroupValue: "ID",
				},
				{
					FieldID:    fieldGrandChildA.ID(),
					Type:       metric.Count,
					Category:   metric.Basic,
					Owner:      metric.Field,
					Value:      float64(recordCountFieldGrandChildA),
					GroupValue: "ID",
				},
				{
					FieldID:    fieldGrandChildA.ID(),
					Type:       metric.NullCount,
					Category:   metric.Basic,
					Owner:      metric.Field,
					Value:      float64(recordNullCountFieldGrandChildA),
					GroupValue: "ID",
				},
			}

			queryExecutor.On("Run", testifyMock.Anything, testifyMock.AnythingOfType("string"), job.FieldLevelQuery).Return(rowsGroup1, nil).Once()
			queryExecutor.On("Run", testifyMock.Anything, testifyMock.AnythingOfType("string"), job.FieldLevelQuery).Return(rowsGroup2, nil).Once()
			queryExecutor.On("Run", testifyMock.Anything, testifyMock.AnythingOfType("string"), job.FieldLevelQuery).Return(rowsGroup3, nil).Once()

			metadataStore.On("GetMetadata", profile.URN).Return(spec, nil)

			profiler := New(queryExecutor, metadataStore)

			result, err := profiler.Profile(entry, profile, metricSpecs)

			assert.Equal(t, expected, result)
			assert.Nil(t, err)
		})
		t.Run("should return error if query execution fails", func(t *testing.T) {
			profile := &job.Profile{
				Filter:    "active = true",
				GroupName: "`field_grouping`",
				URN:       "sample-project.sample_dataset.sample_table",
			}

			spec := &meta.TableSpec{
				Fields: []*meta.FieldSpec{fieldRootB},
			}

			metricSpecs := []*metric.Spec{
				{
					Name:     metric.Count,
					FieldID:  fieldRootB.ID(),
					TableID:  spec.TableID(),
					Owner:    metric.Field,
					Optional: false,
				},
			}

			queryError := errors.New("query execution error")

			var rows []protocol.Row

			metadataStore := mock.NewMetadataStore()
			defer metadataStore.AssertExpectations(t)

			queryExecutor := mock.NewQueryExecutor()
			defer queryExecutor.AssertExpectations(t)

			metadataStore.On("GetMetadata", profile.URN).Return(spec, nil)
			queryExecutor.On("Run", testifyMock.Anything, testifyMock.AnythingOfType("string"), job.FieldLevelQuery).Return(rows, queryError)

			profiler := New(queryExecutor, metadataStore)
			metrics, err := profiler.Profile(entry, profile, metricSpecs)

			assert.Nil(t, metrics)
			assert.Equal(t, queryError, err)
		})
	})
}
