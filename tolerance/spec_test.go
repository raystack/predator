package tolerance

import (
	"errors"
	"github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/meta"
	"github.com/odpf/predator/protocol/metric"
	"github.com/stretchr/testify/assert"
	"testing"
)

var flatSpecYamlFileContent = `- tableid: "project.dataset.table"
  fieldid: ""
  metricname: "duplication_pct"
  tolerancerules:
    less_than_eq: 0.0
- tableid: "project.dataset.table"
  fieldid: "field1"
  metricname: "nullness_pct"
  tolerancerules:
    less_than_eq: 10.0`

var compactSpecYamlFileContent = `tableid: "project.dataset.table"
tablemetrics:
- metricname: "duplication_pct"
  tolerance:
    less_than_eq: 0.0
  metadata:
    uniquefields:
    - unique_field
- metricname: "invalid_pct"
  condition: "field1 - field2 - field3 != 0"
  tolerance:
    less_than_eq: 0.0

fields:
- fieldid: "field3"
  fieldmetrics:
  - metricname: "nullness_pct"
    tolerance:
      less_than_eq: 10.0
      more_than: 5.0

- fieldid: "field4"
  fieldmetrics:
  - metricname: "invalid_pct"
    condition: "field4 <= 0"
    tolerance:
      less_than_eq: 0.0`

func TestParser(t *testing.T) {
	t.Run("FlatSpecParser", func(t *testing.T) {
		t.Run("Parse", func(t *testing.T) {
			t.Run("should return tolerances", func(t *testing.T) {
				tableID := "project.dataset.table"

				expected := &protocol.ToleranceSpec{
					URN: tableID,
					Tolerances: []*protocol.Tolerance{
						{
							TableURN:   tableID,
							MetricName: metric.DuplicationPct,
							ToleranceRules: []protocol.ToleranceRule{
								{
									Comparator: protocol.ComparatorLessThanEq,
									Value:      0.0,
								},
							},
						},
						{
							TableURN:   tableID,
							FieldID:    "field1",
							MetricName: metric.NullnessPct,
							ToleranceRules: []protocol.ToleranceRule{
								{
									Comparator: protocol.ComparatorLessThanEq,
									Value:      10.0,
								},
							},
						},
					},
				}

				parser := &FlatSpecParser{}
				result, _ := parser.Parse([]byte(flatSpecYamlFileContent))

				assert.Equal(t, expected, result)
			})
			t.Run("should error when Parse failed", func(t *testing.T) {
				parser := &FlatSpecParser{}
				_, err := parser.Parse([]byte("abcdef/)"))
				assert.NotNil(t, err)
			})
		})
	})
	t.Run("CompactSpecParser", func(t *testing.T) {
		t.Run("Parse", func(t *testing.T) {
			t.Run("should return tolerances", func(t *testing.T) {
				tableID := "project.dataset.table"
				uniqueFields := []string{"unique_field"}

				expected := &protocol.ToleranceSpec{
					URN: tableID,
					Tolerances: []*protocol.Tolerance{
						{
							TableURN:   tableID,
							MetricName: metric.DuplicationPct,
							ToleranceRules: []protocol.ToleranceRule{
								{
									Comparator: protocol.ComparatorLessThanEq,
									Value:      0.0,
								},
							},
							Metadata: map[string]interface{}{
								metric.UniqueFields: uniqueFields,
							},
						},
						{
							TableURN:   tableID,
							MetricName: metric.InvalidPct,
							Condition:  "field1 - field2 - field3 != 0",
							ToleranceRules: []protocol.ToleranceRule{
								{
									Comparator: protocol.ComparatorLessThanEq,
									Value:      0.0,
								},
							},
						},
						{
							TableURN:   tableID,
							FieldID:    "field3",
							MetricName: metric.NullnessPct,
							ToleranceRules: []protocol.ToleranceRule{
								{
									Comparator: protocol.ComparatorLessThanEq,
									Value:      10.0,
								},
								{
									Comparator: protocol.ComparatorMoreThan,
									Value:      5.0,
								},
							},
						},
						{
							TableURN:   tableID,
							FieldID:    "field4",
							MetricName: metric.InvalidPct,
							Condition:  "field4 <= 0",
							ToleranceRules: []protocol.ToleranceRule{
								{
									Comparator: protocol.ComparatorLessThanEq,
									Value:      0.0,
								},
							},
						},
					},
				}

				parser := &CompactSpecParser{}
				result, _ := parser.Parse([]byte(compactSpecYamlFileContent))

				assert.Equal(t, expected, result)
			})
			t.Run("should error when Parse failed", func(t *testing.T) {
				parser := &CompactSpecParser{}
				_, err := parser.Parse([]byte(content))
				assert.NotNil(t, err)
			})
		})
		t.Run("Serialize", func(t *testing.T) {
			t.Run("should return yaml", func(t *testing.T) {
				tableID := "project.dataset.table"
				uniqueFields := []string{"unique_field"}
				tolerances := []*protocol.Tolerance{
					{
						TableURN:   tableID,
						MetricName: metric.DuplicationPct,
						ToleranceRules: []protocol.ToleranceRule{
							{
								Comparator: protocol.ComparatorLessThanEq,
								Value:      0.0,
							},
						},
						Metadata: map[string]interface{}{
							metric.UniqueFields: uniqueFields,
						},
					},
					{
						TableURN:   tableID,
						MetricName: metric.InvalidPct,
						Condition:  "field1 - field2 - field3 != 0",
						ToleranceRules: []protocol.ToleranceRule{
							{
								Comparator: protocol.ComparatorLessThanEq,
								Value:      0.0,
							},
						},
					},
					{
						TableURN:   tableID,
						FieldID:    "field1",
						MetricName: metric.InvalidPct,
						Condition:  "field1 is null and status='SUCCESS'",
						ToleranceRules: []protocol.ToleranceRule{
							{
								Comparator: protocol.ComparatorLessThanEq,
								Value:      0.0,
							},
						},
					},
					{
						TableURN:   tableID,
						FieldID:    "field2",
						MetricName: metric.InvalidPct,
						Condition:  "field4 <= 0",
						ToleranceRules: []protocol.ToleranceRule{
							{
								Comparator: protocol.ComparatorLessThanEq,
								Value:      0.0,
							},
						},
					},
					{
						TableURN:   tableID,
						FieldID:    "field3",
						MetricName: metric.NullnessPct,
						ToleranceRules: []protocol.ToleranceRule{
							{
								Comparator: protocol.ComparatorLessThanEq,
								Value:      10.0,
							},
							{
								Comparator: protocol.ComparatorMoreThan,
								Value:      5.0,
							},
						},
					},
				}

				toleranceSpec := &protocol.ToleranceSpec{
					URN:        tableID,
					Tolerances: tolerances,
				}

				expected := `tableid: project.dataset.table
tablemetrics:
- metricname: duplication_pct
  condition: ""
  metadata:
    uniquefields:
    - unique_field
  tolerance:
    less_than_eq: 0
- metricname: invalid_pct
  condition: field1 - field2 - field3 != 0
  metadata: {}
  tolerance:
    less_than_eq: 0
fields:
- fieldid: field1
  fieldmetrics:
  - metricname: invalid_pct
    condition: field1 is null and status='SUCCESS'
    metadata: {}
    tolerance:
      less_than_eq: 0
- fieldid: field2
  fieldmetrics:
  - metricname: invalid_pct
    condition: field4 <= 0
    metadata: {}
    tolerance:
      less_than_eq: 0
- fieldid: field3
  fieldmetrics:
  - metricname: nullness_pct
    condition: ""
    metadata: {}
    tolerance:
      less_than_eq: 10
      more_than: 5
`

				parser := &CompactSpecParser{}
				result, err := parser.Serialise(toleranceSpec)

				assert.Nil(t, err)
				assert.Equal(t, expected, string(result))
			})
		})
	})
}

func TestSpecValidator(t *testing.T) {
	t.Run("should return nil, when spec is valid", func(t *testing.T) {
		urn := "project-1.dataset_a.table_x"

		tableSpec := &meta.TableSpec{
			TimePartitioningType: meta.DayPartitioning,
			PartitionField:       "field_date",
			Fields: []*meta.FieldSpec{
				{
					Name:      "field_date",
					FieldType: meta.FieldTypeDate,
				},
			},
		}

		toleranceSpec := &protocol.ToleranceSpec{
			URN: urn,
			Tolerances: []*protocol.Tolerance{
				{
					FieldID:    "field_date",
					MetricName: metric.NullnessPct,
				},
			},
		}

		metadataStore := mock.NewMetadataStore()
		defer metadataStore.AssertExpectations(t)

		metadataStore.On("GetMetadata", urn).Return(tableSpec, nil)

		specValidator := NewSpecValidator(metadataStore)
		err := specValidator.Validate(toleranceSpec)

		assert.Nil(t, err)
	})
	t.Run("should return error, when table not found", func(t *testing.T) {
		urn := "project-1.dataset_a.table_x"

		var tableSpec *meta.TableSpec

		toleranceSpec := &protocol.ToleranceSpec{
			URN: urn,
			Tolerances: []*protocol.Tolerance{
				{
					FieldID: "field_date",
				},
			},
		}

		metadataStore := mock.NewMetadataStore()
		defer metadataStore.AssertExpectations(t)

		metadataStore.On("GetMetadata", urn).Return(tableSpec, protocol.ErrTableMetadataNotFound)

		specValidator := NewSpecValidator(metadataStore)
		err := specValidator.Validate(toleranceSpec)

		assert.True(t, protocol.IsSpecInvalidError(err))
	})
	t.Run("should return spec invalid error and collect all error messages when field not found", func(t *testing.T) {
		urn := "project-1.dataset_a.table_x"

		tableSpec := &meta.TableSpec{}

		toleranceSpec := &protocol.ToleranceSpec{
			URN: urn,
			Tolerances: []*protocol.Tolerance{
				{
					FieldID:    "field_date",
					MetricName: metric.NullnessPct,
				},
				{
					FieldID:    "field_unknown",
					MetricName: metric.NullnessPct,
				},
			},
		}

		metadataStore := mock.NewMetadataStore()
		defer metadataStore.AssertExpectations(t)

		metadataStore.On("GetMetadata", urn).Return(tableSpec, nil)

		specValidator := NewSpecValidator(metadataStore)
		err := specValidator.Validate(toleranceSpec)

		assert.True(t, protocol.IsSpecInvalidError(err))

		specInvalidErr := err.(*protocol.ErrSpecInvalid)
		assert.Len(t, specInvalidErr.Errors, 2)
	})
	t.Run("should return spec invalid error unsupported metric is configured", func(t *testing.T) {
		urn := "project-1.dataset_a.table_x"

		tableSpec := &meta.TableSpec{
			TimePartitioningType: meta.DayPartitioning,
			PartitionField:       "field_date",
			Fields: []*meta.FieldSpec{
				{
					Name:      "field_date",
					FieldType: meta.FieldTypeDate,
				},
			},
		}

		toleranceSpec := &protocol.ToleranceSpec{
			URN: urn,
			Tolerances: []*protocol.Tolerance{
				{
					FieldID:    "field_date",
					MetricName: metric.TrendInconsistencyPct,
				},
				{
					FieldID:    "field_date",
					MetricName: metric.Type("new_metric"),
				},
			},
		}

		metadataStore := mock.NewMetadataStore()
		defer metadataStore.AssertExpectations(t)

		metadataStore.On("GetMetadata", urn).Return(tableSpec, nil)

		specValidator := NewSpecValidator(metadataStore)
		err := specValidator.Validate(toleranceSpec)

		assert.True(t, protocol.IsSpecInvalidError(err))

		specInvalidErr := err.(*protocol.ErrSpecInvalid)

		assert.Len(t, specInvalidErr.Errors, 2)
		assert.Equal(t, specInvalidErr.Errors[0].Error(), "metric : trend_inconsistency_pct is not supported")
		assert.Equal(t, specInvalidErr.Errors[1].Error(), "metric : new_metric is not supported")
	})
	t.Run("should return other error and return immediately when API call failed", func(t *testing.T) {
		urn := "project-1.dataset_a.table_x"

		var tableSpec *meta.TableSpec

		toleranceSpec := &protocol.ToleranceSpec{
			URN: urn,
			Tolerances: []*protocol.Tolerance{
				{
					FieldID: "field_date",
				},
			},
		}

		metadataStore := mock.NewMetadataStore()
		defer metadataStore.AssertExpectations(t)

		metadataStore.On("GetMetadata", urn).Return(tableSpec, errors.New("API error"))

		specValidator := NewSpecValidator(metadataStore)
		err := specValidator.Validate(toleranceSpec)

		assert.Error(t, err)
		assert.False(t, protocol.IsSpecInvalidError(err))
	})
}
