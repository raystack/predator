package auditor

import (
	"github.com/odpf/predator/protocol/metric"
	"testing"
	"time"

	"github.com/odpf/predator/protocol"
	"github.com/stretchr/testify/assert"
)

func TestMetricComparator(t *testing.T) {
	t.Run("validate", func(t *testing.T) {
		tableID := "project.dataset.table"
		condition := "field1 > 0"
		metricDuplicationPct := &metric.Metric{
			Type:      metric.DuplicationPct,
			Category:  metric.Quality,
			Owner:     metric.Table,
			Value:     10.0,
			Timestamp: time.Time{},
		}
		metricRowCount := &metric.Metric{
			Type:      metric.RowCount,
			Category:  metric.Quality,
			Owner:     metric.Table,
			Value:     100.0,
			Timestamp: time.Time{},
		}
		metricField1NullnessPct := &metric.Metric{
			Type:      metric.NullnessPct,
			Category:  metric.Quality,
			Owner:     metric.Field,
			FieldID:   "field1",
			Value:     10.0,
			Timestamp: time.Time{},
		}
		metricField1InvalidPct := &metric.Metric{
			Type:      metric.InvalidPct,
			Category:  metric.Quality,
			Owner:     metric.Field,
			FieldID:   "field1",
			Value:     10.0,
			Condition: condition,
			Timestamp: time.Time{},
		}
		toleranceDuplicationPct := &protocol.Tolerance{
			TableURN:   tableID,
			MetricName: metric.DuplicationPct,
			ToleranceRules: []protocol.ToleranceRule{
				{
					Comparator: protocol.ComparatorLessThanEq,
					Value:      0.0,
				},
			},
		}
		toleranceRowCount := &protocol.Tolerance{
			TableURN:   tableID,
			MetricName: metric.RowCount,
			ToleranceRules: []protocol.ToleranceRule{
				{
					Comparator: protocol.ComparatorMoreThanEq,
					Value:      1.0,
				},
			},
		}
		toleranceField1NullnessPct := &protocol.Tolerance{
			FieldID:    "field1",
			TableURN:   tableID,
			MetricName: metric.NullnessPct,
			ToleranceRules: []protocol.ToleranceRule{
				{
					Comparator: protocol.ComparatorLessThanEq,
					Value:      0.0,
				},
			},
		}
		toleranceField1InvalidPct := &protocol.Tolerance{
			FieldID:    "field1",
			TableURN:   tableID,
			MetricName: metric.InvalidPct,
			Condition:  condition,
			ToleranceRules: []protocol.ToleranceRule{
				{
					Comparator: protocol.ComparatorLessThanEq,
					Value:      0.0,
				},
			},
		}

		tests := []struct {
			description  string
			qualityScore []*metric.Metric
			tolerance    []*protocol.Tolerance
			expected     []*protocol.ValidatedMetric
		}{
			{
				description: "should validate metrics against tolerance spec",
				qualityScore: []*metric.Metric{
					metricDuplicationPct,
					metricRowCount,
					metricField1NullnessPct,
					metricField1InvalidPct,
				},
				tolerance: []*protocol.Tolerance{
					toleranceDuplicationPct,
					toleranceRowCount,
					toleranceField1NullnessPct,
					toleranceField1InvalidPct,
				},
				expected: []*protocol.ValidatedMetric{
					{
						Metric:         metricDuplicationPct,
						ToleranceRules: toleranceDuplicationPct.ToleranceRules,
						PassFlag:       false,
					},
					{
						Metric:         metricRowCount,
						ToleranceRules: toleranceRowCount.ToleranceRules,
						PassFlag:       true,
					},
					{
						Metric:         metricField1NullnessPct,
						ToleranceRules: toleranceField1NullnessPct.ToleranceRules,
						PassFlag:       false,
					},
					{
						Metric:         metricField1InvalidPct,
						ToleranceRules: toleranceField1InvalidPct.ToleranceRules,
						PassFlag:       false,
					},
				},
			},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				result, err := validate(test.qualityScore, test.tolerance)
				assert.Equal(t, test.expected, result)
				assert.Nil(t, err)
			})
		}

		t.Run("should return error  when a quality score is not found", func(t *testing.T) {
			tableID := "sample-project.sample_dataset.sample_table"
			qualityScore := []*metric.Metric{
				{
					Type:      metric.DuplicationPct,
					Category:  metric.Quality,
					Owner:     metric.Table,
					Value:     10.0,
					Timestamp: time.Time{},
				},
			}

			tolerance := []*protocol.Tolerance{
				{
					TableURN:   tableID,
					MetricName: "row_count",
					ToleranceRules: []protocol.ToleranceRule{
						{
							Comparator: protocol.ComparatorMoreThanEq,
							Value:      1.0,
						},
						{
							Comparator: protocol.ComparatorLessThanEq,
							Value:      10.0,
						},
					},
				},
			}

			result, err := validate(qualityScore, tolerance)
			var expected []*protocol.ValidatedMetric
			assert.Equal(t, expected, result)
			assert.NotNil(t, err)
		})
	})
	t.Run("compare", func(t *testing.T) {
		tests := []struct {
			description string
			rule        protocol.Comparator
			ruleValue   float64
			scoreValue  float64
			expected    bool
		}{
			{
				description: "should pass if more than",
				rule:        protocol.ComparatorMoreThan,
				ruleValue:   20.0,
				scoreValue:  21.0,
				expected:    true,
			},
			{
				description: "should pass if more than or equal",
				rule:        protocol.ComparatorMoreThanEq,
				ruleValue:   20.0,
				scoreValue:  20.0,
				expected:    true,
			},
			{
				description: "should pass if less than",
				rule:        protocol.ComparatorLessThan,
				ruleValue:   20.0,
				scoreValue:  19.0,
				expected:    true,
			},
			{
				description: "should pass if less than or equal",
				rule:        protocol.ComparatorLessThanEq,
				ruleValue:   20.0,
				scoreValue:  20.0,
				expected:    true,
			},
			{
				description: "should not pass if more than",
				rule:        protocol.ComparatorMoreThan,
				ruleValue:   20.0,
				scoreValue:  20.0,
				expected:    false,
			},
			{
				description: "should not pass if more than or equal",
				rule:        protocol.ComparatorMoreThanEq,
				ruleValue:   20.0,
				scoreValue:  19.0,
				expected:    false,
			},
			{
				description: "should not pass if less than",
				rule:        protocol.ComparatorLessThan,
				ruleValue:   20.0,
				scoreValue:  20.0,
				expected:    false,
			},
			{
				description: "should not pass if less than or equal",
				rule:        protocol.ComparatorLessThanEq,
				ruleValue:   20.0,
				scoreValue:  21.0,
				expected:    false,
			},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				result := compare(test.rule, test.scoreValue, test.ruleValue)
				assert.Equal(t, test.expected, result)
			})
		}
	})
	t.Run("check", func(t *testing.T) {
		tests := []struct {
			description    string
			score          *metric.Metric
			toleranceRules []protocol.ToleranceRule
			expected       bool
		}{
			{
				description: "should check metric using one rule",
				score: &metric.Metric{
					Type:      metric.DuplicationPct,
					Category:  metric.Quality,
					Owner:     metric.Table,
					Value:     10.0,
					Timestamp: time.Time{},
				},
				toleranceRules: []protocol.ToleranceRule{
					{
						Comparator: protocol.ComparatorLessThanEq,
						Value:      20.0,
					},
				},
				expected: true,
			},
			{
				description: "should check metric using multiple rule",
				score: &metric.Metric{
					Type:      metric.DuplicationPct,
					Category:  metric.Quality,
					Owner:     metric.Table,
					Value:     10.0,
					Timestamp: time.Time{},
				},
				toleranceRules: []protocol.ToleranceRule{
					{
						Comparator: protocol.ComparatorLessThanEq,
						Value:      20.0,
					},
					{
						Comparator: protocol.ComparatorMoreThanEq,
						Value:      10.0,
					},
				},
				expected: true,
			},
			{
				description: "should fail when using multiple rule",
				score: &metric.Metric{
					Type:      metric.RowCount,
					Category:  metric.Quality,
					Owner:     metric.Table,
					Value:     1000.0,
					Timestamp: time.Time{},
				},
				toleranceRules: []protocol.ToleranceRule{
					{
						Comparator: protocol.ComparatorMoreThanEq,
						Value:      100.0,
					},
					{
						Comparator: protocol.ComparatorLessThan,
						Value:      1000.0,
					},
				},
				expected: false,
			},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				result := check(test.score, test.toleranceRules)
				assert.Equal(t, test.expected, result)
			})
		}
	})
}
