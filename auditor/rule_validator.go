package auditor

import (
	"fmt"

	"github.com/odpf/predator/protocol/metric"

	"github.com/odpf/predator/protocol"
)

//DefaultRuleValidator as a default rule validator
type DefaultRuleValidator struct {
}

//NewDefaultRuleValidator construct DefaultRuleValidator
func NewDefaultRuleValidator() *DefaultRuleValidator {
	return &DefaultRuleValidator{}
}

//Validate to validate metrics based on tolerances
func (d DefaultRuleValidator) Validate(metrics []*metric.Metric, tolerances []*protocol.Tolerance) ([]*protocol.ValidatedMetric, error) {
	return validate(metrics, tolerances)
}

func validate(metrics []*metric.Metric, tolerances []*protocol.Tolerance) ([]*protocol.ValidatedMetric, error) {
	var result []*protocol.ValidatedMetric
	for _, t := range tolerances {
		scores := metric.NewFinder(metrics).
			WithType(t.MetricName).
			WithFieldID(t.FieldID).
			WithCondition(t.Condition).
			WithCategory(metric.Quality).
			Find()

		if scores == nil {
			return nil, fmt.Errorf("failed to find quality score %s ,for field %s, with name %s", t.TableURN, t.FieldID, t.MetricName)
		}

		for _, score := range scores {
			pass := check(score, t.ToleranceRules)
			report := &protocol.ValidatedMetric{
				Metric:         score,
				ToleranceRules: t.ToleranceRules,
				PassFlag:       pass,
			}
			result = append(result, report)
		}
	}

	return result, nil
}

func check(score *metric.Metric, toleranceRules []protocol.ToleranceRule) bool {
	var countPass int
	for _, rule := range toleranceRules {
		scoreValue := score.Value
		ruleValue := rule.Value

		res := compare(rule.Comparator, scoreValue, ruleValue)
		if res {
			countPass++
		}
	}

	var pass bool

	if countPass == len(toleranceRules) {
		pass = true
	}
	return pass
}

func compare(logic protocol.Comparator, scoreValue float64, ruleValue float64) bool {
	var res bool

	switch logic {
	case protocol.ComparatorLessThan:
		if scoreValue < ruleValue {
			res = true
		}
	case protocol.ComparatorLessThanEq:
		if scoreValue <= ruleValue {
			res = true
		}
	case protocol.ComparatorMoreThan:
		if scoreValue > ruleValue {
			res = true
		}
	case protocol.ComparatorMoreThanEq:
		if scoreValue >= ruleValue {
			res = true
		}
	}
	return res
}
