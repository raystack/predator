package audit

import (
	"errors"
	"fmt"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/odpf/predator/protocol/metric"
	"github.com/odpf/predator/util"
	"strings"
)

//DefaultAuditSummaryFactory to create summary log message of audit
type DefaultAuditSummaryFactory struct {
	toleranceStore protocol.ToleranceStore
}

//NewAuditSummaryFactory is constructor
func NewAuditSummaryFactory(toleranceStore protocol.ToleranceStore) *DefaultAuditSummaryFactory {
	return &DefaultAuditSummaryFactory{
		toleranceStore: toleranceStore,
	}
}

//Create create summary log message of audit
func (a *DefaultAuditSummaryFactory) Create(auditResults []*protocol.AuditReport, auditJob *job.Audit) (*protocol.AuditSummary, error) {
	if auditJob.TotalRecords > 0 {
		if len(auditResults) > 0 {
			summary := formSummary(auditResults)
			return summary, nil
		} else {
			return &protocol.AuditSummary{
				IsPass:  false,
				Message: "EXPECT SOME AUDIT RESULT BUT NO AUDIT RESULT FOUND",
			}, nil
		}
	}
	return a.formNoRecordsSummary(auditJob)
}

func formSummary(auditResults []*protocol.AuditReport) *protocol.AuditSummary {
	passFlag := isAllResultPass(auditResults)
	message := formMessage(auditResults)
	summary := &protocol.AuditSummary{
		IsPass:  passFlag,
		Message: message,
	}
	return summary
}

func isAllResultPass(auditResults []*protocol.AuditReport) bool {
	var passFlag = true
	for _, auditResult := range auditResults {
		passFlag = passFlag && auditResult.PassFlag
	}
	return passFlag
}

func (a *DefaultAuditSummaryFactory) formNoRecordsSummary(auditJob *job.Audit) (*protocol.AuditSummary, error) {
	hasAvailabilityRules, err := a.hasAvailabilityRule(auditJob.URN)
	if err != nil {
		return nil, errors.New("unable to check availability rules")
	}

	if !hasAvailabilityRules {
		msg := "NO RECORDS PROFILED AND AUDITED"
		return &protocol.AuditSummary{
			IsPass:  true,
			Message: msg,
		}, nil
	}

	return &protocol.AuditSummary{
		IsPass:  false,
		Message: "EXPECT SOME RECORDS BUT NO RECORDS FOUND",
	}, nil
}

func (a *DefaultAuditSummaryFactory) hasAvailabilityRule(urn string) (bool, error) {
	availabilityRules := false

	toleranceSpec, err := a.toleranceStore.GetByTableID(urn)
	if err != nil {
		return false, err
	}

	for _, tolerance := range toleranceSpec.Tolerances {
		if tolerance.MetricName == metric.RowCount {
			for _, toleranceRule := range tolerance.ToleranceRules {
				if toleranceRule.Comparator == protocol.ComparatorMoreThan && toleranceRule.Value >= 0 {
					availabilityRules = true
					break
				}
				if toleranceRule.Comparator == protocol.ComparatorMoreThanEq && toleranceRule.Value >= 1 {
					availabilityRules = true
					break
				}
			}
		}
	}
	return availabilityRules, nil
}

func formFieldInfo(fieldID string) string {
	var fieldInfo string
	if fieldID != "" {
		fieldInfo = fmt.Sprintf("OF %s ", strings.ToUpper(fieldID))
	}
	return fieldInfo
}

func formGroupInfo(group string) string {
	var groupInfo string
	if group != "" {
		groupInfo = fmt.Sprintf("IN GROUP %s", group)
	}
	return groupInfo
}

func formToleranceInfo(toleranceRules []protocol.ToleranceRule) string {
	var toleranceRulesInfo []string
	for _, toleranceRule := range toleranceRules {
		toleranceRuleInfo := fmt.Sprintf("%s %.2f", strings.ToUpper(string(toleranceRule.Comparator)), toleranceRule.Value)
		toleranceRulesInfo = append(toleranceRulesInfo, toleranceRuleInfo)
	}
	return strings.Join(toleranceRulesInfo, ", ")
}

func formIssueMessage(element *protocol.AuditReport) string {
	fieldInfo := formFieldInfo(element.FieldID)
	groupInfo := formGroupInfo(element.GroupValue)
	toleranceInfo := formToleranceInfo(element.ToleranceRules)
	metricValue := util.RoundMetricValue(element.MetricValue)
	conditionInfo := formConditionInfo(element.MetricName, element.Condition)

	return fmt.Sprintf("%s %sIS NOT PASSED THE TOLERANCE %s%s\nTolerance: %s\nACTUAL VALUE: %s", strings.ToUpper(element.MetricName.String()), fieldInfo, groupInfo, conditionInfo, toleranceInfo, metricValue)
}

func formConditionInfo(metricName metric.Type, condition string) string {
	var conditionInfo string
	if metricName == metric.InvalidPct {
		conditionInfo = fmt.Sprintf("\nCONDITION: %s", strings.ToUpper(condition))
	}
	return conditionInfo
}

func formMessage(auditResults []*protocol.AuditReport) string {
	message := FormIssueSummary(auditResults)
	if len(message) == 0 {
		message = "ALL METRICS PASSED THE TOLERANCE"
	}
	return message
}

func FormIssueSummary(auditResults []*protocol.AuditReport) string {
	var issueMessages []string
	for _, auditResult := range auditResults {
		if !auditResult.PassFlag {
			issueMessages = append(issueMessages, formIssueMessage(auditResult))
		}
	}

	return strings.Join(issueMessages, "\n\n")
}
