package protocol

import (
	"context"
	"fmt"
	"github.com/odpf/predator/util"
	"strings"
	"time"

	"github.com/odpf/predator/protocol/job"
	"github.com/odpf/predator/protocol/metric"
)

//AuditReport is the result of audit
type AuditReport struct {
	AuditID        string
	Partition      string
	GroupValue     string
	TableURN       string
	FieldID        string
	MetricName     metric.Type
	MetricValue    float64
	Condition      string
	Metadata       map[string]interface{}
	ToleranceRules []ToleranceRule
	PassFlag       bool
	EventTimestamp time.Time
}

//AuditGroup is a type to do group by operation on AuditReport
type AuditGroup []*AuditReport

//ByPartitionDate group by partition date
func (ag AuditGroup) ByPartitionDate() map[string][]*AuditReport {
	r := make(map[string][]*AuditReport)
	for _, a := range []*AuditReport(ag) {
		r[a.Partition] = append(r[a.Partition], a)
	}
	return r
}

//ByGroupValue group by group value
func (ag AuditGroup) ByGroupValue() map[string][]*AuditReport {
	r := make(map[string][]*AuditReport)
	for _, a := range []*AuditReport(ag) {
		r[a.GroupValue] = append(r[a.GroupValue], a)
	}
	return r
}

//ByFieldID group by field ID
func (ag AuditGroup) ByFieldID() map[string][]*AuditReport {
	auditResults := make(map[string][]*AuditReport)
	for _, result := range []*AuditReport(ag) {
		auditResults[result.FieldID] = append(auditResults[result.FieldID], result)
	}
	return auditResults
}

func formFieldInfo(fieldID string) string {
	var fieldInfo string
	if fieldID != "" {
		fieldInfo = fmt.Sprintf("OF %s ", strings.ToUpper(fieldID))
	}
	return fieldInfo
}

func formPartitionInfo(partition string) string {
	var partitionInfo string
	if partition != "" {
		partitionInfo = fmt.Sprintf("IN PARTITION %s", partition)
	}
	return partitionInfo
}

func formToleranceInfo(toleranceRules []ToleranceRule) string {
	var toleranceRulesInfo []string
	for _, toleranceRule := range toleranceRules {
		toleranceRuleInfo := fmt.Sprintf("%s %.2f", strings.ToUpper(string(toleranceRule.Comparator)), toleranceRule.Value)
		toleranceRulesInfo = append(toleranceRulesInfo, toleranceRuleInfo)
	}
	return strings.Join(toleranceRulesInfo, ", ")
}

func formIssueMessage(element *AuditReport) string {
	fieldInfo := formFieldInfo(element.FieldID)
	partitionInfo := formPartitionInfo(element.Partition)
	toleranceInfo := formToleranceInfo(element.ToleranceRules)
	metricValue := util.RoundMetricValue(element.MetricValue)
	conditionInfo := formConditionInfo(element.MetricName, element.Condition)

	return fmt.Sprintf("%s %sIS NOT PASSED THE TOLERANCE %s%s\nTolerance: %s\nACTUAL VALUE: %s", strings.ToUpper(element.MetricName.String()), fieldInfo, partitionInfo, conditionInfo, toleranceInfo, metricValue)
}

func formConditionInfo(metricName metric.Type, condition string) string {
	var conditionInfo string
	if metricName == metric.InvalidPct {
		conditionInfo = fmt.Sprintf("\nCONDITION: %s", strings.ToUpper(condition))
	}
	return conditionInfo
}

//FormIssueSummary create issue summary
func FormIssueSummary(auditResults []*AuditReport) string {
	var issueMessages []string
	for _, auditResult := range auditResults {
		if !auditResult.PassFlag {
			issueMessages = append(issueMessages, formIssueMessage(auditResult))
		}
	}

	return strings.Join(issueMessages, "\n\n")
}

//ValidatedMetric is metric audited
type ValidatedMetric struct {
	Metric         *metric.Metric
	ToleranceRules []ToleranceRule
	PassFlag       bool
}

//AuditService is service of auditor
type AuditService interface {
	//RunAudit start audit service
	RunAudit(profileID string) (*AuditResult, error)
}

//Auditor to compare quality result with tolerances
type Auditor interface {
	Audit(audit *job.Audit) ([]*AuditReport, error)
}

//AuditResult is audit job and the report detail
type AuditResult struct {
	Audit        *job.Audit
	AuditReports []*AuditReport
}

//AuditSummary is summary of audit
type AuditSummary struct {
	IsPass  bool
	Message string
}

//AuditStore is store for audit entity
type AuditStore interface {
	CreateAudit(audit *job.Audit) (*job.Audit, error)
	UpdateAudit(audit *job.Audit) error
}

//AuditResultStore to store the auditing result
type AuditResultStore interface {
	StoreResults(results []*AuditReport) error
}

//AuditPublisher for publisher for audit
type AuditPublisher interface {
	PublishAuditResult(audit *job.Audit, auditResult []*AuditReport) error
	Close(ctx context.Context) error
}

type AuditSummaryFactory interface {
	Create(auditResults []*AuditReport, auditJob *job.Audit) (*AuditSummary, error)
}
