package auditor

import (
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/odpf/predator/protocol/metric"
)

var logger = log.New(os.Stdout, "INFO: ", log.Lshortfile|log.LstdFlags)

//RuleValidator to validate metric value with rule
type RuleValidator interface {
	Validate(metrics []*metric.Metric, tolerances []*protocol.Tolerance) ([]*protocol.ValidatedMetric, error)
}

//Auditor as a structure of auditor
type Auditor struct {
	ruleValidator  RuleValidator
	toleranceStore protocol.ToleranceStore
	metadataStore  protocol.MetadataStore
	metricStore    protocol.MetricStore
}

//New create Auditor
func New(toleranceStore protocol.ToleranceStore,
	validator RuleValidator,
	metadataStore protocol.MetadataStore,
	metricStore protocol.MetricStore) *Auditor {
	return &Auditor{
		ruleValidator:  validator,
		toleranceStore: toleranceStore,
		metadataStore:  metadataStore,
		metricStore:    metricStore,
	}
}

//Audit audit entry point
func (a *Auditor) Audit(audit *job.Audit) ([]*protocol.AuditReport, error) {
	if audit.TotalRecords == 0 {
		return nil, nil
	}

	tolerance, err := a.toleranceStore.GetByTableID(audit.URN)
	if err != nil {
		e := fmt.Errorf("failed to try to get tolerances for table %s ,%w", audit.URN, err)
		logger.Println(e)
		return nil, e
	}

	auditResults, err := a.auditing(audit, tolerance.Tolerances)
	if err != nil {
		return nil, err
	}

	return auditResults, nil
}

func (a *Auditor) auditing(audit *job.Audit, tolerance []*protocol.Tolerance) ([]*protocol.AuditReport, error) {
	metrics, err := a.metricStore.GetMetricsByProfileID(audit.ProfileID)
	if err != nil {
		e := fmt.Errorf("failed to get metrics for table %s,%w", audit.URN, err)
		logger.Println(e)
		return nil, e
	}

	validatedMetrics, err := a.ruleValidator.Validate(metrics, tolerance)
	if err != nil {
		e := fmt.Errorf("failed to check score against tolerance rules for table %s,%w", audit.URN, err)
		logger.Println(e)
		return nil, e
	}

	auditReports := generateAuditReports(audit, validatedMetrics)
	if audit.TotalRecords > 0 && len(auditReports) == 0 {
		return nil, errors.New("failed to audit result")
	}

	return auditReports, err
}

func generateAuditReports(audit *job.Audit, validatedMetrics []*protocol.ValidatedMetric) []*protocol.AuditReport {
	var auditReports []*protocol.AuditReport
	for _, validatedMetric := range validatedMetrics {
		auditReport := &protocol.AuditReport{
			AuditID:        audit.ID,
			GroupValue:     validatedMetric.Metric.GroupValue,
			TableURN:       audit.URN,
			FieldID:        validatedMetric.Metric.FieldID,
			MetricName:     validatedMetric.Metric.Type,
			MetricValue:    validatedMetric.Metric.Value,
			Condition:      validatedMetric.Metric.Condition,
			Metadata:       validatedMetric.Metric.Metadata,
			ToleranceRules: validatedMetric.ToleranceRules,
			PassFlag:       validatedMetric.PassFlag,
			EventTimestamp: audit.EventTimestamp,
		}
		auditReports = append(auditReports, auditReport)
	}
	return auditReports
}
