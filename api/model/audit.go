package model

import (
	"time"

	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
)

//AuditResult is result of audit
type AuditResult struct {
	FieldID        string                   `json:"field_id"`
	MetricName     string                   `json:"metric_name"`
	MetricValue    float64                  `json:"metric_value"`
	Condition      string                   `json:"condition"`
	Metadata       map[string]interface{}   `json:"metadata"`
	ToleranceRules []protocol.ToleranceRule `json:"tolerance_rule"`
	Pass           bool                     `json:"pass"`
}

//AuditResultGroup is result of audit per group
type AuditResultGroup struct {
	GroupValue   string        `json:"group_value"`
	AuditResults []AuditResult `json:"audit_results"`
	Pass         bool          `json:"pass"`
}

//AuditResponse is response of audit
type AuditResponse struct {
	AuditID      string             `json:"audit_id"`
	ProfileID    string             `json:"profile_id"`
	URN          string             `json:"urn"`
	GroupName    string             `json:"group_name"`
	Filter       string             `json:"filter"`
	Mode         job.Mode           `json:"mode"`
	Status       string             `json:"status"`
	Pass         bool               `json:"pass"`
	Message      string             `json:"message"`
	TotalRecords int64              `json:"total_records"`
	Result       []AuditResultGroup `json:"result"`
	CreatedAt    time.Time          `json:"created_at"`
}
