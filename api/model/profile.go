package model

import (
	"errors"
	"strings"
	"time"

	"github.com/odpf/predator/protocol/job"
)

//ProfileRequest request to start profile
type ProfileRequest struct {
	URN       string   `json:"urn"`
	Filter    string   `json:"filter"`
	Group     string   `json:"group"`
	Mode      job.Mode `json:"mode"`
	AuditTime string   `json:"audit_time"`
}

//Validate to check data payload
func (p *ProfileRequest) Validate() error {
	tableURN := strings.TrimSpace(p.URN)

	if tableURN == "" {
		return errors.New("URN is required")
	} else if len(strings.Split(tableURN, ".")) != 3 {
		return errors.New("wrong URN format")
	}

	if err := p.Mode.IsValid(); err != nil {
		return err
	}

	return nil
}

//ProfileResponse profile information and the state and metric produced
type ProfileResponse struct {
	ID           string         `json:"profile_id"`
	URN          string         `json:"urn"`
	Filter       string         `json:"filter"`
	Group        string         `json:"group"`
	Mode         job.Mode       `json:"mode"`
	AuditTime    time.Time      `json:"audit_time"`
	CreatedAt    time.Time      `json:"created_at"`
	UpdatedAt    time.Time      `json:"updated_at,omitempty"`
	State        job.State      `json:"state,omitempty"`
	Message      string         `json:"message,omitempty"`
	TotalRecords int64          `json:"total_records"`
	Metrics      []*MetricGroup `json:"metrics,omitempty"`
}

//Log represents log of profile and audit
type Log struct {
	Status         string    `json:"status"`
	Message        string    `json:"message"`
	EventTimestamp time.Time `json:"event_timestamp"`
}

//ProfileLogResponse represents logs of profile
type ProfileLogResponse struct {
	ID           string    `json:"profile_id"`
	URN          string    `json:"urn"`
	Filter       string    `json:"filter"`
	Group        string    `json:"group"`
	Mode         job.Mode  `json:"mode"`
	AuditTime    time.Time `json:"audit_time"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at,omitempty"`
	TotalRecords int64     `json:"total_records"`
	State        job.State `json:"state,omitempty"`
	Logs         []Log     `json:"logs"`
}
