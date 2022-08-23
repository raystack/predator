package audit

import (
	"fmt"
	"github.com/odpf/predator/stats"
	"time"

	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
)

//Service as audit service
type Service struct {
	profileStore           protocol.ProfileStore
	auditStore             protocol.AuditStore
	resultStore            protocol.AuditResultStore
	auditor                protocol.Auditor
	publisher              protocol.Publisher
	messageProviderFactory protocol.MessageProviderFactory
	metadataStore          protocol.MetadataStore
	statsClientBuilder     stats.ClientBuilder
}

//NewService is constructor
func NewService(profileStore protocol.ProfileStore,
	auditStore protocol.AuditStore,
	resultStore protocol.AuditResultStore,
	auditor protocol.Auditor,
	publisher protocol.Publisher,
	messageProviderFactory protocol.MessageProviderFactory,
	metadataStore protocol.MetadataStore,
	statsClientBuilder stats.ClientBuilder) *Service {
	return &Service{
		profileStore:           profileStore,
		auditStore:             auditStore,
		resultStore:            resultStore,
		auditor:                auditor,
		publisher:              publisher,
		messageProviderFactory: messageProviderFactory,
		metadataStore:          metadataStore,
		statsClientBuilder:     statsClientBuilder,
	}
}

//RunAudit to run audit and return the result
func (s *Service) RunAudit(profileID string) (*protocol.AuditResult, error) {
	profile, err := s.profileStore.Get(profileID)
	if err != nil {
		return nil, err
	}
	auditInput := &job.Audit{
		ProfileID:      profile.ID,
		URN:            profile.URN,
		TotalRecords:   profile.TotalRecords,
		EventTimestamp: time.Now().In(time.UTC),
		State:          job.StateCreated,
		Message:        fmt.Sprintf("Start AuditReport on Table %s", profile.URN),
	}

	label, err := protocol.ParseLabel(profile.URN)
	if err != nil {
		return nil, err
	}

	clientBuilder := s.statsClientBuilder.WithURN(label)
	statsClient, err := clientBuilder.Build()
	if err != nil {
		return nil, err
	}

	audit, err := s.auditStore.CreateAudit(auditInput)
	if err != nil {
		return nil, err
	}

	jobCreatedMetric := stats.Metric("audit.job.created.count")
	statsClient.Increment(jobCreatedMetric)

	reports, err := s.run(statsClient, audit)
	if err != nil {
		audit.Message = fmt.Sprintf("AuditReport Table %s failed - %v", audit.URN, err)
		audit.State = job.StateFailed
		err = s.auditStore.UpdateAudit(audit)
		jobFailedMetric := stats.Metric("audit.job.failed.count")
		statsClient.Increment(jobFailedMetric)
		return nil, err
	}

	audit.Message = fmt.Sprintf("Table %s has all audited", audit.URN)
	audit.State = job.StateCompleted
	err = s.auditStore.UpdateAudit(audit)
	if err != nil {
		return nil, err
	}

	jobCompletedMetric := stats.Metric("audit.job.completed.count")
	statsClient.Increment(jobCompletedMetric)

	jobDurationStat := stats.Metric("audit.job.time")
	start := audit.EventTimestamp
	end := time.Now().In(time.UTC)
	statsClient.DurationOf(jobDurationStat, start, end)

	auditResult := &protocol.AuditResult{
		Audit:        audit,
		AuditReports: reports,
	}
	return auditResult, err
}

func (s *Service) run(statsClient stats.Client, audit *job.Audit) ([]*protocol.AuditReport, error) {
	jobInprogressMetric := stats.Metric("audit.job.inprogress.count")
	statsClient.Increment(jobInprogressMetric)

	auditResults, err := s.auditor.Audit(audit)
	if err != nil {
		return nil, err
	}

	if err = s.resultStore.StoreResults(auditResults); err != nil {
		return nil, err
	}

	messageProviders := s.messageProviderFactory.CreateAuditMessage(audit, auditResults)
	for _, messageProvider := range messageProviders {
		if err = s.publisher.Publish(messageProvider); err != nil {
			return nil, err
		}
	}

	return auditResults, nil
}
