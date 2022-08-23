package audit

import (
	"fmt"
	"github.com/odpf/predator/publisher/message"
	"testing"

	"github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/stretchr/testify/assert"
)

func TestAuditService(t *testing.T) {
	t.Run("RunAudit", func(t *testing.T) {
		t.Run("should able to run audit", func(t *testing.T) {
			profileID := "profile-abcd"
			tableURN := "a.b.c"
			groupValue := "2019-01-01"
			profile := &job.Profile{
				ID:  profileID,
				URN: tableURN,
			}
			label := &protocol.Label{
				Project: "a",
				Dataset: "b",
				Table:   "c",
			}

			messageStart := fmt.Sprintf("Start AuditReport on Table %s", tableURN)
			messageCompleted := fmt.Sprintf("Table %s has all audited", tableURN)

			profileStore := mock.NewProfileStore()
			profileStore.On("Get", profileID).Return(profile, nil)

			// Create AuditReport
			auditInput := &job.Audit{
				ProfileID: profileID,
				State:     job.StateCreated,
				URN:       tableURN,
				Message:   messageStart,
			}
			auditID := "audit-abcd"
			auditOutput := &job.Audit{
				ID:        auditID,
				ProfileID: profileID,
				State:     job.StateCreated,
				URN:       tableURN,
				Message:   messageStart,
			}

			// Insert to AuditReport
			auditStore := mock.NewAuditStore()
			auditStore.On("CreateAudit", auditInput).Return(auditOutput, nil)
			defer auditStore.AssertExpectations(t)

			// Do AuditReport
			auditReports := []*protocol.AuditReport{
				{
					AuditID:     auditID,
					TableURN:    tableURN,
					MetricName:  "duplication_pct",
					MetricValue: 0.1,
					GroupValue:  groupValue,
					ToleranceRules: []protocol.ToleranceRule{
						{
							Comparator: protocol.ComparatorLessThanEq,
							Value:      1.0,
						},
					},
					PassFlag: true,
				},
				{
					AuditID:     auditID,
					TableURN:    tableURN,
					MetricName:  "row_count",
					MetricValue: 100.0,
					GroupValue:  groupValue,
					ToleranceRules: []protocol.ToleranceRule{
						{
							Comparator: protocol.ComparatorMoreThanEq,
							Value:      1.0,
						},
					},
					PassFlag: true,
				},
			}

			messageProviders := []protocol.MessageProvider{
				&message.Provider{},
			}

			messageBuilderFactory := mock.NewMessageProviderFactory()
			defer messageBuilderFactory.AssertExpectations(t)
			messageBuilderFactory.On("CreateAuditMessage", auditOutput, auditReports).Return(messageProviders)

			auditor := mock.NewAuditor()
			auditor.On("Audit", auditOutput).Return(auditReports, nil)
			defer auditor.AssertExpectations(t)

			// Publishing
			publisher := mock.NewPublisher()
			defer publisher.AssertExpectations(t)
			publisher.On("Publish", messageProviders[0]).Return(nil)

			// Store Result
			resultStore := mock.NewAuditResultStore()
			resultStore.On("StoreResults", auditReports).Return(nil)
			defer auditor.AssertExpectations(t)

			auditOutput.Message = messageCompleted
			auditOutput.State = job.StateCompleted
			auditStore.On("UpdateAudit", auditOutput).Return(nil)
			defer auditStore.AssertExpectations(t)

			statsClientBuilder := mock.NewStatBuilder()
			defer statsClientBuilder.AssertExpectations(t)

			statsClient := mock.NewDummyStats()
			statsClientBuilder.On("WithURN", label).Return(statsClientBuilder)
			statsClientBuilder.On("Build").Return(statsClient, nil)

			auditService := &Service{
				profileStore:           profileStore,
				auditStore:             auditStore,
				resultStore:            resultStore,
				auditor:                auditor,
				publisher:              publisher,
				messageProviderFactory: messageBuilderFactory,
				statsClientBuilder:     statsClientBuilder,
			}

			expectedResult := &protocol.AuditResult{
				Audit:        auditOutput,
				AuditReports: auditReports,
			}

			actualResult, actualErr := auditService.RunAudit(profileID)
			assert.Equal(t, expectedResult, actualResult)
			assert.Nil(t, actualErr)
		})
	})
}
