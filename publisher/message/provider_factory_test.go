package message

import (
	"github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/odpf/predator/protocol/metric"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBuilderFactory(t *testing.T) {
	t.Run("CreateProfileMessage", func(t *testing.T) {
		t.Run("should create profile message builder", func(t *testing.T) {
			metadataStore := mock.NewMetadataStore()

			profile := &job.Profile{}
			metrics := []*metric.Metric{
				{
					GroupValue: "2019-01-01",
				},
				{
					GroupValue: "2019-01-02",
				},
			}

			factory := &ProviderFactory{
				MetadataStore: metadataStore,
			}

			messageProviders := factory.CreateProfileMessage(profile, metrics)
			expected := []protocol.MessageProvider{
				&Provider{
					KeyBuilder: &ProfileKeyProtoBuilder{
						Metrics: []*metric.Metric{metrics[0]},
						Profile: profile,
					},
					ValueBuilder: &ProfileValueProtoBuilder{
						Metrics:       []*metric.Metric{metrics[0]},
						Profile:       profile,
						MetadataStore: metadataStore,
					},
				},
				&Provider{
					KeyBuilder: &ProfileKeyProtoBuilder{
						Metrics: []*metric.Metric{metrics[1]},
						Profile: profile,
					},
					ValueBuilder: &ProfileValueProtoBuilder{
						Metrics:       []*metric.Metric{metrics[1]},
						Profile:       profile,
						MetadataStore: metadataStore,
					},
				},
			}

			assert.Equal(t, expected, messageProviders)
		})
	})
	t.Run("CreateAuditMessage", func(t *testing.T) {
		t.Run("should create audit message builder", func(t *testing.T) {
			profileStore := mock.NewProfileStore()

			audit := &job.Audit{}
			auditResult := []*protocol.AuditReport{
				{
					GroupValue: "2019-01-01",
				},
				{
					GroupValue: "2019-01-02",
				},
			}

			factory := &ProviderFactory{ProfileStore: profileStore}
			messageProviders := factory.CreateAuditMessage(audit, auditResult)

			expected := []protocol.MessageProvider{
				&Provider{
					ValueBuilder: &AuditValueProtoBuilder{
						AuditResult:  []*protocol.AuditReport{auditResult[0]},
						Audit:        audit,
						ProfileStore: profileStore,
					},
					KeyBuilder: &AuditKeyProtoBuilder{
						AuditResult:  []*protocol.AuditReport{auditResult[0]},
						Audit:        audit,
						ProfileStore: profileStore,
					},
				},
				&Provider{
					ValueBuilder: &AuditValueProtoBuilder{
						AuditResult:  []*protocol.AuditReport{auditResult[1]},
						Audit:        audit,
						ProfileStore: profileStore,
					},
					KeyBuilder: &AuditKeyProtoBuilder{
						AuditResult:  []*protocol.AuditReport{auditResult[1]},
						Audit:        audit,
						ProfileStore: profileStore,
					},
				},
			}

			assert.Equal(t, expected, messageProviders)
		})
	})
}
