package message

import (
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/odpf/predator/protocol/metric"
	"sort"
)

type ProviderFactory struct {
	ProfileStore  protocol.ProfileStore
	MetadataStore protocol.MetadataStore
}

func NewProviderFactory(profileStore protocol.ProfileStore, metadataStore protocol.MetadataStore) *ProviderFactory {
	return &ProviderFactory{ProfileStore: profileStore, MetadataStore: metadataStore}
}

func (d *ProviderFactory) CreateProfileMessage(profile *job.Profile, metrics []*metric.Metric) []protocol.MessageProvider {
	metricsByGroup := metric.Group(metrics).ByGroupValue()

	var providers []protocol.MessageProvider
	for _, metricsInGroup := range metricsByGroup {
		p := &Provider{
			KeyBuilder: &ProfileKeyProtoBuilder{
				Metrics: metricsInGroup,
				Profile: profile,
			},
			ValueBuilder: &ProfileValueProtoBuilder{
				Metrics:       metricsInGroup,
				Profile:       profile,
				MetadataStore: d.MetadataStore,
			},
		}
		providers = append(providers, p)
	}

	return providers
}

func (d *ProviderFactory) CreateAuditMessage(audit *job.Audit, auditResults []*protocol.AuditReport) []protocol.MessageProvider {
	auditGroup := protocol.AuditGroup(auditResults)
	resultsPerGroup := auditGroup.ByGroupValue()

	var groups []string
	for group := range resultsPerGroup {
		groups = append(groups, group)
	}

	sort.Strings(groups)

	var providers []protocol.MessageProvider
	for _, group := range groups {
		reports := resultsPerGroup[group]
		p := &Provider{
			KeyBuilder: &AuditKeyProtoBuilder{
				AuditResult:  reports,
				Audit:        audit,
				ProfileStore: d.ProfileStore,
			},
			ValueBuilder: &AuditValueProtoBuilder{
				AuditResult:  reports,
				Audit:        audit,
				ProfileStore: d.ProfileStore,
			},
		}
		providers = append(providers, p)
	}
	return providers
}
