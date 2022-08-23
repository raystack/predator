package builder

import (
	"errors"
	"fmt"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/stats"
)

type MultiTenancy struct {
	multiTenancyEnabled bool
	entityStore         protocol.EntityStore
	client              stats.Client

	entity      *protocol.Entity
	urn         *protocol.Label
	environment string
	podName     string
	deployment  string
}

func NewMultiTenancy(multiTenancyEnabled bool, entityStore protocol.EntityStore, client stats.Client) *MultiTenancy {
	return &MultiTenancy{multiTenancyEnabled: multiTenancyEnabled, entityStore: entityStore, client: client}
}

func (s *MultiTenancy) WithEntity(entity *protocol.Entity) stats.ClientBuilder {
	c := s.clone()
	c.entity = entity
	return c
}

func (s *MultiTenancy) WithURN(urn *protocol.Label) stats.ClientBuilder {
	c := s.clone()
	c.urn = urn
	return c
}

func (s *MultiTenancy) WithEnvironment(environment string) stats.ClientBuilder {
	c := s.clone()
	c.environment = environment
	return c
}

func (s *MultiTenancy) WithPodName(podName string) stats.ClientBuilder {
	c := s.clone()
	c.podName = podName
	return c
}

func (s *MultiTenancy) WithDeployment(deployment string) stats.ClientBuilder {
	c := s.clone()
	c.deployment = deployment
	return c
}

func (s *MultiTenancy) Build() (stats.Client, error) {
	var tags []stats.KV

	if s.multiTenancyEnabled {
		var entity *protocol.Entity

		if s.entity != nil {
			entity = s.entity
		} else if s.urn != nil {
			var err error
			entity, err = s.entityStore.GetEntityByProjectID(s.urn.Project)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, errors.New("enabling multi tenancy requires Entity or EntityByProjectID being set")
		}

		entityTag := stats.KV{K: "entity", V: entity.ID}
		tags = append(tags, entityTag)

		envTag := stats.KV{K: "environment", V: entity.Environment}
		tags = append(tags, envTag)
	} else {
		if s.environment != "" {
			envTag := stats.KV{K: "environment", V: s.environment}
			tags = append(tags, envTag)
		}
	}

	if s.podName != "" {
		podTag := stats.KV{K: "pod", V: s.podName}
		tags = append(tags, podTag)
	}

	if s.deployment != "" {
		deploymentTag := stats.KV{K: "deployment", V: s.deployment}
		tags = append(tags, deploymentTag)
	}

	if s.urn != nil {
		projectTag := stats.KV{K: "project", V: s.urn.Project}
		datasetTag := stats.KV{K: "dataset", V: s.urn.Dataset}
		tableTag := stats.KV{K: "table", V: s.urn.Table}

		tags = append(tags, []stats.KV{projectTag, datasetTag, tableTag}...)
	}

	fmt.Println(tags)
	return s.client.WithTags(tags...), nil
}

func (s *MultiTenancy) clone() *MultiTenancy {
	return &MultiTenancy{
		multiTenancyEnabled: s.multiTenancyEnabled,
		entityStore:         s.entityStore,
		client:              s.client,
		deployment:          s.deployment,
		entity:              s.entity,
		urn:                 s.urn,
		environment:         s.environment,
		podName:             s.podName,
	}
}
