package builder

import (
	"github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/stats"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMultitenancy(t *testing.T) {
	t.Run("Build", func(t *testing.T) {
		t.Run("should return stat with pod name", func(t *testing.T) {
			pod := "1234"
			tags := []stats.KV{{K: "pod", V: pod}}

			statsClient := mock.NewDummyStats()
			defer statsClient.AssertExpectations(t)

			statsClient.On("WithTags", tags).Return(statsClient, nil)

			var builder stats.ClientBuilder
			builder = NewMultiTenancy(false, nil, statsClient)
			builder = builder.WithPodName(pod)
			_, err := builder.Build()

			assert.Nil(t, err)
		})
		t.Run("should return stat with environment tags", func(t *testing.T) {
			env := "env-name"
			tags := []stats.KV{{K: "environment", V: env}}

			statsClient := mock.NewDummyStats()
			defer statsClient.AssertExpectations(t)

			statsClient.On("WithTags", tags).Return(statsClient, nil)

			var builder stats.ClientBuilder
			builder = NewMultiTenancy(false, nil, statsClient)
			builder = builder.WithEnvironment(env)
			_, err := builder.Build()

			assert.Nil(t, err)
		})
		t.Run("should return stat with deployment tags", func(t *testing.T) {
			deployment := "predator-1"
			tags := []stats.KV{{K: "deployment", V: deployment}}

			statsClient := mock.NewDummyStats()
			defer statsClient.AssertExpectations(t)

			statsClient.On("WithTags", tags).Return(statsClient, nil)

			var builder stats.ClientBuilder
			builder = NewMultiTenancy(false, nil, statsClient)
			builder = builder.WithDeployment(deployment)
			_, err := builder.Build()

			assert.Nil(t, err)
		})
		t.Run("should return stat with urn label tags", func(t *testing.T) {
			urn := &protocol.Label{
				Project: "a",
				Dataset: "b",
				Table:   "c",
			}
			projectTag := stats.KV{K: "project", V: "a"}
			datasetTag := stats.KV{K: "dataset", V: "b"}
			tableTag := stats.KV{K: "table", V: "c"}

			tags := []stats.KV{projectTag, datasetTag, tableTag}

			statsClient := mock.NewDummyStats()
			defer statsClient.AssertExpectations(t)

			statsClient.On("WithTags", tags).Return(statsClient, nil)

			var builder stats.ClientBuilder
			builder = NewMultiTenancy(false, nil, statsClient)
			builder = builder.WithURN(urn)
			_, err := builder.Build()

			assert.Nil(t, err)
		})
		t.Run("should return stat given entity information", func(t *testing.T) {
			entity := &protocol.Entity{
				ID:          "entity-1",
				Environment: "env-a",
			}
			tags := []stats.KV{
				{K: "entity", V: "entity-1"},
				{K: "environment", V: "env-a"},
			}

			statsClient := mock.NewDummyStats()
			defer statsClient.AssertExpectations(t)

			statsClient.On("WithTags", tags).Return(statsClient, nil)

			var builder stats.ClientBuilder
			builder = NewMultiTenancy(true, nil, statsClient)
			builder = builder.WithEntity(entity)
			_, err := builder.Build()

			assert.Nil(t, err)
		})
		t.Run("should return stat given urn when multi tenancy enabled", func(t *testing.T) {
			urn := &protocol.Label{
				Project: "entity-1-project-1",
				Dataset: "a",
				Table:   "b",
			}
			entity := &protocol.Entity{
				ID:            "entity-1",
				Environment:   "env-a",
				GcpProjectIDs: []string{"entity-1-project-1"},
			}
			tags := []stats.KV{
				{K: "entity", V: "entity-1"},
				{K: "environment", V: "env-a"},
				{K: "project", V: "entity-1-project-1"},
				{K: "dataset", V: "a"},
				{K: "table", V: "b"},
			}

			entityStore := mock.NewEntityStore()
			defer entityStore.AssertExpectations(t)

			entityStore.On("GetEntityByProjectID", urn.Project).Return(entity, nil)

			statsClient := mock.NewDummyStats()
			defer statsClient.AssertExpectations(t)

			statsClient.On("WithTags", tags).Return(statsClient, nil)

			var builder stats.ClientBuilder
			builder = NewMultiTenancy(true, entityStore, statsClient)
			builder = builder.WithURN(urn)
			_, err := builder.Build()

			assert.Nil(t, err)
		})
	})
}
