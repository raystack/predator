package metadata_test

import (
	"errors"
	"github.com/odpf/predator/metadata"
	predatormock "github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/meta"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCache(t *testing.T) {
	t.Run("CachedStore", func(t *testing.T) {
		t.Run("GetMetadata", func(t *testing.T) {
			t.Run("should return tablespec", func(t *testing.T) {
				urn := "sample_project.sample_dataset.sample_table"
				cacheExpiration := 3

				field1 := &meta.FieldSpec{
					Name:      "field_1",
					FieldType: meta.FieldTypeString,
					Mode:      meta.ModeNullable,
					Parent:    nil,
					Level:     meta.RootLevel,
				}

				field2 := &meta.FieldSpec{
					Name:      "field_2",
					FieldType: meta.FieldTypeRecord,
					Mode:      meta.ModeRepeated,
					Parent:    nil,
					Level:     meta.RootLevel,
				}

				field2a := &meta.FieldSpec{
					Name:      "field_2_a",
					FieldType: meta.FieldTypeRecord,
					Mode:      meta.ModeRepeated,
					Parent:    field2,
					Level:     2,
				}

				field2ax := &meta.FieldSpec{
					Name:      "field_2_a_x",
					FieldType: meta.FieldTypeInteger,
					Mode:      meta.ModeNullable,
					Parent:    field2a,
					Level:     3,
				}

				field2.Fields = []*meta.FieldSpec{field2a}
				field2a.Fields = []*meta.FieldSpec{field2ax}

				tableSpec := &meta.TableSpec{
					ProjectName: "sample-project",
					DatasetName: "sample_dataset",
					TableName:   "sample_table",
					Fields:      []*meta.FieldSpec{field1, field2},
				}

				source := predatormock.NewMetadataStore()
				source.On("GetMetadata", urn).Return(tableSpec, nil)

				store := metadata.NewCachedStore(cacheExpiration, source)

				result, err := store.GetMetadata(urn)

				assert.Nil(t, err)
				assert.Equal(t, tableSpec, result)
				assert.ElementsMatch(t, tableSpec.FieldsFlatten(), result.FieldsFlatten())
			})
			t.Run("should return error when unable to get metadata", func(t *testing.T) {
				urn := "sample_project.sample_dataset.sample_table"
				cacheExpiration := 3
				expectedErr := errors.New("API error")

				source := predatormock.NewMetadataStore()
				defer source.AssertExpectations(t)
				source.On("GetMetadata", urn).Return(&meta.TableSpec{}, expectedErr)

				metadataStore := metadata.NewCachedStore(cacheExpiration, source)

				_, err := metadataStore.GetMetadata(urn)

				assert.Equal(t, expectedErr, err)
			})
			t.Run("should return ErrMetadataNotFound when table did not exist", func(t *testing.T) {
				urn := "sample_project.sample_dataset.sample_table"
				cacheExpiration := 3

				source := predatormock.NewMetadataStore()
				defer source.AssertExpectations(t)
				source.On("GetMetadata", urn).Return(&meta.TableSpec{}, protocol.ErrTableMetadataNotFound)

				metadataStore := metadata.NewCachedStore(cacheExpiration, source)

				_, err := metadataStore.GetMetadata(urn)
				assert.Equal(t, protocol.ErrTableMetadataNotFound, err)
			})
		})
	})
}
