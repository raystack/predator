package tolerance

import (
	"errors"
	"github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestUploadFactory(t *testing.T) {
	t.Run("Create", func(t *testing.T) {
		t.Run("should create upload task", func(t *testing.T) {
			gitURL := "sample-git-url-entity-1"
			pathPrefix := "predator"

			gitInfo := &protocol.GitInfo{
				URL:        gitURL,
				CommitID:   "123abcd456",
				PathPrefix: pathPrefix,
			}

			projectIDs := []string{"entity-1-project-1", "entity-1-project-2"}
			entity := &protocol.Entity{
				ID:            "entity-1",
				Name:          "entity-1-name",
				Environment:   "env-a",
				GitURL:        gitURL,
				GcpProjectIDs: projectIDs,
			}

			fileStore := mock.NewMockFileStore()
			defer fileStore.AssertExpectations(t)

			gitRepositoryFac := mock.NewMockGitRepositoryFactory()
			defer gitRepositoryFac.AssertExpectations(t)

			gitRepository := mock.NewMockGitRepository()
			defer gitRepository.AssertExpectations(t)

			sourceStoreFactory := mock.NewMockToleranceStoreFactory()
			defer sourceStoreFactory.AssertExpectations(t)

			sourceStore := mock.NewToleranceStore()
			defer sourceStore.AssertExpectations(t)

			destStore := mock.NewToleranceStore()
			defer destStore.AssertExpectations(t)

			entityStore := mock.NewEntityStore()
			defer entityStore.AssertExpectations(t)

			statsClient := mock.NewDummyStats()
			defer statsClient.AssertExpectations(t)

			statsClientBuilder := mock.NewStatBuilder()
			defer statsClientBuilder.AssertExpectations(t)

			metadataStore := mock.NewMetadataStore()
			defer metadataStore.AssertExpectations(t)

			specValidator := NewSpecValidator(metadataStore)

			statsClientBuilder.On("WithEntity", entity).Return(statsClientBuilder)
			statsClientBuilder.On("Build").Return(statsClient, nil)

			uploadTask := &Upload{
				source:        NewEntityBasedStore(entity, sourceStore),
				destination:   NewEntityBasedStore(entity, destStore),
				statsClient:   statsClient,
				specValidator: specValidator,
			}

			gitRepositoryFac.On("CreateWithPrefix", gitURL, pathPrefix).Return(gitRepository, nil)

			gitRepository.On("Checkout", gitInfo.CommitID).Return(fileStore, nil)

			sourceStoreFactory.On("CreateWithOptions", fileStore, protocol.Git).Return(sourceStore, nil)

			entityStore.On("GetEntityByGitURL", gitURL).Return(entity, nil)

			factory := NewUploadFactory(true, entityStore, sourceStoreFactory, destStore, gitRepositoryFac, statsClientBuilder, metadataStore)
			result, err := factory.Create(gitInfo)

			assert.Nil(t, err)
			assert.Equal(t, uploadTask, result)
		})
		t.Run("should return error when entity not found", func(t *testing.T) {
			multiTenancyEnabled := true
			gitURL := "sample-git-url-entity-1"
			pathPrefix := "predator"

			gitInfo := &protocol.GitInfo{
				URL:        gitURL,
				CommitID:   "123abcd456",
				PathPrefix: pathPrefix,
			}

			fileStore := mock.NewMockFileStore()
			defer fileStore.AssertExpectations(t)

			sourceStore := mock.NewToleranceStore()
			defer sourceStore.AssertExpectations(t)

			gitRepositoryFac := mock.NewMockGitRepositoryFactory()
			defer gitRepositoryFac.AssertExpectations(t)

			gitRepository := mock.NewMockGitRepository()
			defer gitRepository.AssertExpectations(t)

			sourceStoreFactory := mock.NewMockToleranceStoreFactory()
			defer sourceStoreFactory.AssertExpectations(t)

			entityStore := mock.NewEntityStore()
			defer entityStore.AssertExpectations(t)

			gitRepositoryFac.On("CreateWithPrefix", gitURL, pathPrefix).Return(gitRepository, nil)

			gitRepository.On("Checkout", gitInfo.CommitID).Return(fileStore, nil)

			sourceStoreFactory.On("CreateWithOptions", fileStore, protocol.Git).Return(sourceStore, nil)

			entityStore.On("GetEntityByGitURL", gitURL).Return(&protocol.Entity{}, protocol.ErrEntityNotFound)

			factory := NewUploadFactory(multiTenancyEnabled, entityStore, sourceStoreFactory, nil, gitRepositoryFac, nil, nil)
			upload, err := factory.Create(gitInfo)

			assert.Nil(t, upload)
			assert.Error(t, err)
		})
		t.Run("should create upload task when multi tenancy not enabled", func(t *testing.T) {
			gitURL := "sample-git-url-entity-1"
			pathPrefix := "predator"

			gitInfo := &protocol.GitInfo{
				URL:        gitURL,
				CommitID:   "123abcd456",
				PathPrefix: pathPrefix,
			}

			fileStore := mock.NewMockFileStore()
			defer fileStore.AssertExpectations(t)

			gitRepositoryFac := mock.NewMockGitRepositoryFactory()
			defer gitRepositoryFac.AssertExpectations(t)

			gitRepository := mock.NewMockGitRepository()
			defer gitRepository.AssertExpectations(t)

			sourceStoreFactory := mock.NewMockToleranceStoreFactory()
			defer sourceStoreFactory.AssertExpectations(t)

			sourceStore := mock.NewToleranceStore()
			defer sourceStore.AssertExpectations(t)

			destStore := mock.NewToleranceStore()
			defer destStore.AssertExpectations(t)

			entityStore := mock.NewEntityStore()
			defer entityStore.AssertExpectations(t)

			statsClient := mock.NewDummyStats()
			defer statsClient.AssertExpectations(t)

			statsClientBuilder := mock.NewStatBuilder()
			defer statsClientBuilder.AssertExpectations(t)

			metadataStore := mock.NewMetadataStore()
			defer metadataStore.AssertExpectations(t)

			specValidator := NewSpecValidator(metadataStore)

			statsClientBuilder.On("Build").Return(statsClient, nil)

			uploadTask := &Upload{
				source:        sourceStore,
				destination:   destStore,
				statsClient:   statsClient,
				specValidator: specValidator,
			}

			gitRepositoryFac.On("CreateWithPrefix", gitURL, pathPrefix).Return(gitRepository, nil)
			gitRepository.On("Checkout", gitInfo.CommitID).Return(fileStore, nil)
			sourceStoreFactory.On("CreateWithOptions", fileStore, protocol.Git).Return(sourceStore, nil)

			factory := NewUploadFactory(false, entityStore, sourceStoreFactory, destStore, gitRepositoryFac, statsClientBuilder, metadataStore)
			result, err := factory.Create(gitInfo)

			assert.Equal(t, uploadTask, result)
			assert.Nil(t, err)
		})
	})
}

func TestUpload(t *testing.T) {
	t.Run("Upload", func(t *testing.T) {
		t.Run("Run", func(t *testing.T) {
			t.Run("should upload files from git repository to destination", func(t *testing.T) {
				toleranceSpecs := []*protocol.ToleranceSpec{
					{
						URN: "entity-1-project-2.dataset_a.table_x",
					},
					{
						URN: "entity-1-project-1.dataset_b.table_x",
					},
				}

				sourceURNs := []string{
					"entity-1-project-2.dataset_a.table_x",
					"entity-1-project-1.dataset_b.table_x",
				}

				destURNs := []string{
					"entity-1-project-1.dataset_b.table_x",
					"entity-1-project-1.dataset_d.table_x",
				}

				toCreate := []string{"entity-1-project-2.dataset_a.table_x", "entity-1-project-1.dataset_b.table_x"}
				toRemove := []string{"entity-1-project-1.dataset_d.table_x"}

				sourceStore := mock.NewToleranceStore()
				defer sourceStore.AssertExpectations(t)

				destStore := mock.NewToleranceStore()
				defer destStore.AssertExpectations(t)

				specValidator := mock.NewSpecValidator()
				defer specValidator.AssertExpectations(t)

				destStore.On("GetResourceNames").Return(destURNs, nil)
				sourceStore.On("GetResourceNames").Return(sourceURNs, nil)

				for i, c := range toCreate {
					sourceStore.On("GetByTableID", c).Return(toleranceSpecs[i], nil)
					specValidator.On("Validate", toleranceSpecs[i]).Return(nil)
				}

				for i, c := range toCreate {
					sourceStore.On("GetByTableID", c).Return(toleranceSpecs[i], nil)
					destStore.On("Create", toleranceSpecs[i]).Return(nil)
				}

				for _, r := range toRemove {
					destStore.On("Delete", r).Return(nil)
				}

				statsClient := mock.NewDummyStats()
				defer statsClient.AssertExpectations(t)

				upload := &Upload{
					source:        sourceStore,
					destination:   destStore,
					statsClient:   statsClient,
					specValidator: specValidator,
				}

				report, err := upload.Run()

				expectedReport := &job.Diff{
					Add:    []string{"entity-1-project-2.dataset_a.table_x"},
					Remove: []string{"entity-1-project-1.dataset_d.table_x"},
					Update: []string{"entity-1-project-1.dataset_b.table_x"},
				}

				assert.Equal(t, expectedReport, report)
				assert.Nil(t, err)
			})
			t.Run("should return error when unable to get resource names from git repository", func(t *testing.T) {
				sourceURNs := []string{
					"entity-1-project-2.dataset_a.table_x",
					"entity-1-project-1.dataset_b.table_x",
				}

				fileErr := errors.New("wrong directory format")

				sourceStore := mock.NewToleranceStore()
				defer sourceStore.AssertExpectations(t)

				destStore := mock.NewToleranceStore()
				defer destStore.AssertExpectations(t)

				sourceStore.On("GetResourceNames").Return(sourceURNs, fileErr)

				statsClient := mock.NewDummyStats()
				defer statsClient.AssertExpectations(t)

				upload := &Upload{
					source:      sourceStore,
					destination: destStore,
					statsClient: statsClient,
				}

				report, err := upload.Run()

				var diff *job.Diff
				assert.Equal(t, diff, report)
				assert.Error(t, err)
			})
			t.Run("should return error when unable to create tolerance to destination storage", func(t *testing.T) {
				toleranceSpecs := []*protocol.ToleranceSpec{
					{
						URN: "entity-1-project-2.dataset_c.table_x",
					},
				}

				sourceURNs := []string{
					"entity-1-project-2.dataset_c.table_x",
				}

				destURNs := []string{
					"entity-1-project-1.dataset_b.table_x",
					"entity-1-project-1.dataset_d.table_x",
				}

				toRemove := []string{
					"entity-1-project-1.dataset_b.table_x",
					"entity-1-project-1.dataset_d.table_x",
				}

				apiErr := errors.New("gcs error")

				sourceStore := mock.NewToleranceStore()
				defer sourceStore.AssertExpectations(t)

				destStore := mock.NewToleranceStore()
				defer destStore.AssertExpectations(t)

				specValidator := mock.NewSpecValidator()
				defer specValidator.AssertExpectations(t)

				destStore.On("GetResourceNames").Return(destURNs, nil)
				sourceStore.On("GetResourceNames").Return(sourceURNs, nil)

				sourceStore.On("GetByTableID", sourceURNs[0]).Return(toleranceSpecs[0], nil)
				destStore.On("Create", toleranceSpecs[0]).Return(apiErr)

				for i, c := range sourceURNs {
					sourceStore.On("GetByTableID", c).Return(toleranceSpecs[i], nil)
					specValidator.On("Validate", toleranceSpecs[i]).Return(nil)
				}

				for _, r := range toRemove {
					destStore.On("Delete", r).Return(nil)
				}

				statsClient := mock.NewDummyStats()
				defer statsClient.AssertExpectations(t)

				upload := &Upload{
					source:        sourceStore,
					destination:   destStore,
					statsClient:   statsClient,
					specValidator: specValidator,
				}

				report, err := upload.Run()

				assert.Nil(t, report)
				assert.Error(t, err)
			})
			t.Run("should return ErrUploadSpecValidation when spec file is invalid", func(t *testing.T) {
				toleranceSpecs := []*protocol.ToleranceSpec{
					{
						URN: "entity-1-project-2.dataset_c.table_x",
					},
				}

				sourceURNs := []string{
					"entity-1-project-2.dataset_c.table_x",
				}

				destURNs := []string{
					"entity-1-project-1.dataset_b.table_x",
					"entity-1-project-1.dataset_d.table_x",
				}

				errSpecInvalid := &protocol.ErrSpecInvalid{
					URN:    "entity-1-project-2.dataset_c.table_x",
					Errors: []error{errors.New("field not found")},
				}

				errSpecValidation := &protocol.ErrUploadSpecValidation{Errors: []error{errSpecInvalid}}

				sourceStore := mock.NewToleranceStore()
				defer sourceStore.AssertExpectations(t)

				destStore := mock.NewToleranceStore()
				defer destStore.AssertExpectations(t)

				specValidator := mock.NewSpecValidator()
				defer specValidator.AssertExpectations(t)

				destStore.On("GetResourceNames").Return(destURNs, nil)
				sourceStore.On("GetResourceNames").Return(sourceURNs, nil)

				sourceStore.On("GetByTableID", "entity-1-project-2.dataset_c.table_x").Return(toleranceSpecs[0], nil)
				specValidator.On("Validate", toleranceSpecs[0]).Return(errSpecInvalid)

				statsClient := mock.NewDummyStats()
				defer statsClient.AssertExpectations(t)

				upload := &Upload{
					source:        sourceStore,
					destination:   destStore,
					statsClient:   statsClient,
					specValidator: specValidator,
				}

				report, err := upload.Run()

				assert.Nil(t, report)
				assert.Equal(t, errSpecValidation, err)
			})
		})
	})
}
