package tolerance

import (
	"fmt"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
	"github.com/odpf/predator/stats"
	"golang.org/x/sync/errgroup"
	"time"
)

type UploadFactory struct {
	multiTenancyEnabled  bool
	entityStore          protocol.EntityStore
	sourceFactory        protocol.ToleranceStoreFactory
	destination          protocol.ToleranceStore
	gitRepositoryFactory protocol.GitRepositoryFactory
	statsClientBuilder   stats.ClientBuilder
	metadataStore        protocol.MetadataStore
}

//NewUploadFactory create UploadFactory
func NewUploadFactory(multiTenancyEnabled bool,
	entityStore protocol.EntityStore,
	sourceFactory protocol.ToleranceStoreFactory,
	destination protocol.ToleranceStore,
	gitRepositoryFactory protocol.GitRepositoryFactory,
	statsFactory stats.ClientBuilder,
	metadataStore protocol.MetadataStore) *UploadFactory {
	return &UploadFactory{
		multiTenancyEnabled:  multiTenancyEnabled,
		entityStore:          entityStore,
		sourceFactory:        sourceFactory,
		destination:          destination,
		gitRepositoryFactory: gitRepositoryFactory,
		statsClientBuilder:   statsFactory,
		metadataStore:        metadataStore,
	}
}

//Create create protocol.Task
func (u *UploadFactory) Create(gitRepo *protocol.GitInfo) (protocol.Task, error) {
	gitRepository := u.gitRepositoryFactory.CreateWithPrefix(gitRepo.URL, gitRepo.PathPrefix)
	fileStore, err := gitRepository.Checkout(gitRepo.CommitID)
	if err != nil {
		return nil, fmt.Errorf("failed to clone git repository\n%w", err)
	}

	gitSource, err := u.sourceFactory.CreateWithOptions(fileStore, protocol.Git)
	if err != nil {
		return nil, err
	}

	var source protocol.ToleranceStore
	var destination protocol.ToleranceStore
	var statsClient stats.Client

	if u.multiTenancyEnabled {
		entity, err := u.entityStore.GetEntityByGitURL(gitRepo.URL)
		if err != nil {
			return nil, err
		}
		source = NewEntityBasedStore(entity, gitSource)
		destination = NewEntityBasedStore(entity, u.destination)
		clientBuilder := u.statsClientBuilder.WithEntity(entity)
		statsClient, err = clientBuilder.Build()
	} else {
		source = gitSource
		destination = u.destination
		statsClient, err = u.statsClientBuilder.Build()
	}

	return &Upload{
		source:        source,
		destination:   destination,
		statsClient:   statsClient,
		specValidator: NewSpecValidator(u.metadataStore),
	}, nil
}

type Upload struct {
	source        protocol.ToleranceStore
	destination   protocol.ToleranceStore
	statsClient   stats.Client
	specValidator protocol.SpecValidator
}

//Run get entity information
func (u *Upload) Run() (i interface{}, err error) {
	var diff *job.Diff
	startTime := time.Now().In(time.UTC)

	sourceURNs, err := u.source.GetResourceNames()
	if err != nil {
		return diff, fmt.Errorf("failed to get resources"+
			"from source repository\n%w", err)
	}

	destURNs, err := u.destination.GetResourceNames()
	if err != nil {
		return diff, fmt.Errorf("failed to read resources"+
			"from destination:\n%w", err)
	}

	diff = job.DiffBetween(sourceURNs, destURNs)

	var toBeCreated []string
	toBeCreated = append(toBeCreated, diff.Add...)
	toBeCreated = append(toBeCreated, diff.Update...)

	err = u.validateSpecFiles(toBeCreated)
	if err != nil {
		return nil, err
	}

	err = u.syncFiles(toBeCreated, diff.Remove)
	if err != nil {
		return nil, err
	}

	currentSpecCount := len(sourceURNs)
	totalSpecCount := currentSpecCount + (diff.AddedCount() - diff.RemovedCount())
	specCountStat := stats.Metric("spec.count")
	u.statsClient.Gauge(specCountStat, float64(totalSpecCount))

	specAddedCountStat := stats.Metric("spec.added.count")
	u.statsClient.IncrementBy(specAddedCountStat, int64(diff.AddedCount()))

	specRemovedCountStat := stats.Metric("spec.removed.count")
	u.statsClient.IncrementBy(specRemovedCountStat, int64(diff.RemovedCount()))

	specUpdatedCountStat := stats.Metric("spec.updated.count")
	u.statsClient.IncrementBy(specUpdatedCountStat, int64(diff.UpdatedCount()))

	jobDurationStat := stats.Metric("spec.upload.job.time")
	u.statsClient.DurationUntilNow(jobDurationStat, startTime)

	return diff, nil
}

func (u *Upload) syncFiles(toBeCreated []string, toBeRemoved []string) error {
	g := new(errgroup.Group)
	for _, urn := range toBeCreated {
		id := urn
		g.Go(func() error {
			spec, err := u.source.GetByTableID(id)
			if err != nil {
				return err
			}

			err = u.destination.Create(spec)
			if err != nil {
				return err
			}

			return nil
		})
	}

	for _, urn := range toBeRemoved {
		id := urn
		g.Go(func() error {
			if err := u.destination.Delete(id); err != nil {
				return fmt.Errorf("failed to remove %s spec because:\n%w", urn, err)
			}
			return nil
		})
	}

	return g.Wait()
}

func (u *Upload) validateSpecFiles(urns []string) error {
	errorChan := make(chan error, len(urns))

	for _, urn := range urns {
		id := urn
		go func() {
			var err error
			defer func() {
				errorChan <- err
			}()

			spec, err := u.source.GetByTableID(id)
			if err != nil {
				return
			}

			err = u.specValidator.Validate(spec)
			if err != nil {
				return
			}
		}()
	}

	var errors []error

	for i := 0; i < len(urns); i++ {
		err := <-errorChan
		if err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		var validationErrors []error
		for _, err := range errors {
			if protocol.IsSpecInvalidError(err) {
				validationErrors = append(validationErrors, err)
			}
		}

		if len(validationErrors) > 0 {
			return &protocol.ErrUploadSpecValidation{Errors: errors}
		} else {
			firstError := errors[0]
			return firstError
		}
	}

	return nil
}
