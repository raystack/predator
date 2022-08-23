package tolerance

import (
	"errors"
	"fmt"
	"github.com/odpf/predator/protocol"
)

//FileBasedStore is storage of tolerance spec that use file as storage
type FileBasedStore struct {
	fileStore    protocol.FileStore
	pathResolver protocol.PathResolver
	parser       Parser
}

//NewFileBasedStore is constructor
func NewFileBasedStore(toleranceRepo protocol.FileStore, pathResolver protocol.PathResolver, parser Parser) *FileBasedStore {
	return &FileBasedStore{
		fileStore:    toleranceRepo,
		pathResolver: pathResolver,
		parser:       parser,
	}
}

//GetByTableID to get tolerances of a table using table ID
func (f *FileBasedStore) GetByTableID(tableID string) (*protocol.ToleranceSpec, error) {
	relativePath, err := f.pathResolver.GetPath(tableID)
	if err != nil {
		return nil, err
	}

	file, err := f.fileStore.Get(relativePath)
	if err != nil {
		if errors.Is(err, protocol.ErrFileNotFound) {
			return nil, fmt.Errorf("failed to get file %s :\n%w", relativePath, protocol.ErrToleranceNotFound)
		}
		return nil, fmt.Errorf("failed to get file %s :\n%w", relativePath, err)
	}

	toleranceSpec, err := f.parser.Parse(file.Content)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s :\n%w", relativePath, err)
	}

	return toleranceSpec, nil
}

func (f *FileBasedStore) Create(spec *protocol.ToleranceSpec) error {
	filePath, err := f.pathResolver.GetPath(spec.URN)
	if err != nil {
		return err
	}

	content, err := f.parser.Serialise(spec)
	if err != nil {
		return fmt.Errorf("failed to write yaml %s spec %w", spec.URN, err)
	}
	file := &protocol.File{
		Path:    filePath,
		Content: content,
	}

	if err = f.fileStore.Create(file); err != nil {
		return fmt.Errorf("failed to create file %s :\n%w", file.Path, err)
	}

	return nil
}

func (f *FileBasedStore) Delete(tableID string) error {
	filePath, err := f.pathResolver.GetPath(tableID)
	if err != nil {
		return err
	}
	if err = f.fileStore.Delete(filePath); err != nil {
		return fmt.Errorf("failed to delete file %s :\n%w", filePath, err)
	}
	return nil
}

func (f *FileBasedStore) GetAll() ([]*protocol.ToleranceSpec, error) {
	files, err := f.fileStore.GetAll()
	if err != nil {
		return nil, err
	}

	var specs []*protocol.ToleranceSpec
	for _, file := range files {
		urn, err := f.pathResolver.GetURN(file.Path)
		if err != nil {
			return nil, err
		}

		spec, err := f.parser.Parse(file.Content)
		if err != nil {
			return nil, fmt.Errorf("failed to parse file %s :\n%w", file.Path, err)
		}

		if urn != spec.URN {
			return nil, fmt.Errorf("invalid tableID %s", file.Path)
		}

		specs = append(specs, spec)
	}

	return specs, nil
}

func (f *FileBasedStore) GetByProjectID(projectID string) ([]*protocol.ToleranceSpec, error) {
	paths, err := f.fileStore.GetPaths()
	if err != nil {
		return nil, err
	}

	var specs []*protocol.ToleranceSpec
	for _, path := range paths {
		urn, err := f.pathResolver.GetURN(path)
		if err != nil {
			return nil, err
		}

		label, err := protocol.ParseLabel(urn)
		if err != nil {
			return nil, err
		}

		if label.Project == projectID {
			file, err := f.fileStore.Get(path)
			if err != nil {
				return nil, err
			}

			spec, err := f.parser.Parse(file.Content)
			if err != nil {
				return nil, err
			}

			specs = append(specs, spec)
		}
	}

	return specs, nil
}

func (f *FileBasedStore) GetResourceNames() ([]string, error) {
	paths, err := f.fileStore.GetPaths()
	if err != nil {
		return nil, err
	}

	var urns []string
	for _, path := range paths {
		urn, err := f.pathResolver.GetURN(path)
		if err != nil {
			return nil, fmt.Errorf("failed to get urn from path %s :\n%w", path, err)
		}

		urns = append(urns, urn)
	}

	return urns, nil
}

type Factory struct {
	resolverFactory  *PathResolverFactory
	fileStoreFactory protocol.FileStoreFactory
}

//NewFactory create Factory of protocol.ToleranceStore
func NewFactory(resolverFactory *PathResolverFactory, fileStoreFactory protocol.FileStoreFactory) *Factory {
	return &Factory{resolverFactory: resolverFactory, fileStoreFactory: fileStoreFactory}
}

//Create multiple implementation of protocol.ToleranceStore
//this Method only support protocol.MultiTenancy and protocol.Default protocol.PathType directory structure of protocol.PathResolver
func (t *Factory) Create(URL string, multiTenancyEnabled bool) (protocol.ToleranceStore, error) {
	fileStore, err := t.fileStoreFactory.Create(URL)
	if err != nil {
		return nil, err
	}

	var resolver protocol.PathResolver
	if multiTenancyEnabled {
		resolver = t.resolverFactory.CreateResolver(protocol.MultiTenancy)
	} else {
		resolver = t.resolverFactory.CreateResolver(protocol.Default)
	}

	return NewFileBasedStore(fileStore, resolver, NewSmartParser()), nil
}

//CreateWithOptions intended to create more customised version of protocol.ToleranceStore
//this support custom protocol.FileStore and all of protocol.PathType including
//protocol.MultiTenancy, protocol.Default and protocol.Git
func (t *Factory) CreateWithOptions(store protocol.FileStore, pathType protocol.PathType) (protocol.ToleranceStore, error) {
	var resolver protocol.PathResolver
	switch pathType {
	case protocol.MultiTenancy:
		resolver = t.resolverFactory.CreateResolver(protocol.MultiTenancy)
	case protocol.Default:
		resolver = t.resolverFactory.CreateResolver(protocol.Default)
	case protocol.Git:
		resolver = t.resolverFactory.CreateResolver(protocol.Git)
	default:
		return nil, errors.New("unsupported protocol.PathType")
	}
	return NewFileBasedStore(store, resolver, NewSmartParser()), nil
}
