package tolerance

import (
	"errors"
	"fmt"
	"github.com/odpf/predator/protocol"
	"regexp"
)

type PathResolverFactory struct {
	entityStore protocol.EntityStore
}

//NewPathResolverFactory create PathResolverFactory
func NewPathResolverFactory(entityStore protocol.EntityStore) *PathResolverFactory {
	return &PathResolverFactory{entityStore: entityStore}
}

//CreateResolver create PathResolver
func (p *PathResolverFactory) CreateResolver(pathType protocol.PathType) protocol.PathResolver {
	switch pathType {
	case protocol.Git:
		return &GitPathResolver{}
	case protocol.MultiTenancy:
		return &MultiTenancyPathResolver{entityStore: p.entityStore}
	case protocol.Default:
		return &DefaultPathResolver{}
	}
	return nil
}

var defaultPathPattern = regexp.MustCompile(`(?P<project>[\w-_]+)\.(?P<dataset>[\w_]+)\.(?P<table>[\w_]+).yaml`)

//DefaultPathResolver to get file path with {project-id}.{dataset}.{tablename}.yaml format
//this is the default setting that applied without multi tenancy enabled
type DefaultPathResolver struct {
}

func (d *DefaultPathResolver) GetURN(filePath string) (string, error) {
	matches := defaultPathPattern.FindStringSubmatch(filePath)
	if len(matches) == 0 {
		return "", errors.New("wrong path format")
	}
	label := &protocol.Label{
		Project: matches[1],
		Dataset: matches[2],
		Table:   matches[3],
	}

	return label.String(), nil
}

func (d *DefaultPathResolver) GetPath(urn string) (string, error) {
	label, err := protocol.ParseLabel(urn)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s.yaml", label.String()), nil
}

var multiTenancyPathPattern = regexp.MustCompile(`(?P<entity>[\w-_]+)/(?P<environment>[\w-_]+)/(?P<project>[\w-_]+)/(?P<dataset>[\w_]+)/(?P<table>[\w_]+).yaml`)

//MultiTenancyPathResolver is path resolver for tolerances spec in multi tenancy configuration
//the path format is {environment}/{entity}/{project-id}/{dataset}/{tablename}.yaml
type MultiTenancyPathResolver struct {
	entityStore protocol.EntityStore
}

func (p *MultiTenancyPathResolver) GetURN(filePath string) (string, error) {
	matches := multiTenancyPathPattern.FindStringSubmatch(filePath)
	if len(matches) == 0 {
		return "", errors.New("wrong path format")
	}
	label := &protocol.Label{
		Project: matches[3],
		Dataset: matches[4],
		Table:   matches[5],
	}

	return label.String(), nil
}

func (p *MultiTenancyPathResolver) GetPath(urn string) (string, error) {
	label, err := protocol.ParseLabel(urn)
	if err != nil {
		return "", err
	}

	entities, err := p.entityStore.GetAll()
	if err != nil {
		return "", err
	}

	entity, err := protocol.EntityFinder(entities).FindByProjectID(label.Project)
	if err != nil {
		return "", err
	}

	return multiTenancyPath(entity, label)
}

func multiTenancyPath(entity *protocol.Entity, lb *protocol.Label) (string, error) {
	shortFilename := fmt.Sprintf("%s.yaml", lb.Table)
	return fmt.Sprintf("%s/%s/%s/%s/%s", entity.Environment, entity.Name, lb.Project, lb.Dataset, shortFilename), nil
}

var gitPathPattern = regexp.MustCompile(`(?P<project>[\w-_]+)/(?P<dataset>[\w_]+)/(?P<table>[\w_]+).yaml`)

//GitPathResolver is path resolver for tolerances specs in git repository
// the path format is {project-id}/{dataset}/{tablename}.yaml
type GitPathResolver struct {
}

func (g *GitPathResolver) GetURN(filePath string) (string, error) {
	matches := gitPathPattern.FindStringSubmatch(filePath)
	if len(matches) == 0 {
		return "", fmt.Errorf("wrong path format %s", filePath)
	}

	label := &protocol.Label{
		Project: matches[1],
		Dataset: matches[2],
		Table:   matches[3],
	}

	return label.String(), nil
}

func (g *GitPathResolver) GetPath(urn string) (string, error) {
	label, err := protocol.ParseLabel(urn)
	if err != nil {
		return "", err
	}

	return gitPath(label)
}

func gitPath(lb *protocol.Label) (string, error) {
	shortFilename := fmt.Sprintf("%s.yaml", lb.Table)
	return fmt.Sprintf("%s/%s/%s", lb.Project, lb.Dataset, shortFilename), nil
}
