package tolerance

import (
	"errors"
	"fmt"
	predatormock "github.com/odpf/predator/mock"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/metric"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

type pathResolverMock struct {
	mock.Mock
}

func (p *pathResolverMock) GetURN(filePath string) (string, error) {
	args := p.Called(filePath)

	return args.String(0), args.Error(1)
}

func (p *pathResolverMock) GetPath(urn string) (string, error) {
	args := p.Called(urn)

	return args.String(0), args.Error(1)
}

type parserMock struct {
	Parser
	mock.Mock
}

func (p *parserMock) Parse(content []byte) (*protocol.ToleranceSpec, error) {
	args := p.Called(content)

	return args.Get(0).(*protocol.ToleranceSpec), args.Error(1)
}

func (p *parserMock) Serialise(toleranceSpec *protocol.ToleranceSpec) ([]byte, error) {
	args := p.Called(toleranceSpec)
	return args.Get(0).([]byte), args.Error(1)
}

func TestToleranceStore(t *testing.T) {
	t.Run("GetByTableID", func(t *testing.T) {
		t.Run("should return tolerance given tableID", func(t *testing.T) {
			tableID := "project.dataset.table"
			resolverPath := fmt.Sprintf("%s.yaml", tableID)

			file := &protocol.File{
				Path:    resolverPath,
				Content: []byte(content),
			}

			tolerances := []*protocol.Tolerance{
				{
					TableURN:   tableID,
					MetricName: metric.DuplicationPct,
					ToleranceRules: []protocol.ToleranceRule{
						{
							Comparator: protocol.ComparatorLessThanEq,
							Value:      0.0,
						},
					},
				},
				{
					TableURN:   tableID,
					FieldID:    "sample_field",
					MetricName: metric.NullnessPct,
					ToleranceRules: []protocol.ToleranceRule{
						{
							Comparator: protocol.ComparatorLessThanEq,
							Value:      10.0,
						},
					},
				},
			}

			toleranceSpec := &protocol.ToleranceSpec{
				URN:        tableID,
				Tolerances: tolerances,
			}

			resolver := &pathResolverMock{}
			defer resolver.AssertExpectations(t)

			fileStore := predatormock.NewMockFileStore()
			defer fileStore.AssertExpectations(t)

			parser := &parserMock{}
			defer parser.AssertExpectations(t)

			resolver.On("GetPath", tableID).Return(resolverPath, nil)
			fileStore.On("Get", resolverPath).Return(file, nil)
			parser.On("Parse", []byte(content)).Return(toleranceSpec, nil)

			toleranceStore := NewFileBasedStore(fileStore, resolver, parser)

			result, err := toleranceStore.GetByTableID(tableID)

			assert.Nil(t, err)
			assert.Equal(t, toleranceSpec, result)
		})
		t.Run("should return ErrToleranceNotFound when no tolerance file found", func(t *testing.T) {
			tableID := "project.dataset.table"
			resolverPath := fmt.Sprintf("%s.yaml", tableID)

			resolver := &pathResolverMock{}
			defer resolver.AssertExpectations(t)

			fileStore := predatormock.NewMockFileStore()
			defer fileStore.AssertExpectations(t)

			parser := &parserMock{}
			defer parser.AssertExpectations(t)

			resolver.On("GetPath", tableID).Return(resolverPath, nil)
			fileStore.On("Get", resolverPath).Return(&protocol.File{}, protocol.ErrFileNotFound)

			toleranceStore := NewFileBasedStore(fileStore, resolver, parser)

			_, err := toleranceStore.GetByTableID(tableID)

			assert.Error(t, err)
			assert.True(t, errors.Is(err, protocol.ErrToleranceNotFound))
		})
		t.Run("should return error when file resolve failed failed", func(t *testing.T) {
			tableID := "project.dataset.table"

			namingErr := errors.New("naming error")

			resolver := &pathResolverMock{}
			defer resolver.AssertExpectations(t)

			fileStore := predatormock.NewMockFileStore()
			defer fileStore.AssertExpectations(t)

			parser := &parserMock{}
			defer parser.AssertExpectations(t)

			resolver.On("GetPath", tableID).Return("", namingErr)

			toleranceStore := NewFileBasedStore(fileStore, resolver, parser)

			_, err := toleranceStore.GetByTableID(tableID)

			assert.Equal(t, namingErr, err)
		})
		t.Run("should return error when parsing failed", func(t *testing.T) {
			tableID := "project.dataset.table"
			resolverPath := fmt.Sprintf("%s.yaml", tableID)

			file := &protocol.File{
				Path:    resolverPath,
				Content: []byte(content),
			}

			parseErr := errors.New("parse err")

			resolver := &pathResolverMock{}
			defer resolver.AssertExpectations(t)

			fileStore := predatormock.NewMockFileStore()
			defer fileStore.AssertExpectations(t)

			parser := &parserMock{}
			defer parser.AssertExpectations(t)

			resolver.On("GetPath", tableID).Return(resolverPath, nil)
			fileStore.On("Get", resolverPath).Return(file, nil)
			var toleranceSpec *protocol.ToleranceSpec
			parser.On("Parse", []byte(content)).Return(toleranceSpec, parseErr)

			toleranceStore := NewFileBasedStore(fileStore, resolver, parser)

			_, err := toleranceStore.GetByTableID(tableID)

			assert.Error(t, err)
			assert.True(t, errors.Is(err, parseErr))
		})
		t.Run("should return error when get file failed", func(t *testing.T) {
			tableID := "project.dataset.table"
			resolverPath := fmt.Sprintf("%s.yaml", tableID)

			ioErr := errors.New("parse err")

			resolver := &pathResolverMock{}
			defer resolver.AssertExpectations(t)

			fileStore := predatormock.NewMockFileStore()
			defer fileStore.AssertExpectations(t)

			parser := &parserMock{}
			defer parser.AssertExpectations(t)

			resolver.On("GetPath", tableID).Return(resolverPath, nil)
			fileStore.On("Get", resolverPath).Return(&protocol.File{}, ioErr)

			toleranceStore := NewFileBasedStore(fileStore, resolver, parser)

			_, err := toleranceStore.GetByTableID(tableID)

			assert.Error(t, err)
			assert.True(t, errors.Is(err, ioErr))
		})
	})
	t.Run("Create", func(t *testing.T) {
		t.Run("should return create tolerance spec", func(t *testing.T) {
			tableID := "project.dataset.table"
			spec := &protocol.ToleranceSpec{
				URN: tableID,
				Tolerances: []*protocol.Tolerance{
					{},
				},
			}
			filePath := "project.dataset.table.yaml"

			content := []byte("{}")
			file := &protocol.File{
				Path:    filePath,
				Content: content,
			}

			resolver := &pathResolverMock{}
			defer resolver.AssertExpectations(t)

			parser := &parserMock{}
			defer parser.AssertExpectations(t)

			fileStore := predatormock.NewMockFileStore()
			defer fileStore.AssertExpectations(t)

			resolver.On("GetPath", tableID).Return(filePath, nil)
			parser.On("Serialise", spec).Return(content, nil)
			fileStore.On("Create", file).Return(nil)

			toleranceStore := NewFileBasedStore(fileStore, resolver, parser)
			err := toleranceStore.Create(spec)

			assert.Nil(t, err)
		})
		t.Run("should return err when unable to get path", func(t *testing.T) {
			tableID := "project.dataset"
			spec := &protocol.ToleranceSpec{
				URN: tableID,
				Tolerances: []*protocol.Tolerance{
					{},
				},
			}

			nameErr := errors.New("naming format error")

			resolver := &pathResolverMock{}
			defer resolver.AssertExpectations(t)

			parser := &parserMock{}
			defer parser.AssertExpectations(t)

			fileStore := predatormock.NewMockFileStore()
			defer fileStore.AssertExpectations(t)

			resolver.On("GetPath", tableID).Return("", nameErr)

			toleranceStore := NewFileBasedStore(fileStore, resolver, parser)
			err := toleranceStore.Create(spec)

			assert.Equal(t, nameErr, err)
		})
		t.Run("should return err serialise fail", func(t *testing.T) {
			tableID := "project.dataset.table"
			spec := &protocol.ToleranceSpec{
				URN: tableID,
				Tolerances: []*protocol.Tolerance{
					{},
				},
			}
			filePath := "project.dataset.table.yaml"

			serialErr := errors.New("serialization err")

			resolver := &pathResolverMock{}
			defer resolver.AssertExpectations(t)

			parser := &parserMock{}
			defer parser.AssertExpectations(t)

			fileStore := predatormock.NewMockFileStore()
			defer fileStore.AssertExpectations(t)

			resolver.On("GetPath", tableID).Return(filePath, nil)
			var b []byte
			parser.On("Serialise", spec).Return(b, serialErr)

			toleranceStore := NewFileBasedStore(fileStore, resolver, parser)
			err := toleranceStore.Create(spec)

			assert.Error(t, err)
			assert.True(t, errors.Is(err, serialErr))

		})
		t.Run("should return err when file creation fail", func(t *testing.T) {
			tableID := "project.dataset.table"
			spec := &protocol.ToleranceSpec{
				URN: tableID,
				Tolerances: []*protocol.Tolerance{
					{},
				},
			}
			filePath := "project.dataset.table.yaml"

			content := []byte("{}")
			file := &protocol.File{
				Path:    filePath,
				Content: content,
			}

			ioErr := errors.New("io error")

			resolver := &pathResolverMock{}
			defer resolver.AssertExpectations(t)

			parser := &parserMock{}
			defer parser.AssertExpectations(t)

			fileStore := predatormock.NewMockFileStore()
			defer fileStore.AssertExpectations(t)

			resolver.On("GetPath", tableID).Return(filePath, nil)
			parser.On("Serialise", spec).Return(content, nil)
			fileStore.On("Create", file).Return(ioErr)

			toleranceStore := NewFileBasedStore(fileStore, resolver, parser)
			err := toleranceStore.Create(spec)

			assert.Error(t, err)
			assert.True(t, errors.Is(err, ioErr))
		})
	})
	t.Run("Delete", func(t *testing.T) {
		t.Run("should delete tolerance spec", func(t *testing.T) {
			tableID := "project.dataset.table"
			filePath := "project.dataset.table.yaml"

			resolver := &pathResolverMock{}
			defer resolver.AssertExpectations(t)

			parser := &parserMock{}
			defer parser.AssertExpectations(t)

			fileStore := predatormock.NewMockFileStore()
			defer fileStore.AssertExpectations(t)

			resolver.On("GetPath", tableID).Return(filePath, nil)
			fileStore.On("Delete", filePath).Return(nil)

			toleranceStore := NewFileBasedStore(fileStore, resolver, parser)
			err := toleranceStore.Delete(tableID)

			assert.Nil(t, err)
		})
		t.Run("should return err when unable to get path", func(t *testing.T) {
			tableID := "project.dataset.table"

			nameErr := errors.New("naming error")

			resolver := &pathResolverMock{}
			defer resolver.AssertExpectations(t)

			parser := &parserMock{}
			defer parser.AssertExpectations(t)

			fileStore := predatormock.NewMockFileStore()
			defer fileStore.AssertExpectations(t)

			resolver.On("GetPath", tableID).Return("", nameErr)

			toleranceStore := NewFileBasedStore(fileStore, resolver, parser)
			err := toleranceStore.Delete(tableID)

			assert.Equal(t, nameErr, err)
		})
		t.Run("should return ErrToleranceNotFound when no tolerance file found", func(t *testing.T) {
			tableID := "project.dataset.table"
			filePath := "project.dataset.table.yaml"

			resolver := &pathResolverMock{}
			defer resolver.AssertExpectations(t)

			parser := &parserMock{}
			defer parser.AssertExpectations(t)

			fileStore := predatormock.NewMockFileStore()
			defer fileStore.AssertExpectations(t)

			resolver.On("GetPath", tableID).Return(filePath, nil)
			fileStore.On("Delete", filePath).Return(protocol.ErrFileNotFound)

			toleranceStore := NewFileBasedStore(fileStore, resolver, parser)
			err := toleranceStore.Delete(tableID)

			assert.NotNil(t, err)
		})
	})
	t.Run("GetAll", func(t *testing.T) {
		t.Run("should return all tolerance specs", func(t *testing.T) {
			files := []*protocol.File{
				{
					Path:    "project-1.dataset_a.table_x.yaml",
					Content: []byte("{team_a}"),
				},
				{
					Path:    "project-1.dataset_b.table_x.yaml",
					Content: []byte("{team_b}"),
				},
			}

			toleranceSpec := []*protocol.ToleranceSpec{
				{
					URN:        "project-1.dataset_a.table_x",
					Tolerances: []*protocol.Tolerance{},
				},
				{
					URN:        "project-1.dataset_b.table_x",
					Tolerances: []*protocol.Tolerance{},
				},
			}

			resolver := &pathResolverMock{}
			defer resolver.AssertExpectations(t)

			parser := &parserMock{}
			defer parser.AssertExpectations(t)

			fileStore := predatormock.NewMockFileStore()
			defer fileStore.AssertExpectations(t)

			fileStore.On("GetAll").Return(files, nil)

			for i, file := range files {
				resolver.On("GetURN", file.Path).Return(toleranceSpec[i].URN, nil)
				parser.On("Parse", file.Content).Return(toleranceSpec[i], nil)
			}

			toleranceStore := NewFileBasedStore(fileStore, resolver, parser)
			result, err := toleranceStore.GetAll()

			assert.Nil(t, err)
			assert.Equal(t, toleranceSpec, result)
		})
		t.Run("should return error when unable to get files", func(t *testing.T) {
			ioErr := errors.New("unable to read file")

			resolver := &pathResolverMock{}
			defer resolver.AssertExpectations(t)

			parser := &parserMock{}
			defer parser.AssertExpectations(t)

			fileStore := predatormock.NewMockFileStore()
			defer fileStore.AssertExpectations(t)

			var files []*protocol.File
			fileStore.On("GetAll").Return(files, ioErr)

			toleranceStore := NewFileBasedStore(fileStore, resolver, parser)
			_, err := toleranceStore.GetAll()

			assert.Equal(t, ioErr, err)
		})
		t.Run("should return error when file resolve failed failed", func(t *testing.T) {
			namingErr := errors.New("name format error")
			files := []*protocol.File{
				{
					Path:    "project-1.dataset_a.yaml",
					Content: []byte("{team_a}"),
				},
				{
					Path:    "project-1.dataset_b.yaml",
					Content: []byte("{team_b}"),
				},
			}

			resolver := &pathResolverMock{}
			defer resolver.AssertExpectations(t)

			parser := &parserMock{}
			defer parser.AssertExpectations(t)

			fileStore := predatormock.NewMockFileStore()
			defer fileStore.AssertExpectations(t)

			fileStore.On("GetAll").Return(files, nil)
			resolver.On("GetURN", files[0].Path).Return("", namingErr)

			toleranceStore := NewFileBasedStore(fileStore, resolver, parser)
			_, err := toleranceStore.GetAll()

			assert.Equal(t, namingErr, err)
		})
		t.Run("should return error when parsing failed", func(t *testing.T) {
			files := []*protocol.File{
				{
					Path:    "project-1.dataset_a.table_x.yaml",
					Content: []byte("{team_a}"),
				},
				{
					Path:    "project-1.dataset_b.table_x.yaml",
					Content: []byte("{team_b}"),
				},
			}

			toleranceSpec := []*protocol.ToleranceSpec{
				{
					URN:        "project-1.dataset_a.table_x",
					Tolerances: []*protocol.Tolerance{},
				},
				{
					URN:        "project-1.dataset_b.table_x",
					Tolerances: []*protocol.Tolerance{},
				},
			}

			resolver := &pathResolverMock{}
			defer resolver.AssertExpectations(t)

			parser := &parserMock{}
			defer parser.AssertExpectations(t)

			fileStore := predatormock.NewMockFileStore()
			defer fileStore.AssertExpectations(t)

			fileStore.On("GetAll").Return(files, nil)

			resolver.On("GetURN", files[0].Path).Return(toleranceSpec[0].URN, nil)
			parseErr := errors.New("wrong format")
			var spec *protocol.ToleranceSpec
			parser.On("Parse", files[0].Content).Return(spec, parseErr)

			toleranceStore := NewFileBasedStore(fileStore, resolver, parser)
			_, err := toleranceStore.GetAll()

			assert.Error(t, err)
			assert.True(t, errors.Is(err, parseErr))
		})
	})
	t.Run("GetByProjectID", func(t *testing.T) {
		t.Run("should return tolerance specs given tableID", func(t *testing.T) {
			paths := []string{
				"project-1.dataset_a.table_x.yaml",
				"project-1.dataset_b.table_x.yaml",
				"project-2.dataset_a.table_x.yaml",
			}

			urns := []string{
				"project-1.dataset_a.table_x",
				"project-1.dataset_b.table_x",
				"project-2.dataset_a.table_x",
			}

			files := []*protocol.File{
				{
					Path:    "project-1.dataset_a.table_x.yaml",
					Content: []byte("{team_a}"),
				},
				{
					Path:    "project-1.dataset_b.table_x.yaml",
					Content: []byte("{team_b}"),
				},
			}

			toleranceSpec := []*protocol.ToleranceSpec{
				{
					URN:        "project-1.dataset_a.table_x",
					Tolerances: []*protocol.Tolerance{},
				},
				{
					URN:        "project-1.dataset_b.table_x",
					Tolerances: []*protocol.Tolerance{},
				},
			}

			resolver := &pathResolverMock{}
			defer resolver.AssertExpectations(t)

			parser := &parserMock{}
			defer parser.AssertExpectations(t)

			fileStore := predatormock.NewMockFileStore()
			defer fileStore.AssertExpectations(t)

			fileStore.On("GetPaths").Return(paths, nil)

			for i, p := range paths {
				resolver.On("GetURN", p).Return(urns[i], nil)
			}

			for i, file := range files {
				fileStore.On("Get", paths[i]).Return(files[i], nil)
				parser.On("Parse", file.Content).Return(toleranceSpec[i], nil)
			}

			toleranceStore := NewFileBasedStore(fileStore, resolver, parser)
			result, err := toleranceStore.GetByProjectID("project-1")

			assert.Nil(t, err)
			assert.Equal(t, toleranceSpec, result)
		})
		t.Run("should return error when unable to get file paths", func(t *testing.T) {
			resolver := &pathResolverMock{}
			defer resolver.AssertExpectations(t)

			parser := &parserMock{}
			defer parser.AssertExpectations(t)

			fileStore := predatormock.NewMockFileStore()
			defer fileStore.AssertExpectations(t)

			apiErr := errors.New("api error")
			var ps []string
			fileStore.On("GetPaths").Return(ps, apiErr)

			toleranceStore := NewFileBasedStore(fileStore, resolver, parser)
			_, err := toleranceStore.GetByProjectID("project-1")

			assert.Equal(t, apiErr, err)
		})
		t.Run("should return error when get file failed", func(t *testing.T) {
			paths := []string{
				"project-1.dataset_a.table_x.yaml",
				"project-1.dataset_b.table_x.yaml",
				"project-2.dataset_a.table_x.yaml",
			}

			urns := []string{
				"project-1.dataset_a.table_x",
				"project-1.dataset_b.table_x",
				"project-2.dataset_a.table_x",
			}

			resolver := &pathResolverMock{}
			defer resolver.AssertExpectations(t)

			parser := &parserMock{}
			defer parser.AssertExpectations(t)

			fileStore := predatormock.NewMockFileStore()
			defer fileStore.AssertExpectations(t)

			fileStore.On("GetPaths").Return(paths, nil)

			resolver.On("GetURN", paths[0]).Return(urns[0], nil)

			var f *protocol.File
			apiErr := errors.New("api error")
			fileStore.On("Get", paths[0]).Return(f, apiErr)

			toleranceStore := NewFileBasedStore(fileStore, resolver, parser)
			_, err := toleranceStore.GetByProjectID("project-1")

			assert.Equal(t, apiErr, err)
		})
		t.Run("should return error when parsing failed", func(t *testing.T) {
			paths := []string{
				"project-1.dataset_a.table_x.yaml",
				"project-1.dataset_b.table_x.yaml",
				"project-2.dataset_a.table_x.yaml",
			}

			urns := []string{
				"project-1.dataset_a.table_x",
				"project-1.dataset_b.table_x",
				"project-2.dataset_a.table_x",
			}

			files := []*protocol.File{
				{
					Path:    "project-1.dataset_a.table_x.yaml",
					Content: []byte("{team-a}"),
				},
				{
					Path:    "project-1.dataset_b.table_x.yaml",
					Content: []byte("{team-b}"),
				},
			}

			resolver := &pathResolverMock{}
			defer resolver.AssertExpectations(t)

			parser := &parserMock{}
			defer parser.AssertExpectations(t)

			fileStore := predatormock.NewMockFileStore()
			defer fileStore.AssertExpectations(t)

			fileStore.On("GetPaths").Return(paths, nil)

			resolver.On("GetURN", paths[0]).Return(urns[0], nil)
			fileStore.On("Get", paths[0]).Return(files[0], nil)
			var tls *protocol.ToleranceSpec
			parseErr := errors.New("wrong format")
			parser.On("Parse", files[0].Content).Return(tls, parseErr)

			toleranceStore := NewFileBasedStore(fileStore, resolver, parser)
			_, err := toleranceStore.GetByProjectID("project-1")

			assert.Equal(t, parseErr, err)
		})
	})
	t.Run("GetResourceNames", func(t *testing.T) {
		t.Run("should return all resource names", func(t *testing.T) {
			paths := []string{
				"project-1.dataset_a.table_x.yaml",
				"project-1.dataset_b.table_x.yaml",
			}

			urns := []string{
				"project-1.dataset_a.table_x",
				"project-1.dataset_b.table_x",
			}

			resolver := &pathResolverMock{}
			defer resolver.AssertExpectations(t)

			parser := &parserMock{}
			defer parser.AssertExpectations(t)

			fileStore := predatormock.NewMockFileStore()
			defer fileStore.AssertExpectations(t)

			fileStore.On("GetPaths").Return(paths, nil)

			for i, path := range paths {
				resolver.On("GetURN", path).Return(urns[i], nil)
			}

			toleranceStore := NewFileBasedStore(fileStore, resolver, parser)
			result, err := toleranceStore.GetResourceNames()

			assert.Nil(t, err)
			assert.Equal(t, urns, result)
		})
		t.Run("should return error when unable to get paths", func(t *testing.T) {
			ioErr := errors.New("unable to read file")

			resolver := &pathResolverMock{}
			defer resolver.AssertExpectations(t)

			parser := &parserMock{}
			defer parser.AssertExpectations(t)

			fileStore := predatormock.NewMockFileStore()
			defer fileStore.AssertExpectations(t)

			var files []*protocol.File
			fileStore.On("GetAll").Return(files, ioErr)

			toleranceStore := NewFileBasedStore(fileStore, resolver, parser)
			_, err := toleranceStore.GetAll()

			assert.Equal(t, ioErr, err)
		})
		t.Run("should return error when get urn failed", func(t *testing.T) {
			namingErr := errors.New("name format error")
			files := []*protocol.File{
				{
					Path:    "dataset_a.table_x.yaml",
					Content: []byte("{team_a}"),
				},
				{
					Path:    "dataset_b.table_x.yaml",
					Content: []byte("{team_b}"),
				},
			}

			resolver := &pathResolverMock{}
			defer resolver.AssertExpectations(t)

			parser := &parserMock{}
			defer parser.AssertExpectations(t)

			fileStore := predatormock.NewMockFileStore()
			defer fileStore.AssertExpectations(t)

			fileStore.On("GetAll").Return(files, nil)
			resolver.On("GetURN", files[0].Path).Return("", namingErr)

			toleranceStore := NewFileBasedStore(fileStore, resolver, parser)
			_, err := toleranceStore.GetAll()

			assert.Equal(t, namingErr, err)
		})
	})
}

var content = `- tableid: "project.dataset.table"
  fieldid: ""
  metricname: "duplication_pct"
  tolerancerules:
    less_than_eq: 0.0
- tableid: "project.dataset.table"
  fieldid: "sample_field"
  metricname: "nullness_pct"
  tolerancerules:
    less_than_eq: 10.0`
