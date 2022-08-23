package protocol

import (
	"errors"
	"fmt"
	"regexp"
)

//Ext is default yaml file extension
var Ext = ".yaml"

//File a file
type File struct {
	Path    string
	Content []byte
}

var ErrFileNotFound = errors.New("file not found")

//FileStore is storage that contains tolerance spec in a file
type FileStore interface {
	Get(filePath string) (*File, error)
	GetAll() ([]*File, error)
	GetPaths() ([]string, error)
	Create(file *File) error
	Delete(filePath string) error
}

//FileStoreFactory is creator of FileStore
type FileStoreFactory interface {
	Create(URL string) (FileStore, error)
}

//PathType path of directory structure of a FileStore that is supported
type PathType string

const (
	//Git is path type that used to resolve git directory structure
	Git PathType = "git"
	//MultiTenancy is path type that used to resolve multi tenancy directory structure
	MultiTenancy PathType = "multi_tenancy"
	//Default simple path type, mapping from project.dataset.table to project.dataset.table.yaml
	Default PathType = "default"
)

//PathResolver to get path from resource name with possibility of multiple layout
type PathResolver interface {
	GetPath(urn string) (string, error)
	GetURN(filePath string) (string, error)
}

var labelPattern = regexp.MustCompile(`(?P<project>[\w-_]+)\.(?P<dataset>[\w_]+)\.(?P<table>[\w_]+)`)

//Label is structured bigquery resource identifier
type Label struct {
	Project string
	Dataset string
	Table   string
}

func (l *Label) String() string {
	return fmt.Sprintf("%s.%s.%s", l.Project, l.Dataset, l.Table)
}

//ParseLabel create Label from string formatted bigquery fully qualified identifier
func ParseLabel(URN string) (*Label, error) {
	el := labelPattern.FindStringSubmatch(URN)

	if len(el) == 0 {
		return nil, errors.New("wrong URN format")
	}

	return &Label{
		Project: el[1],
		Dataset: el[2],
		Table:   el[3],
	}, nil
}
