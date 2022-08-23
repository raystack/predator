package tolerance

import (
	"bytes"
	"cloud.google.com/go/storage"
	"context"
	"errors"
	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"github.com/odpf/predator/protocol"
	"google.golang.org/api/iterator"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path/filepath"
)

const defaultFilePerm = 0700

var defaultLogger = log.New(os.Stdout, "INFO: ", log.LstdFlags)

var errNotSupported = errors.New("operation not supported")

//DefaultFileStoreFactory to create specific FileBasedToleranceRepository
//from URL path
type DefaultFileStoreFactory struct {
	client stiface.Client
}

//NewFileStoreFactory to create FileBasedToleranceRepository based on URL path
func NewFileStoreFactory(client stiface.Client) *DefaultFileStoreFactory {
	return &DefaultFileStoreFactory{
		client: client,
	}
}

//Create to create FileBasedToleranceRepository based on URL path
func (f *DefaultFileStoreFactory) Create(URL string) (protocol.FileStore, error) {
	u, err := url.Parse(URL)

	if err != nil {
		return nil, err
	}

	if u.Scheme == "gs" {
		return NewGcsRepository(f.client, URL), nil
	}

	return NewLocalRepository(URL), nil
}

//GcsFileStorage is FileStore that use google cloud storage
//every file placed under basePath directory
//please always resolve filepath with basePath to get the fullPath
type GcsFileStorage struct {
	client   stiface.Client
	bucket   stiface.BucketHandle
	basePath string
}

//NewGcsRepository to create GcsFileStorage
func NewGcsRepository(client stiface.Client, gcsPath string) *GcsFileStorage {
	URL, _ := url.Parse(gcsPath)
	bucketName := URL.Host
	bucketHandle := client.Bucket(bucketName)
	basePath, _ := filepath.Rel("/", URL.Path)
	return &GcsFileStorage{
		client:   client,
		bucket:   bucketHandle,
		basePath: basePath,
	}
}

//GetPaths get all files path
//only support files that has .yaml extension
func (r *GcsFileStorage) GetPaths() ([]string, error) {
	q := &storage.Query{
		Prefix: r.basePath,
	}
	it := r.bucket.Objects(context.Background(), q)

	var objects []*storage.ObjectAttrs
	for {
		objAttrs, err := it.Next()
		if err != nil && err != iterator.Done {
			return nil, err
		}

		if err == iterator.Done {
			break
		}

		ext := filepath.Ext(objAttrs.Name)
		if ext == protocol.Ext {
			objects = append(objects, objAttrs)
		}
	}

	var paths []string
	for _, objectAttrs := range objects {
		fullPath := objectAttrs.Name
		rel, err := filepath.Rel(r.basePath, fullPath)
		if err != nil {
			return nil, err
		}
		paths = append(paths, rel)
	}

	return paths, nil
}

func (r *GcsFileStorage) GetAll() ([]*protocol.File, error) {
	return nil, errNotSupported
}

func (r *GcsFileStorage) Create(file *protocol.File) (err error) {
	fullPath := r.getPath(file.Path)

	defaultLogger.Printf("create file %s ", fullPath)
	writer := r.bucket.Object(fullPath).NewWriter(context.Background())
	defer func() {
		cErr := writer.Close()
		if err == nil {
			err = cErr
		}
	}()

	_, err = writer.Write(file.Content)
	return err
}

func (r *GcsFileStorage) Delete(filePath string) error {
	fullPath := r.getPath(filePath)
	err := r.bucket.Object(fullPath).Delete(context.Background())
	if err == storage.ErrObjectNotExist {
		return protocol.ErrFileNotFound
	}
	return err
}

func (r *GcsFileStorage) getPath(fileName string) string {
	return filepath.Join(r.basePath, fileName)
}

//GetFile to read a file from google cloud storage
func (r *GcsFileStorage) Get(filePath string) (file *protocol.File, err error) {
	fullPath := r.getPath(filePath)
	objHandle := r.bucket.Object(fullPath)

	reader, err := objHandle.NewReader(context.Background())
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return nil, protocol.ErrFileNotFound
		}
		return nil, err
	}

	defer func() {
		cErr := reader.Close()
		if err == nil {
			err = cErr
		}
	}()

	var b bytes.Buffer
	if _, err := b.ReadFrom(reader); err != nil {
		return nil, err
	}

	return &protocol.File{
		Path:    filePath,
		Content: b.Bytes(),
	}, err
}

//LocalFileStorage is a FileBasedToleranceRepository that use local file as tolerance
//every files in this storage is located under baseDir
//always resolve the path with base dir for any operation
type LocalFileStorage struct {
	baseDir string
}

func (l *LocalFileStorage) GetPaths() ([]string, error) {
	var fullPaths []string
	if err := filepath.Walk(l.baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		if name := info.Name(); filepath.Ext(name) == protocol.Ext {
			fullPaths = append(fullPaths, path)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	var paths []string
	for _, filePath := range fullPaths {
		rel, err := filepath.Rel(l.baseDir, filePath)
		if err != nil {
			return nil, err
		}
		paths = append(paths, rel)
	}

	return paths, nil
}

//NewLocalRepository is constructor of LocalFileStorage
func NewLocalRepository(baseDir string) *LocalFileStorage {
	return &LocalFileStorage{
		baseDir: baseDir,
	}
}

//GetAll read all files that managed by storage
//by convention only file with .yaml files will be returned
func (l *LocalFileStorage) GetAll() ([]*protocol.File, error) {
	var filePaths []string
	if err := filepath.Walk(l.baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		if name := info.Name(); filepath.Ext(name) == protocol.Ext {
			filePaths = append(filePaths, path)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	var files []*protocol.File
	for _, filePath := range filePaths {
		content, err := ioutil.ReadFile(filePath)
		if err != nil {
			return nil, err
		}

		rel, err := filepath.Rel(l.baseDir, filePath)
		if err != nil {
			return nil, err
		}

		file := &protocol.File{
			Path:    rel,
			Content: content,
		}

		files = append(files, file)
	}

	return files, nil
}

func (l *LocalFileStorage) Create(file *protocol.File) error {
	fullPath := filepath.Join(l.baseDir, file.Path)
	dirPath := filepath.Dir(fullPath)

	_, err := os.Stat(dirPath)
	if os.IsNotExist(err) {
		if err = os.MkdirAll(dirPath, defaultFilePerm); err != nil {
			return err
		}
	}

	defaultLogger.Printf("create file %s ", fullPath)
	return ioutil.WriteFile(fullPath, file.Content, defaultFilePerm)
}

func (l *LocalFileStorage) Delete(filePath string) error {
	fullPath := filepath.Join(l.baseDir, filePath)
	defaultLogger.Printf("create file %s ", fullPath)

	if err := os.Remove(fullPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return protocol.ErrFileNotFound
		}
		return err
	}

	dirPath := filepath.Dir(fullPath)

	fileInfos, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return err
	}

	if len(fileInfos) == 0 {
		return os.RemoveAll(dirPath)
	}

	return nil
}

//Get to read file from local directory
func (l *LocalFileStorage) Get(filePath string) (*protocol.File, error) {
	fullPath := filepath.Join(l.baseDir, filePath)

	content, err := ioutil.ReadFile(fullPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, protocol.ErrFileNotFound
		}
		return nil, err
	}

	return &protocol.File{
		Path:    filePath,
		Content: content,
	}, err
}
