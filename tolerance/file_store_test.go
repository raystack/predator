package tolerance

import (
	"bytes"
	"cloud.google.com/go/storage"
	"context"
	"errors"
	"fmt"
	"github.com/odpf/predator/protocol"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFileStoreFactory(t *testing.T) {
	t.Run("Create", func(t *testing.T) {
		t.Run("should create GCS tolerance repository given gcs path", func(t *testing.T) {
			bucket := &storageBucketMock{}

			client := &storageClientMock{}
			defer client.AssertExpectations(t)

			client.On("Bucket", "bucket").Return(bucket)

			factory := NewFileStoreFactory(client)
			repo, err := factory.Create("gs://bucket/abcd")

			_, ok := repo.(*GcsFileStorage)
			assert.True(t, ok)
			assert.Nil(t, err)
		})
		t.Run("should create LocalFile repository given other than gcs path", func(t *testing.T) {
			factory := NewFileStoreFactory(nil)
			repo, err := factory.Create("/etc/conf/tolerance")

			_, ok := repo.(*LocalFileStorage)
			assert.True(t, ok)
			assert.Nil(t, err)
		})
	})
}

func TestGCSFileStorage(t *testing.T) {
	t.Run("Create", func(t *testing.T) {
		t.Run("should create file", func(t *testing.T) {
			tableID := "project.dataset.table"
			bucket := "abcdbucket"
			path := "def"
			gcsPath := fmt.Sprintf("gs://%s/%s", bucket, path)
			filePath := fmt.Sprintf("%s/%s.yaml", path, tableID)
			resolverPath := fmt.Sprintf("%s.yaml", tableID)

			file := &protocol.File{
				Path:    resolverPath,
				Content: []byte(compactSpecYamlFileContent),
			}

			dest := new(bytes.Buffer)
			writer := &objectWriterMock{}
			defer writer.AssertExpectations(t)
			writer.On("Write").Return(dest, nil)
			writer.On("Close").Return(nil)

			obj := &objectHandleMock{}
			defer obj.AssertExpectations(t)
			obj.On("NewWriter", context.Background()).Return(writer, nil)

			bucketHandle := &storageBucketMock{}
			defer bucketHandle.AssertExpectations(t)
			bucketHandle.On("Object", filePath).Return(obj)

			client := &storageClientMock{}
			defer client.AssertExpectations(t)
			client.On("Bucket", bucket).Return(bucketHandle)

			fileRepo := NewGcsRepository(client, gcsPath)
			err := fileRepo.Create(file)

			assert.Nil(t, err)
		})
		t.Run("should return error when write failed", func(t *testing.T) {
			tableID := "project.dataset.table"
			bucket := "abcdbucket"
			path := "def"
			gcsPath := fmt.Sprintf("gs://%s/%s", bucket, path)
			filePath := fmt.Sprintf("%s/%s.yaml", path, tableID)
			resolverPath := fmt.Sprintf("%s.yaml", tableID)

			file := &protocol.File{
				Path:    resolverPath,
				Content: []byte(compactSpecYamlFileContent),
			}
			ioErr := errors.New("io error")

			dest := new(bytes.Buffer)
			writer := &objectWriterMock{}
			defer writer.AssertExpectations(t)
			writer.On("Write").Return(dest, ioErr)
			writer.On("Close").Return(nil)

			obj := &objectHandleMock{}
			defer obj.AssertExpectations(t)
			obj.On("NewWriter", context.Background()).Return(writer, nil)

			bucketHandle := &storageBucketMock{}
			defer bucketHandle.AssertExpectations(t)
			bucketHandle.On("Object", filePath).Return(obj)

			client := &storageClientMock{}
			defer client.AssertExpectations(t)
			client.On("Bucket", bucket).Return(bucketHandle)

			fileRepo := NewGcsRepository(client, gcsPath)
			err := fileRepo.Create(file)

			assert.Error(t, err)
		})
		t.Run("should return error when closing file failed", func(t *testing.T) {
			tableID := "project.dataset.table"
			bucket := "abcdbucket"
			path := "def"
			gcsPath := fmt.Sprintf("gs://%s/%s", bucket, path)
			filePath := fmt.Sprintf("%s/%s.yaml", path, tableID)
			resolverPath := fmt.Sprintf("%s.yaml", tableID)

			file := &protocol.File{
				Path:    resolverPath,
				Content: []byte(compactSpecYamlFileContent),
			}

			ioErr := errors.New("error on closing file")

			dest := new(bytes.Buffer)
			writer := &objectWriterMock{}
			defer writer.AssertExpectations(t)
			writer.On("Write").Return(dest, nil)
			writer.On("Close").Return(ioErr)

			obj := &objectHandleMock{}
			defer obj.AssertExpectations(t)
			obj.On("NewWriter", context.Background()).Return(writer, nil)

			bucketHandle := &storageBucketMock{}
			defer bucketHandle.AssertExpectations(t)
			bucketHandle.On("Object", filePath).Return(obj)

			client := &storageClientMock{}
			defer client.AssertExpectations(t)
			client.On("Bucket", bucket).Return(bucketHandle)

			fileRepo := NewGcsRepository(client, gcsPath)
			err := fileRepo.Create(file)

			assert.Error(t, err)
			assert.True(t, errors.Is(err, ioErr))
		})
	})
	t.Run("Delete", func(t *testing.T) {
		t.Run("should delete file", func(t *testing.T) {
			tableID := "project.dataset.table"
			bucket := "abcdbucket"
			path := "def"
			gcsPath := fmt.Sprintf("gs://%s/%s", bucket, path)
			filePath := fmt.Sprintf("%s/%s.yaml", path, tableID)
			resolverPath := fmt.Sprintf("%s.yaml", tableID)

			obj := &objectHandleMock{}
			defer obj.AssertExpectations(t)

			bucketHandle := &storageBucketMock{}
			defer bucketHandle.AssertExpectations(t)

			client := &storageClientMock{}
			defer client.AssertExpectations(t)

			client.On("Bucket", bucket).Return(bucketHandle)
			bucketHandle.On("Object", filePath).Return(obj)
			obj.On("Delete", context.Background()).Return(nil)

			fileRepo := NewGcsRepository(client, gcsPath)
			err := fileRepo.Delete(resolverPath)

			assert.Nil(t, err)
		})
		t.Run("should return protocol.ErrFileNotFound when file not exist", func(t *testing.T) {
			tableID := "project.dataset.table"
			bucket := "abcdbucket"
			path := "def"
			gcsPath := fmt.Sprintf("gs://%s/%s", bucket, path)
			filePath := fmt.Sprintf("%s/%s.yaml", path, tableID)
			resolverPath := fmt.Sprintf("%s.yaml", tableID)

			obj := &objectHandleMock{}
			defer obj.AssertExpectations(t)

			bucketHandle := &storageBucketMock{}
			defer bucketHandle.AssertExpectations(t)

			client := &storageClientMock{}
			defer client.AssertExpectations(t)

			client.On("Bucket", bucket).Return(bucketHandle)
			bucketHandle.On("Object", filePath).Return(obj)
			obj.On("Delete", context.Background()).Return(storage.ErrObjectNotExist)

			fileRepo := NewGcsRepository(client, gcsPath)
			err := fileRepo.Delete(resolverPath)

			assert.Equal(t, protocol.ErrFileNotFound, err)
		})
	})
	t.Run("GetPaths", func(t *testing.T) {
		t.Run("should return file paths", func(t *testing.T) {
			bucket := "abcdbucket"
			path := "def"
			gcsPath := fmt.Sprintf("gs://%s/%s", bucket, path)
			q := &storage.Query{
				Prefix: path,
			}

			files := []*storage.ObjectAttrs{
				{
					Name: "def/project-1/dataset_a/table_x.yaml",
				},
				{
					Name: "def/project-1/dataset_b/table_x.yaml",
				},
			}

			files = append(files, &storage.ObjectAttrs{
				Name: "def/abcd.git",
			})

			paths := []string{
				"project-1/dataset_a/table_x.yaml",
				"project-1/dataset_b/table_x.yaml",
			}

			objIterator := newMockObjectIterator(files)

			bucketHandle := &storageBucketMock{}
			defer bucketHandle.AssertExpectations(t)

			client := &storageClientMock{}
			defer client.AssertExpectations(t)

			client.On("Bucket", bucket).Return(bucketHandle)
			bucketHandle.On("Objects", context.Background(), q).Return(objIterator)

			fileRepo := NewGcsRepository(client, gcsPath)
			result, err := fileRepo.GetPaths()

			assert.Nil(t, err)
			assert.Equal(t, paths, result)
		})
	})
	t.Run("Get", func(t *testing.T) {
		t.Run("should return file", func(t *testing.T) {
			tableID := "project.dataset.table"
			bucket := "abcdbucket"
			path := "def"
			gcsPath := fmt.Sprintf("gs://%s/%s", bucket, path)
			filePath := fmt.Sprintf("%s/%s.yaml", path, tableID)
			resolverPath := fmt.Sprintf("%s.yaml", tableID)

			src := bytes.NewBufferString(compactSpecYamlFileContent)
			reader := &objectReaderMock{}
			defer reader.AssertExpectations(t)
			reader.On("Read").Return(src, nil)
			reader.On("Close").Return(nil)

			obj := &objectHandleMock{}
			defer obj.AssertExpectations(t)
			obj.On("NewReader", context.Background()).Return(reader, nil)

			bucketHandle := &storageBucketMock{}
			defer bucketHandle.AssertExpectations(t)
			bucketHandle.On("Object", filePath).Return(obj)

			client := &storageClientMock{}
			defer client.AssertExpectations(t)
			client.On("Bucket", bucket).Return(bucketHandle)

			fileRepo := NewGcsRepository(client, gcsPath)
			result, err := fileRepo.Get(resolverPath)

			expected := &protocol.File{
				Path:    resolverPath,
				Content: []byte(compactSpecYamlFileContent),
			}

			assert.Nil(t, err)
			assert.Equal(t, expected, result)
		})
		t.Run("should error when read file failed", func(t *testing.T) {
			tableID := "project.dataset.table"
			bucket := "abcdbucket"
			path := "def"
			gcsPath := fmt.Sprintf("gs://%s/%s", bucket, path)
			filePath := fmt.Sprintf("%s/%s.yaml", path, tableID)
			resolverPath := fmt.Sprintf("%s.yaml", tableID)

			ioErr := errors.New("read file error")

			src := bytes.NewBufferString("random content")
			reader := &objectReaderMock{}
			defer reader.AssertExpectations(t)
			reader.On("Read").Return(src, ioErr)
			reader.On("Close").Return(nil)

			obj := &objectHandleMock{}
			defer obj.AssertExpectations(t)
			obj.On("NewReader", context.Background()).Return(reader, nil)

			bucketHandle := &storageBucketMock{}
			defer bucketHandle.AssertExpectations(t)
			bucketHandle.On("Object", filePath).Return(obj)

			client := &storageClientMock{}
			defer client.AssertExpectations(t)
			client.On("Bucket", bucket).Return(bucketHandle)

			fileRepo := NewGcsRepository(client, gcsPath)
			_, err := fileRepo.Get(resolverPath)

			assert.Error(t, err)
		})
		t.Run("should return error when closing file failed", func(t *testing.T) {
			tableID := "project.dataset.table"
			bucket := "abcdbucket"
			path := "def"
			gcsPath := fmt.Sprintf("gs://%s/%s", bucket, path)
			filePath := fmt.Sprintf("%s/%s.yaml", path, tableID)
			resolverPath := fmt.Sprintf("%s.yaml", tableID)
			ioErr := errors.New("file removed error")

			src := bytes.NewBufferString(compactSpecYamlFileContent)
			reader := &objectReaderMock{}
			defer reader.AssertExpectations(t)
			reader.On("Read").Return(src, nil)
			reader.On("Close").Return(ioErr)

			obj := &objectHandleMock{}
			defer obj.AssertExpectations(t)
			obj.On("NewReader", context.Background()).Return(reader, nil)

			bucketHandle := &storageBucketMock{}
			defer bucketHandle.AssertExpectations(t)
			bucketHandle.On("Object", filePath).Return(obj)

			client := &storageClientMock{}
			defer client.AssertExpectations(t)
			client.On("Bucket", bucket).Return(bucketHandle)

			fileRepo := NewGcsRepository(client, gcsPath)
			_, err := fileRepo.Get(resolverPath)

			assert.Error(t, err)
		})
		t.Run("should return protocol.ErrFileNotFound when file not exist", func(t *testing.T) {
			tableID := "project.dataset.table"
			bucket := "abcdbucket"
			path := "def"
			gcsPath := fmt.Sprintf("gs://%s/%s", bucket, path)
			filePath := fmt.Sprintf("%s/%s.yaml", path, tableID)
			resolverPath := fmt.Sprintf("%s.yaml", tableID)

			reader := &objectReaderMock{}
			defer reader.AssertExpectations(t)

			obj := &objectHandleMock{}
			defer obj.AssertExpectations(t)
			obj.On("NewReader", context.Background()).Return(reader, storage.ErrObjectNotExist)

			bucketHandle := &storageBucketMock{}
			defer bucketHandle.AssertExpectations(t)
			bucketHandle.On("Object", filePath).Return(obj)

			client := &storageClientMock{}
			defer client.AssertExpectations(t)
			client.On("Bucket", bucket).Return(bucketHandle)

			fileRepo := NewGcsRepository(client, gcsPath)
			_, err := fileRepo.Get(resolverPath)

			assert.Equal(t, protocol.ErrFileNotFound, err)
		})
	})
}
