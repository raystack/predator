package tolerance

import (
	"context"
	"google.golang.org/api/iterator"
	"io"

	_ "github.com/jinzhu/gorm/dialects/sqlite"

	"cloud.google.com/go/iam"
	"cloud.google.com/go/storage"
	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"github.com/stretchr/testify/mock"
)

type storageClientMock struct {
	stiface.Client
	mock.Mock
}

func (s *storageClientMock) Bucket(name string) stiface.BucketHandle {
	args := s.Called(name)
	return args.Get(0).(stiface.BucketHandle)
}

func (s *storageClientMock) Buckets(ctx context.Context, projectID string) stiface.BucketIterator {
	panic("not implemented")
}

func (s *storageClientMock) Close() error {
	panic("not implemented")
}

type storageBucketMock struct {
	stiface.BucketHandle
	mock.Mock
}

func (b *storageBucketMock) Create(context.Context, string, *storage.BucketAttrs) error {
	panic("not implemented")
}
func (b *storageBucketMock) Delete(context.Context) error {
	panic("not implemented")
}
func (b *storageBucketMock) DefaultObjectACL() stiface.ACLHandle {
	panic("not implemented")
}
func (b *storageBucketMock) Object(name string) stiface.ObjectHandle {
	args := b.Called(name)
	return args.Get(0).(stiface.ObjectHandle)
}
func (b *storageBucketMock) Attrs(ctx context.Context) (*storage.BucketAttrs, error) {
	args := b.Called(ctx)
	return args.Get(0).(*storage.BucketAttrs), args.Error(1)
}
func (b *storageBucketMock) Update(context.Context, storage.BucketAttrsToUpdate) (*storage.BucketAttrs, error) {
	panic("not implemented")
}
func (b *storageBucketMock) If(storage.BucketConditions) stiface.BucketHandle {
	panic("not implemented")
}

func (b *storageBucketMock) Objects(ctx context.Context, query *storage.Query) stiface.ObjectIterator {
	args := b.Called(ctx, query)
	return args.Get(0).(stiface.ObjectIterator)
}

func (b *storageBucketMock) ACL() stiface.ACLHandle {
	panic("not implemented")
}
func (b *storageBucketMock) IAM() *iam.Handle {
	panic("not implemented")
}
func (b *storageBucketMock) UserProject(projectID string) stiface.BucketHandle {
	panic("not implemented")
}
func (b *storageBucketMock) Notifications(context.Context) (map[string]*storage.Notification, error) {
	panic("not implemented")
}
func (b *storageBucketMock) AddNotification(context.Context, *storage.Notification) (*storage.Notification, error) {
	panic("not implemented")
}
func (b *storageBucketMock) DeleteNotification(context.Context, string) error {
	panic("not implemented")
}
func (b *storageBucketMock) LockRetentionPolicy(context.Context) error {
	panic("not implemented")
}

type objectHandleMock struct {
	stiface.ObjectHandle
	mock.Mock
}

func (objHandle *objectHandleMock) ACL() stiface.ACLHandle {
	panic("not implemented")
}
func (objHandle *objectHandleMock) Generation(int64) stiface.ObjectHandle {
	panic("not implemented")
}
func (objHandle *objectHandleMock) If(storage.Conditions) stiface.ObjectHandle {
	panic("not implemented")
}
func (objHandle *objectHandleMock) Key([]byte) stiface.ObjectHandle {
	panic("not implemented")
}
func (objHandle *objectHandleMock) ReadCompressed(bool) stiface.ObjectHandle {
	panic("not implemented")
}
func (objHandle *objectHandleMock) Attrs(ctx context.Context) (*storage.ObjectAttrs, error) {
	args := objHandle.Called(ctx)
	return args.Get(0).(*storage.ObjectAttrs), args.Error(1)
}
func (objHandle *objectHandleMock) Update(context.Context, storage.ObjectAttrsToUpdate) (*storage.ObjectAttrs, error) {
	panic("not implemented")
}
func (objHandle *objectHandleMock) NewReader(ctx context.Context) (stiface.Reader, error) {
	args := objHandle.Called(ctx)
	return args.Get(0).(stiface.Reader), args.Error(1)
}
func (objHandle *objectHandleMock) NewRangeReader(context.Context, int64, int64) (stiface.Reader, error) {
	panic("not implemented")
}
func (objHandle *objectHandleMock) NewWriter(ctx context.Context) stiface.Writer {
	args := objHandle.Called(ctx)
	return args.Get(0).(stiface.Writer)
}
func (objHandle *objectHandleMock) Delete(ctx context.Context) error {
	return objHandle.Called(ctx).Error(0)
}
func (objHandle *objectHandleMock) CopierFrom(stiface.ObjectHandle) stiface.Copier {
	panic("not implemented")
}
func (objHandle *objectHandleMock) ComposerFrom(...stiface.ObjectHandle) stiface.Composer {
	panic("not implemented")
}

type objectReaderMock struct {
	stiface.Reader
	mock.Mock
}

func (r *objectReaderMock) Read(p []byte) (n int, err error) {
	args := r.Called()
	if err := args.Error(1); err != nil {
		n, _ := args.Get(0).(io.Reader).Read(p)
		return n, err
	}
	return args.Get(0).(io.Reader).Read(p)
}

func (r *objectReaderMock) Close() error {
	args := r.Called()
	return args.Error(0)
}

func (r *objectReaderMock) Size() int64 {
	panic("not implemented")
}

func (r *objectReaderMock) Remain() int64 {
	panic("not implemented")
}

func (r *objectReaderMock) ContentType() string {
	panic("not implemented")
}

func (r *objectReaderMock) ContentEncoding() string {
	panic("not implemented")
}

func (r *objectReaderMock) CacheControl() string {
	panic("not implemented")
}

type objectWriterMock struct {
	stiface.Writer
	mock.Mock
}

func (o *objectWriterMock) Write(p []byte) (n int, err error) {
	args := o.Called()
	if err := args.Error(1); err != nil {
		n, _ := args.Get(0).(io.Writer).Write(p)
		return n, err
	}
	return args.Get(0).(io.Writer).Write(p)
}

func (o *objectWriterMock) Close() error {
	args := o.Called()
	return args.Error(0)
}

func (o *objectWriterMock) ObjectAttrs() *storage.ObjectAttrs {
	panic("implement me")
}

func (o *objectWriterMock) SetChunkSize(i int) {
	panic("implement me")
}

func (o *objectWriterMock) SetProgressFunc(f func(int64)) {
	panic("implement me")
}

func (o *objectWriterMock) SetCRC32C(u uint32) {
	panic("implement me")
}

func (o *objectWriterMock) CloseWithError(err error) error {
	panic("implement me")
}

func (o *objectWriterMock) Attrs() *storage.ObjectAttrs {
	panic("implement me")
}

type mockObjectIterator struct {
	mock.Mock
	stiface.ObjectIterator

	objs []*storage.ObjectAttrs
	idx  int
	err  error
}

func newMockObjectIterator(objs []*storage.ObjectAttrs) *mockObjectIterator {
	return &mockObjectIterator{objs: objs}
}

func (m *mockObjectIterator) Next() (*storage.ObjectAttrs, error) {

	if m.err != nil {
		return nil, m.err
	}

	if m.idx < len(m.objs) {
		cur := m.objs[m.idx]
		m.idx++
		return cur, nil
	}

	return nil, iterator.Done
}

func (m *mockObjectIterator) PageInfo() *iterator.PageInfo {
	panic("implement me")
}
