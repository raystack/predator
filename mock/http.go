package mock

import (
	"github.com/stretchr/testify/mock"
	"io"
	"net/http"
)

type mockHttpClient struct {
	mock.Mock
}

func NewHttpClient() *mockHttpClient {
	return &mockHttpClient{}
}

func (c *mockHttpClient) Get(url string) (resp *http.Response, err error) {
	args := c.Called(url)
	return args.Get(0).(*http.Response), args.Error(1)
}

func (c *mockHttpClient) Post(url, contentType string, body io.Reader) (resp *http.Response, err error) {
	args := c.Called(url, contentType, body)
	return args.Get(0).(*http.Response), args.Error(1)
}
