package http

import (
	"io"
	"net/http"
	"time"
)

//Client interface of http client
type Client interface {
	Get(url string) (resp *http.Response, err error)
	Post(url, contentType string, body io.Reader) (resp *http.Response, err error)
}

//DefaultClient default http client
type DefaultClient struct {
	client *http.Client
}

func (d *DefaultClient) Post(url, contentType string, body io.Reader) (resp *http.Response, err error) {
	return d.client.Post(url, contentType, body)
}

//NewDefaultClient constructor of default http client
func NewDefaultClient() *DefaultClient {
	return &DefaultClient{
		client: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
}

//NewClientWithTimeout create http client with timeout duration
func NewClientWithTimeout(d time.Duration) *DefaultClient {
	return &DefaultClient{
		client: &http.Client{
			Timeout: d,
		},
	}
}

//Get http get
func (d *DefaultClient) Get(url string) (resp *http.Response, err error) {
	return d.client.Get(url)
}
