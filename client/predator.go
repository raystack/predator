package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/odpf/predator/api/model"
	xhttp "github.com/odpf/predator/external/http"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/job"
)

const (
	contentType                     = "application/json"
	getProfileTimeoutInSecond       = 1800
	getProfileRetryIntervalInSecond = 5
)

//Predator as Predator API client
type Predator struct {
	hostURL string
	client  xhttp.Client
}

//New to construct Predator client
func New(hostURL string, client xhttp.Client) *Predator {
	return &Predator{
		hostURL: hostURL,
		client:  client,
	}
}

//Upload to upload spec
func (p *Predator) Upload(gitInfo *protocol.GitInfo) (*model.UploadReport, error) {
	var err error
	request := &model.UploadRequest{
		GitURL:     gitInfo.URL,
		CommitID:   gitInfo.CommitID,
		PathPrefix: gitInfo.PathPrefix,
	}

	reqContent, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	body := bytes.NewBuffer(reqContent)
	resourcePath := fmt.Sprintf("%s/v1beta1/spec/upload", p.hostURL)
	resp, err := p.client.Post(resourcePath, contentType, body)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = resp.Body.Close()
	}()

	respContent, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusBadRequest {
			return nil, fmt.Errorf("something wrong with the spec files: %d %s", resp.StatusCode, string(respContent))
		}
		return nil, fmt.Errorf("something wrong with predator server %d %s", resp.StatusCode, string(respContent))
	}

	var uploadReport model.UploadReport
	if err = json.Unmarshal(respContent, &uploadReport); err != nil {
		return nil, err
	}

	return &uploadReport, nil
}

//Profile to call start Profile API
func (p *Predator) Profile(request *model.ProfileRequest) (*model.ProfileResponse, error) {
	reqContent, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	body := bytes.NewBuffer(reqContent)
	resourcePath := fmt.Sprintf("%s/v1beta1/profile", p.hostURL)
	resp, err := p.client.Post(resourcePath, contentType, body)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = resp.Body.Close()
	}()

	respContent, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API error %d %s", resp.StatusCode, string(respContent))
	}

	var profileResponse model.ProfileResponse
	if err = json.Unmarshal(respContent, &profileResponse); err != nil {
		return nil, err
	}

	return &profileResponse, nil
}

func (p *Predator) callGetProfile(profileID string) (*model.ProfileResponse, error) {
	var profileResponse model.ProfileResponse
	resourcePath := fmt.Sprintf("%s/v1beta1/profile/%s", p.hostURL, profileID)
	resp, err := p.client.Get(resourcePath)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = resp.Body.Close()
	}()

	respContent, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API error %d %s", resp.StatusCode, string(respContent))
	}

	if err = json.Unmarshal(respContent, &profileResponse); err != nil {
		return nil, err
	}
	return &profileResponse, nil
}

//GetProfile to call get profile result API
func (p *Predator) GetProfile(profileID string) (*model.ProfileResponse, error) {
	timeout := time.After(time.Duration(getProfileTimeoutInSecond) * time.Second)
	ticker := time.NewTicker(time.Duration(getProfileRetryIntervalInSecond) * time.Second)
	for {
		select {
		case <-timeout:
			return nil, errors.New("Get profile time out")
		case <-ticker.C:
			profileResponse, err := p.callGetProfile(profileID)
			if err != nil {
				return nil, err
			}
			if profileResponse.State == job.StateCompleted || profileResponse.State == job.StateFailed {
				return profileResponse, nil
			}
		}
	}
}

//Audit to call Audit API
func (p *Predator) Audit(profileID string) (*model.AuditResponse, error) {
	resourcePath := fmt.Sprintf("%s/v1beta1/profile/%s/audit", p.hostURL, profileID)
	resp, err := p.client.Post(resourcePath, contentType, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = resp.Body.Close()
	}()

	respContent, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API error %d %s", resp.StatusCode, string(respContent))
	}

	var auditResponse model.AuditResponse
	if err = json.Unmarshal(respContent, &auditResponse); err != nil {
		return nil, err
	}

	return &auditResponse, nil
}
