package symbiosis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
)

const (
	mediaType = "application/json"
)

type StorageService interface {
	ListVolumes(context.Context, *VolumeListParams) ([]Volume, *http.Response, error)
	GetVolumeByID(context.Context, string) (*Volume, *http.Response, error)
	CreateVolumeByName(context.Context, string, *VolumeCreateParams) (*Volume, *http.Response, error)
	DeleteVolumeByID(context.Context, string) (*http.Response, error)
	AttachVolume(context.Context, string, string, *VolumeAttachParams) (*VolumeMount, *http.Response, error)
	DetachVolume(context.Context, string, string) (*http.Response, error)
}
type Client interface {
	NewRequest(context.Context, string, string, interface{}) (*http.Request, error)
	Do(context.Context, *http.Request, interface{}) (*http.Response, error)
}

type StorageServiceOp struct {
	client *ClientOp
}

type ClientOp struct {
	client  *http.Client
	ApiKey  string
	BaseURL *url.URL
}

type Volume struct {
	ID             string    `json:"id"`
	ClusterID      string    `json:"clusterId"`
	Name           string    `json:"name"`
	StorageGi      int64     `json:"storageGi"`
	CreatedAt      time.Time `json:"createdAt"`
	FilesystemType string    `json:"filesystemType"`
	AttachedNodes  []string  `json:"attachedNodes"`
}

type VolumeMount struct {
	DeviceName string `json:"deviceName"`
}

type VolumeListParams struct {
	ClusterID  string `json:"clusterId"`
	VolumeName string `json:"volumeName"`
}

type VolumeAttachParams struct {
	ReadOnly bool `json:"readOnly"`
}

type VolumeCreateParams struct {
	ClusterID  string `json:"clusterId"`
	VolumeName string `json:"volumeName"`
	StorageGi  int64  `json:"storageGi"`
	AccessType string `json:"accessType"`
}

type SymbiosisApiError struct {
	StatusCode int
	ErrorType  string `json:"error"`
	Message    string `json:"message"`
	Path       string `json:"path"`
}

func (error *SymbiosisApiError) Error() string {
	return fmt.Sprintf("Symbiosis API Error: %v (type %v, status %v)", error.Message, error.ErrorType, error.StatusCode)
}

func (svc *StorageServiceOp) ListVolumes(ctx context.Context, params *VolumeListParams) ([]Volume, *http.Response, error) {
	if params == nil {
		return nil, nil, errors.New("VolumeListParams required")
	}
	path := fmt.Sprintf("/rest/v1/volume?clusterId=%s", url.QueryEscape(params.ClusterID))

	req, err := svc.client.NewRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, nil, err
	}

	volumes := []Volume{}
	resp, err := svc.client.Do(ctx, req, volumes)
	if err != nil {
		return nil, resp, err
	}

	return volumes, resp, nil
}

func (svc *StorageServiceOp) GetVolumeByID(ctx context.Context, id string) (*Volume, *http.Response, error) {
	if id == "" {
		return nil, nil, errors.New("id required")
	}
	path := fmt.Sprintf("/rest/v1/volume/%s", id)

	req, err := svc.client.NewRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, nil, err
	}

	volume := new(Volume)
	resp, err := svc.client.Do(ctx, req, volume)
	if err != nil {
		return nil, resp, err
	}

	return volume, resp, nil
}

func (svc *StorageServiceOp) CreateVolumeByName(ctx context.Context, volumeName string, params *VolumeCreateParams) (*Volume, *http.Response, error) {
	if params == nil {
		return nil, nil, errors.New("VolumeCreateParams required")
	}

	req, err := svc.client.NewRequest(ctx, http.MethodPost, "/rest/v1/volume", params)
	if err != nil {
		return nil, nil, err
	}

	volume := new(Volume)
	resp, err := svc.client.Do(ctx, req, volume)
	if err != nil {
		return nil, resp, err
	}

	return volume, resp, nil
}

func (svc *StorageServiceOp) DeleteVolumeByID(ctx context.Context, id string) (*http.Response, error) {
	if id == "" {
		return nil, errors.New("VolumeListParams required")
	}
	path := fmt.Sprintf("/rest/v1/volume/%s", id)

	req, err := svc.client.NewRequest(ctx, http.MethodDelete, path, nil)
	if err != nil {
		return nil, err
	}

	resp, err := svc.client.Do(ctx, req, nil)
	if err != nil {
		return resp, err
	}

	return resp, nil
}

func (svc *StorageServiceOp) AttachVolume(ctx context.Context, volumeID string, nodeID string, params *VolumeAttachParams) (*VolumeMount, *http.Response, error) {
	if volumeID == "" {
		return nil, nil, errors.New("volumeID required")
	}
	if nodeID == "" {
		return nil, nil, errors.New("nodeID required")
	}
	path := fmt.Sprintf("/rest/v1/volume/%s/node/%s", volumeID, nodeID)

	req, err := svc.client.NewRequest(ctx, http.MethodPost, path, params)
	if err != nil {
		return nil, nil, err
	}

	volumeMount := new(VolumeMount)
	resp, err := svc.client.Do(ctx, req, volumeMount)
	if err != nil {
		return nil, resp, err
	}

	return volumeMount, resp, nil
}

func (svc *StorageServiceOp) DetachVolume(ctx context.Context, volumeID string, nodeID string) (*http.Response, error) {
	if volumeID == "" {
		return nil, errors.New("volumeID required")
	}
	if nodeID == "" {
		return nil, errors.New("nodeID required")
	}
	path := fmt.Sprintf("/rest/v1/volume/%s/node/%s", volumeID, nodeID)

	req, err := svc.client.NewRequest(ctx, http.MethodDelete, path, nil)
	if err != nil {
		return nil, err
	}

	resp, err := svc.client.Do(ctx, req, nil)
	if err != nil {
		return resp, err
	}

	return resp, nil
}

func (c *ClientOp) NewRequest(ctx context.Context, method string, path string, body interface{}) (*http.Request, error) {
	u, err := c.BaseURL.Parse(path)
	if err != nil {
		return nil, err
	}

	buf := new(bytes.Buffer)
	if body != nil {
		err = json.NewEncoder(buf).Encode(body)
		if err != nil {
			return nil, err
		}
	}

	req, err := http.NewRequest(method, u.String(), buf)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", mediaType)
	req.Header.Add("Accept", mediaType)
	req.Header.Add("X-Auth-ApiKey", c.ApiKey)
	return req, nil
}

func (c *ClientOp) Do(ctx context.Context, req *http.Request, v interface{}) (*http.Response, error) {
	client := &http.Client{}
	resp, err := client.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if c := resp.StatusCode; c < 200 || c > 299 {
		data, err := ioutil.ReadAll(resp.Body)
		errorResponse := &SymbiosisApiError{
			StatusCode: resp.StatusCode,
		}
		if err == nil && len(data) > 0 {
			err := json.Unmarshal(data, errorResponse)
			if err != nil {
				return nil, errors.New(string(data))
			}
		}

		return nil, errorResponse
	}

	if v != nil {
		if w, ok := v.(io.Writer); ok {
			_, err = io.Copy(w, resp.Body)
			if err != nil {
				return nil, err
			}
		} else {
			err = json.NewDecoder(resp.Body).Decode(v)
			if err != nil {
				return nil, err
			}
		}
	}

	return resp, nil
}
