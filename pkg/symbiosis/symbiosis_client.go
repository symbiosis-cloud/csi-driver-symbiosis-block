package symbiosis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

const (
	defaultBaseURL = "https://api.symbiosis.host/"
	mediaType      = "application/json"
)

type StorageService interface {
	ListVolumes(context.Context, *VolumeListParams) ([]Volume, *http.Response, error)
	GetVolumeByID(context.Context, string) (*Volume, *http.Response, error)
	CreateVolumeByID(context.Context, string, *VolumeCreateParams) (*Volume, *http.Response, error)
	DeleteVolumeByID(context.Context, string) (*http.Response, error)
	AttachVolume(context.Context, string, string, *VolumeAttachParams) (*http.Response, error)
	DetachVolume(context.Context, string, string) (*http.Response, error)

	NewRequest(context.Context, string, string, interface{}) (*http.Request, error)
	Request(context.Context, *http.Request, interface{}) (*http.Response, error)
}

type StorageServiceOp struct {
	client *Client
}

type Client struct {
	client  *http.Client
	BaseURL *url.URL
}

type Volume struct {
	ID             string    `json:"id"`
	ClusterID      string    `json:"clusterId"`
	Name           string    `json:"name"`
	StorageGiB     int64     `json:"storageGiB"`
	CreatedAt      time.Time `json:"createdAt"`
	FilesystemType string    `json:"filesystemType"`
	AttachedNodes  []string  `json:"attachedNodes"`
}

type VolumeListParams struct {
	ClusterId string `json:"clusterId"`
}

type VolumeAttachParams struct {
	ReadOnly bool `json:"readOnly"`
}

type VolumeCreateParams struct {
	Region     string   `json:"region"`
	StorageGiB int64    `json:"storageGiB"`
	AccessType int      `json:"accessType"`
	Tags       []string `json:"tags"`
}

func (svc *StorageServiceOp) ListVolumes(ctx context.Context, params *VolumeListParams) ([]Volume, *http.Response, error) {
	if params == nil {
		return nil, nil, errors.New("VolumeListParams required")
	}
	path := fmt.Sprintf("/rest/v1/cluster/%s/storage", params.ClusterId)

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

func (c *Client) NewRequest(ctx context.Context, method string, path string, body *interface{}) (*http.Request, error) {
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
	return req, nil
}

func (c *Client) Do(ctx context.Context, req *http.Request, v interface{}) (*http.Response, error) {
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

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
