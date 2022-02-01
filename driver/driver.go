/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package symbiosis

import (
	"errors"
	"net/http"
	"net/url"
	"sync"

	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

const (
	kib int64 = 1024
	mib int64 = kib * 1024
	gib int64 = mib * 1024
	tib int64 = gib * 1024
)

type Driver struct {
	config  Config
	storage StorageService
	mounter Mounter
	log     *logrus.Entry

	// gRPC calls involving any of the fields below must be serialized
	// by locking this mutex before starting. Internal helper
	// functions assume that the mutex has been locked.
	mutex sync.Mutex
}

type Config struct {
	DriverName                 string
	Region                     string
	PublishInfoDeviceNameParam string
	Endpoint                   string
	NodeID                     string
	ClusterID                  string
	ApiEndpoint                string
	Token                      string
	VendorVersion              string
}

func NewDriver(cfg Config) (*Driver, error) {
	if cfg.DriverName == "" {
		return nil, errors.New("no driver name provided")
	}

	if cfg.NodeID == "" {
		return nil, errors.New("no node id provided")
	}

	if cfg.Endpoint == "" {
		return nil, errors.New("no driver endpoint provided")
	}
	BaseURL, err := url.Parse(cfg.ApiEndpoint)
	if err != nil {
		return nil, err
	}

	storage := &StorageServiceOp{
		client: &ClientOp{
			client:  &http.Client{},
			ApiKey:  cfg.Token,
			BaseURL: BaseURL,
		},
	}

	logger := logrus.New().WithFields(logrus.Fields{
		"region":        cfg.Region,
		"nodeId":        cfg.NodeID,
		"vendorVersion": cfg.VendorVersion,
	})
	logger.Infof("Driver: %v ", cfg.DriverName)
	mounter := newMounter(logger)

	driver := &Driver{
		config:  cfg,
		storage: storage,
		mounter: mounter,
		log:     logger,
		mutex:   sync.Mutex{},
	}
	return driver, nil
}

func (sym *Driver) Run(ctx context.Context) error {
	s := &nonBlockingGRPCServer{
		log: sym.log,
	}
	s.Start(ctx, sym.config.Endpoint, sym, sym, sym)

	return nil
}
