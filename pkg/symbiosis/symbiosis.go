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
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/golang/glog"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

const (
	kib    int64 = 1024
	mib    int64 = kib * 1024
	gib    int64 = mib * 1024
	gib100 int64 = gib * 100
	tib    int64 = gib * 1024
	tib100 int64 = tib * 100
)

type symbiosisOp struct {
	config  Config
	client  StorageService
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
	PublishInfoVolumeNameParam string
	Endpoint                   string
	ProxyEndpoint              string
	NodeID                     string
	VendorVersion              string
	StateDir                   string
	MaxVolumesPerNode          int64
	MaxVolumeSize              int64
	AttachLimit                int64
	Ephemeral                  bool
	ShowVersion                bool
	EnableAttach               bool
	EnableTopology             bool
	EnableVolumeExpansion      bool
	CheckVolumeLifecycle       bool
}

var (
	vendorVersion = "dev"
)

const (
	// Extension with which snapshot files will be saved.
	snapshotExt = ".snap"
)

func NewDriver(cfg Config) (*symbiosisOp, error) {
	if cfg.DriverName == "" {
		return nil, errors.New("no driver name provided")
	}

	if cfg.NodeID == "" {
		return nil, errors.New("no node id provided")
	}

	if cfg.Endpoint == "" {
		return nil, errors.New("no driver endpoint provided")
	}

	if err := os.MkdirAll(cfg.StateDir, 0750); err != nil {
		return nil, fmt.Errorf("failed to create dataRoot: %v", err)
	}

	glog.Infof("Driver: %v ", cfg.DriverName)
	glog.Infof("Version: %s", cfg.VendorVersion)

	hp := &symbiosisOp{
		config: cfg,
	}
	return hp, nil
}

func (sym *symbiosisOp) Run() error {
	s := NewNonBlockingGRPCServer()
	// hp itself implements ControllerServer, NodeServer, and IdentityServer.
	s.Start(sym.config.Endpoint, sym, sym, sym)
	s.Wait()

	return nil
}

// getVolumePath returns the canonical path for hostpath volume
func (sym *symbiosisOp) getVolumePath(volID string) string {
	return filepath.Join(sym.config.StateDir, volID)
}

// getSnapshotPath returns the full path to where the snapshot is stored
func (sym *symbiosisOp) getSnapshotPath(snapshotID string) string {
	return filepath.Join(sym.config.StateDir, fmt.Sprintf("%s%s", snapshotID, snapshotExt))
}

func (sym *symbiosisOp) sumVolumeSizes(ctx context.Context, kind string) (sum int64) {
	volumes, _, _ := sym.client.ListVolumes(ctx, &VolumeListParams{})
	for _, volume := range volumes {
		sum += volume.StorageGiB
	}
	return
}
