package symbiosis

import (
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"k8s.io/mount-utils"

	"github.com/google/uuid"
	"github.com/kubernetes-csi/csi-test/v4/pkg/sanity"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
)

func TestDriverSuite(t *testing.T) {
	socket := "/tmp/csi.sock"
	endpoint := "unix://" + socket
	if err := os.Remove(socket); err != nil && !os.IsNotExist(err) {
		t.Fatalf("failed to remove unix domain socket file %s, error: %s", socket, err)
	}

	volumes := make(map[string]*Volume)
	nodeID := uuid.New().String()
	vms := make(map[string]*Vm)
	vms[nodeID] = &Vm{
		AttachedVolumes: make([]string, 0),
	}

	fm := &fakeMounter{
		mounted: map[string]string{},
	}
	config := Config{
		DriverName:    "test-driver.symbiosis.host",
		Region:        "test-region-1",
		Endpoint:      "unix://tmp/csi.sock",
		NodeID:        nodeID,
		Token:         "test-token",
		VendorVersion: "0.0.1",
	}

	driver := &Driver{
		config:  config,
		mounter: fm,
		log:     logrus.New().WithField("test_enabled", true),

		storage: &fakeStorageServiceOp{
			volumes: volumes,
			vms:     vms,
		},
		mutex: sync.Mutex{},
	}

	ctx, cancel := context.WithCancel(context.Background())

	var eg errgroup.Group
	eg.Go(func() error {
		return driver.Run(ctx)
	})

	cfg := sanity.NewTestConfig()
	if err := os.RemoveAll(cfg.TargetPath); err != nil {
		t.Fatalf("failed to delete target path %s: %s", cfg.TargetPath, err)
	}
	if err := os.RemoveAll(cfg.StagingPath); err != nil {
		t.Fatalf("failed to delete staging path %s: %s", cfg.StagingPath, err)
	}
	cfg.Address = endpoint
	cfg.IDGen = &idGenerator{}
	cfg.IdempotentCount = 4
	cfg.TestNodeVolumeAttachLimit = true
	cfg.CheckPath = fm.checkMountPath
	sanity.Test(t, cfg)

	cancel()
	if err := eg.Wait(); err != nil {
		t.Errorf("driver run failed: %s", err)
	}
}

type Vm struct {
	AttachedVolumes []string
}

type fakeStorageServiceOp struct {
	volumes        map[string]*Volume
	vms            map[string]*Vm
	listVolumesErr error
}

func (f *fakeStorageServiceOp) ListVolumes(ctx context.Context, param *VolumeListParams) ([]Volume, *http.Response, error) {
	if f.listVolumesErr != nil {
		return nil, nil, f.listVolumesErr
	}

	var volumes []Volume

	for _, vol := range f.volumes {
		volumes = append(volumes, *vol)
	}

	//if param.ClusterName != "" || param.VolumeName != "" {
	if param.VolumeName != "" {
		var filtered []Volume
		for _, vol := range volumes {
			//if (param.ClusterName == "" || vol.ClusterID == param.ClusterName) && (param.VolumeName == "" || vol.Name == param.VolumeName) {
			if param.VolumeName == "" || vol.Name == param.VolumeName {
				filtered = append(filtered, vol)
			}
		}

		return filtered, fakeHttpResponse(), nil
	}

	return volumes, fakeHttpResponse(), nil
}

func (f *fakeStorageServiceOp) GetVolumeByID(ctx context.Context, id string) (*Volume, *http.Response, error) {
	resp := fakeHttpResponse()
	vol, ok := f.volumes[id]
	if !ok {
		resp.StatusCode = http.StatusNotFound
		return nil, resp, errors.New("volume not found")
	}

	return vol, resp, nil
}

func (f *fakeStorageServiceOp) CreateVolumeByName(ctx context.Context, volumeName string, req *VolumeCreateParams) (*Volume, *http.Response, error) {
	id := uuid.New().String()
	vol := &Volume{
		ID:            id,
		ClusterID:     uuid.New().String(),
		Name:          volumeName,
		StorageGi:     req.StorageGi,
		CreatedAt:     time.Now(),
		AttachedNodes: make([]string, 0),
	}

	f.volumes[id] = vol

	return vol, fakeHttpResponse(), nil
}

func (f *fakeStorageServiceOp) DeleteVolumeByID(ctx context.Context, id string) (*http.Response, error) {
	// Hack since csi-test won't unpublish volumes under certain conditions
	for _, vm := range f.vms {
		var updatedAttachedVolumes []string
		for _, attachedVolumeID := range vm.AttachedVolumes {
			if attachedVolumeID != id {
				updatedAttachedVolumes = append(updatedAttachedVolumes, attachedVolumeID)
			}
		}
		vm.AttachedVolumes = updatedAttachedVolumes
	}
	delete(f.volumes, id)
	return fakeHttpResponse(), nil
}

func (f *fakeStorageServiceOp) AttachVolume(ctx context.Context, volumeID string, nodeID string, params *VolumeAttachParams) (*VolumeMount, *http.Response, error) {
	resp := fakeHttpResponse()
	vol, ok := f.volumes[volumeID]
	if !ok {
		resp.StatusCode = http.StatusNotFound
		return nil, resp, errors.New("volume was not found")
	}

	vm, ok := f.vms[nodeID]
	if !ok {
		resp.StatusCode = http.StatusNotFound
		return nil, resp, errors.New("vm was not found")
	}

	if len(vm.AttachedVolumes) >= maxVolumesPerNode {
		resp.StatusCode = http.StatusUnprocessableEntity
		return nil, resp, fmt.Errorf("max volumes per node reached (%v attached, %v max)", len(vm.AttachedVolumes), maxVolumesPerNode)
	}

	for _, attached := range vm.AttachedVolumes {
		if attached == volumeID {
			return nil, resp, nil
		}
	}

	vm.AttachedVolumes = append(vm.AttachedVolumes, volumeID)
	vol.AttachedNodes = append(vol.AttachedNodes, nodeID)

	volumeMount := &VolumeMount{
		DeviceName: "testdevice",
	}

	return volumeMount, resp, nil
}

func (f *fakeStorageServiceOp) DetachVolume(ctx context.Context, volumeID string, nodeID string) (*http.Response, error) {
	resp := fakeHttpResponse()

	vol, ok := f.volumes[volumeID]
	if !ok {
		resp.StatusCode = http.StatusNotFound
		return resp, errors.New("volume was not found")
	}

	vm, ok := f.vms[nodeID]
	if !ok {
		resp.StatusCode = http.StatusNotFound
		return resp, errors.New("vm was not found")
	}

	var found bool
	var updatedAttachedVolumes []string
	for _, attachedVolID := range vm.AttachedVolumes {
		if attachedVolID == volumeID {
			found = true
		} else {
			updatedAttachedVolumes = append(updatedAttachedVolumes, attachedVolID)
		}
	}

	var updatedAttachedNodes []string
	for _, attachedNodeID := range vol.AttachedNodes {
		if attachedNodeID != nodeID {
			updatedAttachedNodes = append(updatedAttachedNodes, attachedNodeID)
		}
	}

	if !found {
		resp.StatusCode = http.StatusNotFound
		return resp, errors.New("volume is not attached to vm")
	}

	vm.AttachedVolumes = updatedAttachedVolumes
	vol.AttachedNodes = updatedAttachedNodes

	return resp, nil
}

type fakeMounter struct {
	mounted map[string]string
}

func (f *fakeMounter) Format(source string, fsType string) error {
	return nil
}

func (f *fakeMounter) Mount(source string, target string, fsType string, options ...string) error {
	f.mounted[target] = source
	return nil
}

func (f *fakeMounter) Unmount(target string) error {
	delete(f.mounted, target)
	return nil
}

func (f *fakeMounter) GetDeviceName(_ mount.Interface, mountPath string) (string, error) {
	if _, ok := f.mounted[mountPath]; ok {
		return "/mnt/sda1", nil
	}

	return "", nil
}

func (f *fakeMounter) IsFormatted(source string) (bool, error) {
	return true, nil
}
func (f *fakeMounter) IsMounted(target string) (bool, error) {
	_, ok := f.mounted[target]
	return ok, nil
}

func (f *fakeMounter) checkMountPath(path string) (sanity.PathKind, error) {
	isMounted, err := f.IsMounted(path)
	if err != nil {
		return "", err
	}
	if isMounted {
		return sanity.PathIsDir, nil
	}
	return sanity.PathIsNotFound, nil
}

func (f *fakeMounter) GetStatistics(volumePath string) (volumeStatistics, error) {
	return volumeStatistics{
		availableBytes: 3 * gib,
		totalBytes:     10 * gib,
		usedBytes:      7 * gib,

		availableInodes: 3000,
		totalInodes:     10000,
		usedInodes:      7000,
	}, nil
}

func (f *fakeMounter) IsBlockDevice(volumePath string) (bool, error) {
	return false, nil
}

func fakeHttpResponse() *http.Response {
	return &http.Response{}
}

type idGenerator struct{}

func (g *idGenerator) GenerateUniqueValidVolumeID() string {
	return uuid.New().String()
}

func (g *idGenerator) GenerateInvalidVolumeID() string {
	return "not-a-uuid"
}

func (g *idGenerator) GenerateUniqueValidNodeID() string {
	return uuid.New().String()
}

func (g *idGenerator) GenerateInvalidNodeID() string {
	return "not-a-uuid"
}

func randString(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
