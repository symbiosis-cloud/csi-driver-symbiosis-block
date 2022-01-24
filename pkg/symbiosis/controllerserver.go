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
	"fmt"
	"sort"

	"github.com/golang/glog"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

type AccessType int

const (
	MountAccess AccessType = iota
	BlockAccess
)

const (
	deviceID = "deviceID"
)

func (sym *symbiosisOp) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (resp *csi.CreateVolumeResponse, finalErr error) {
	if err := sym.validateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		glog.V(3).Infof("invalid create volume req: %v", req)
		return nil, err
	}

	// Check arguments
	if len(req.GetName()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Name missing in request")
	}
	caps := req.GetVolumeCapabilities()
	if caps == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capabilities missing in request")
	}

	// Keep a record of the requested access types.
	var accessTypeMount, accessTypeBlock bool

	for _, cap := range caps {
		if cap.GetBlock() != nil {
			accessTypeBlock = true
		}
		if cap.GetMount() != nil {
			accessTypeMount = true
		}
	}
	// A real driver would also need to check that the other
	// fields in VolumeCapabilities are sane. The check above is
	// just enough to pass the "[Testpattern: Dynamic PV (block
	// volmode)] volumeMode should fail in binding dynamic
	// provisioned PV to PVC" storage E2E test.

	if accessTypeBlock && accessTypeMount {
		return nil, status.Error(codes.InvalidArgument, "cannot have both block and mount access type")
	}

	var requestedAccessType AccessType

	if accessTypeBlock {
		requestedAccessType = BlockAccess
	} else {
		// Default to mount.
		requestedAccessType = MountAccess
	}

	// Lock before acting on global state. A production-quality
	// driver might use more fine-grained locking.
	sym.mutex.Lock()
	defer sym.mutex.Unlock()

	capacity := int64(req.GetCapacityRange().GetRequiredBytes())

	// Need to check for already existing volume name, and if found
	// check for the requested capacity and already allocated capacity
	if exVol, _, err := sym.client.GetVolumeByID(ctx, req.GetName()); err == nil {
		// Since err is nil, it means the volume with the same name already exists
		// need to check if the size of existing volume is the same as in new
		// request
		if exVol.StorageGiB < capacity {
			return nil, status.Errorf(codes.AlreadyExists, "Volume with the same name: %s but with different size already exist", req.GetName())
		}
		if req.GetVolumeContentSource() != nil {
			volumeSource := req.VolumeContentSource
			switch volumeSource.Type.(type) {
			case *csi.VolumeContentSource_Snapshot:
			case *csi.VolumeContentSource_Volume:
				break
			default:
				return nil, status.Errorf(codes.InvalidArgument, "%v not a proper volume source", volumeSource)
			}
		}

		return &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId:      exVol.ID,
				CapacityBytes: int64(exVol.StorageGiB),
				VolumeContext: req.GetParameters(),
				ContentSource: req.GetVolumeContentSource(),
			},
		}, nil
	}

	vol, _, err := sym.client.GetVolumeByID(ctx, req.GetName())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if vol != nil {
		if vol.StorageGiB != req.GetCapacityRange().GetRequiredBytes() {
			return nil, status.Error(codes.AlreadyExists, fmt.Sprintf("invalid option requested size: %d", req.CapacityRange.RequiredBytes))
		}

		return &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId:      vol.ID,
				CapacityBytes: req.GetCapacityRange().GetRequiredBytes(),
				VolumeContext: req.GetParameters(),
				ContentSource: req.GetVolumeContentSource(),
			},
		}, nil
	}

	vol, _, err = sym.client.CreateVolumeByID(ctx, req.GetName(), &VolumeCreateParams{
		StorageGiB: capacity,
		AccessType: int(requestedAccessType),
	})
	if err != nil {
		return nil, err
	}

	glog.V(4).Infof("created volume %s", vol.ID)

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      vol.ID,
			CapacityBytes: req.GetCapacityRange().GetRequiredBytes(),
			VolumeContext: req.GetParameters(),
			ContentSource: req.GetVolumeContentSource(),
		},
	}, nil
}

func (sym *symbiosisOp) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	// Check arguments
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	if err := sym.validateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		glog.V(3).Infof("invalid delete volume req: %v", req)
		return nil, err
	}

	// Lock before acting on global state. A production-quality
	// driver might use more fine-grained locking.
	sym.mutex.Lock()
	defer sym.mutex.Unlock()

	volId := req.GetVolumeId()
	vol, _, err := sym.client.GetVolumeByID(ctx, volId)
	if err != nil {
		// Volume not found: might have already deleted
		return &csi.DeleteVolumeResponse{}, nil
	}

	if len(vol.AttachedNodes) > 0 {

		msg := fmt.Sprintf("Volume '%s' is still attached to nodes '%s'", vol.ID, vol.AttachedNodes)
		return nil, status.Error(codes.Internal, msg)
	}

	if _, err := sym.client.DeleteVolumeByID(ctx, volId); err != nil {
		return nil, fmt.Errorf("failed to delete volume %v: %w", volId, err)
	}
	glog.V(4).Infof("volume %v successfully deleted", volId)

	return &csi.DeleteVolumeResponse{}, nil
}

func (sym *symbiosisOp) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: sym.getControllerServiceCapabilities(),
	}, nil
}

func (sym *symbiosisOp) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {

	// Check arguments
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID cannot be empty")
	}
	if len(req.VolumeCapabilities) == 0 {
		return nil, status.Error(codes.InvalidArgument, req.VolumeId)
	}

	// Lock before acting on global state. A production-quality
	// driver might use more fine-grained locking.
	sym.mutex.Lock()
	defer sym.mutex.Unlock()

	if _, _, err := sym.client.GetVolumeByID(ctx, req.GetVolumeId()); err != nil {
		return nil, err
	}

	for _, cap := range req.GetVolumeCapabilities() {
		if cap.GetMount() == nil && cap.GetBlock() == nil {
			return nil, status.Error(codes.InvalidArgument, "cannot have both mount and block access type be undefined")
		}

		// A real driver would check the capabilities of the given volume with
		// the set of requested capabilities.
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeContext:      req.GetVolumeContext(),
			VolumeCapabilities: req.GetVolumeCapabilities(),
			Parameters:         req.GetParameters(),
		},
	}, nil
}

func (sym *symbiosisOp) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	if !sym.config.EnableAttach {
		return nil, status.Error(codes.Unimplemented, "ControllerPublishVolume is not supported")
	}

	if len(req.VolumeId) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID cannot be empty")
	}
	if len(req.NodeId) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Node ID cannot be empty")
	}
	if req.VolumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capabilities cannot be empty")
	}

	if req.NodeId != sym.config.NodeID {
		return nil, status.Errorf(codes.NotFound, "Not matching Node ID %s to hostpath Node ID %s", req.NodeId, sym.config.NodeID)
	}

	sym.mutex.Lock()
	defer sym.mutex.Unlock()

	vol, _, err := sym.client.GetVolumeByID(ctx, req.VolumeId)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	// Check to see if the volume is already published.
	if len(vol.AttachedNodes) > 0 {
		// TODO Check if readonly flag is compatible with the publish request.

		return &csi.ControllerPublishVolumeResponse{
			PublishContext: map[string]string{},
		}, nil
	}

	if _, err := sym.client.AttachVolume(ctx, req.VolumeId, req.NodeId, &VolumeAttachParams{ReadOnly: req.GetReadonly()}); err != nil {
		return nil, err
	}

	return &csi.ControllerPublishVolumeResponse{
		PublishContext: map[string]string{},
	}, nil
}

func (sym *symbiosisOp) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	if !sym.config.EnableAttach {
		return nil, status.Error(codes.Unimplemented, "ControllerUnpublishVolume is not supported")
	}

	if len(req.VolumeId) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID cannot be empty")
	}

	// Empty node id is not a failure as per Spec
	if req.NodeId != "" && req.NodeId != sym.config.NodeID {
		return nil, status.Errorf(codes.NotFound, "Node ID %s does not match to expected Node ID %s", req.NodeId, sym.config.NodeID)
	}

	sym.mutex.Lock()
	defer sym.mutex.Unlock()

	vol, _, err := sym.client.GetVolumeByID(ctx, req.VolumeId)
	if err != nil {
		// Not an error: a non-existent volume is not published.
		// See also https://github.com/kubernetes-csi/external-attacher/pull/165
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}

	// Check to see if the volume is published on a node
	if len(vol.AttachedNodes) > 0 {
		err := status.Error(codes.Internal, fmt.Sprintf("Volume '%s' is still published by nodes '%s'", vol.ID, vol.AttachedNodes))
		return nil, err
	}

	if _, err := sym.client.DetachVolume(ctx, vol.ID, req.NodeId); err != nil {
		return nil, status.Errorf(codes.Internal, "could not update volume %s: %v", vol.ID, err)
	}

	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (sym *symbiosisOp) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	volumeRes := &csi.ListVolumesResponse{
		Entries: []*csi.ListVolumesResponse_Entry{},
	}

	var (
		volumesLength, maxLength int64
	)

	// Lock before acting on global state. A production-quality
	// driver might use more fine-grained locking.
	sym.mutex.Lock()
	defer sym.mutex.Unlock()

	// Sort by volume ID.
	volumes, _, err := sym.client.ListVolumes(ctx, &VolumeListParams{ClusterId: ""}) // TODO
	if err != nil {
		return nil, status.Error(codes.Aborted, "Failed fetching list of volumes")
	}
	sort.Slice(volumes, func(i, j int) bool {
		return volumes[i].ID < volumes[j].ID
	})

	if req.StartingToken == "" {
		req.StartingToken = "1"
	}

	volumesLength = int64(len(volumes))
	maxLength = int64(req.MaxEntries)

	if maxLength > volumesLength || maxLength <= 0 {
		maxLength = volumesLength
	}

	for _, vol := range volumes {
		volumeRes.Entries = append(volumeRes.Entries, &csi.ListVolumesResponse_Entry{
			Volume: &csi.Volume{
				VolumeId:      vol.ID,
				CapacityBytes: vol.StorageGiB * 1024 * 1024,
			},
			Status: &csi.ListVolumesResponse_VolumeStatus{
				PublishedNodeIds: vol.AttachedNodes,
			},
		})
	}

	glog.V(5).Infof("Volumes are: %+v", *volumeRes)
	return volumeRes, nil
}

func (sym *symbiosisOp) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	// Lock before acting on global state. A production-quality
	// driver might use more fine-grained locking.
	sym.mutex.Lock()
	defer sym.mutex.Unlock()

	volume, _, err := sym.client.GetVolumeByID(ctx, req.GetVolumeId())
	if err != nil {
		return nil, err
	}

	return &csi.ControllerGetVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volume.ID,
			CapacityBytes: volume.StorageGiB,
		},
		Status: &csi.ControllerGetVolumeResponse_VolumeStatus{
			PublishedNodeIds: volume.AttachedNodes,
		},
	}, nil
}

func (sym *symbiosisOp) validateControllerServiceRequest(c csi.ControllerServiceCapability_RPC_Type) error {
	if c == csi.ControllerServiceCapability_RPC_UNKNOWN {
		return nil
	}

	for _, cap := range sym.getControllerServiceCapabilities() {
		if c == cap.GetRpc().GetType() {
			return nil
		}
	}
	return status.Errorf(codes.InvalidArgument, "unsupported capability %s", c)
}

func (sym *symbiosisOp) getControllerServiceCapabilities() []*csi.ControllerServiceCapability {
	var cl []csi.ControllerServiceCapability_RPC_Type = []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_GET_VOLUME,
		csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
		csi.ControllerServiceCapability_RPC_VOLUME_CONDITION,
		csi.ControllerServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
	}

	var csc []*csi.ControllerServiceCapability

	for _, cap := range cl {
		csc = append(csc, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: cap,
				},
			},
		})
	}

	return csc
}

func (sym *symbiosisOp) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ControllerExpandVolume is not supported")
}

func (sym *symbiosisOp) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "CreateSnapshot is not supported")
}

func (sym *symbiosisOp) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "DeleteSnapshot is not supported")
}

func (sym *symbiosisOp) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ListSnapshots is not supported")
}

func (sym *symbiosisOp) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "GetCapacity is not supported")
}
