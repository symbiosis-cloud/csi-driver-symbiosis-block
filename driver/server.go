/*
Copyright 2019 The Kubernetes Authors.

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
	"sync"

	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/symbiosis-cloud/csi-driver-symbiosis-block/internal/endpoint"
)

func NewNonBlockingGRPCServer() *nonBlockingGRPCServer {
	return &nonBlockingGRPCServer{}
}

// NonBlocking server
type nonBlockingGRPCServer struct {
	wg      sync.WaitGroup
	server  *grpc.Server
	log     *logrus.Entry
	cleanup func()
}

func (s *nonBlockingGRPCServer) Start(ctx context.Context, endpoint string, ids csi.IdentityServer, cs csi.ControllerServer, ns csi.NodeServer) {

	s.wg.Add(1)

	s.serve(ctx, endpoint, ids, cs, ns)
}

func (s *nonBlockingGRPCServer) serve(ctx context.Context, ep string, ids csi.IdentityServer, cs csi.ControllerServer, ns csi.NodeServer) {
	listener, cleanup, err := endpoint.Listen(ep)
	if err != nil {
		s.log.Fatalf("Failed to listen: %v", err)
	}

	errHandler := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			s.log.WithError(err).WithField("method", info.FullMethod).Error("method failed")
		}
		return resp, err
	}

	opts := []grpc.ServerOption{
		grpc.UnaryInterceptor(errHandler),
	}
	server := grpc.NewServer(opts...)
	s.server = server
	s.cleanup = cleanup

	if ids != nil {
		csi.RegisterIdentityServer(server, ids)
	}
	if cs != nil {
		csi.RegisterControllerServer(server, cs)
	}
	if ns != nil {
		csi.RegisterNodeServer(server, ns)
	}

	s.log.Infof("Listening for connections on address: %#v", listener.Addr())

	var eg errgroup.Group
	eg.Go(func() error {
		go func() {
			<-ctx.Done()
			server.GracefulStop()
		}()
		return server.Serve(listener)
	})

	eg.Wait()

}
