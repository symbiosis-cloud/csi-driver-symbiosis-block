/*
Copyright 2020 The Kubernetes Authors.

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

package endpoint

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
)

func Listen(endpoint string) (net.Listener, func(), error) {
	u, err := url.Parse(endpoint)
	grpcAddr := path.Join(u.Host, filepath.FromSlash(u.Path))
	if err != nil {
		return nil, nil, err
	}
	if err := os.Remove(grpcAddr); err != nil && !os.IsNotExist(err) {
		return nil, nil, fmt.Errorf("failed to remove unix domain socket file %s, error: %s", grpcAddr, err)
	}

	cleanup := func() {}

	l, err := net.Listen("unix", grpcAddr)
	return l, cleanup, err
}
