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

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path"
	"syscall"

	driver "github.com/symbiosis-cloud/csi-driver-symbiosis-block/driver"
	"github.com/symbiosis-cloud/csi-driver-symbiosis-block/internal/proxy"
)

func init() {
	flag.Set("logtostderr", "true")
}

var (
	version = "dev"
)

func main() {
	cfg := driver.Config{
		VendorVersion: version,
	}

	flag.StringVar(&cfg.Endpoint, "endpoint", "unix://tmp/csi.sock", "CSI endpoint")
	flag.StringVar(&cfg.Token, "token", "", "Symbiosis access token")
	flag.StringVar(&cfg.DriverName, "driver-name", "block.csi.symbiosis.host", "name of the driver")
	flag.StringVar(&cfg.NodeID, "node-id", "", "node id")
	flag.StringVar(&cfg.ClusterID, "cluster-id", "", "cluster id")
	flag.StringVar(&cfg.ApiEndpoint, "api-endpoint", "https://staging.api.symbiosis.host/", "Symbiosis api endpoint")
	showVersion := flag.Bool("version", false, "Show version.")
	// The proxy-endpoint option is intended to used by the Kubernetes E2E test suite
	// for proxying incoming calls to the embedded mock CSI driver.
	proxyEndpoint := flag.String("proxy-endpoint", "", "Instead of running the CSI driver code, just proxy connections from csiEndpoint to the given listening socket.")

	flag.Parse()

	if *showVersion {
		baseName := path.Base(os.Args[0])
		fmt.Println(baseName, version)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	if *proxyEndpoint != "" {
		defer cancel()
		closer, err := proxy.Run(ctx, cfg.Endpoint, *proxyEndpoint)
		if err != nil {
			log.Fatalf("failed to run proxy: %v", err)
		}
		defer closer.Close()

		// Wait for signal
		sigc := make(chan os.Signal, 1)
		sigs := []os.Signal{
			syscall.SIGTERM,
			syscall.SIGHUP,
			syscall.SIGINT,
			syscall.SIGQUIT,
		}
		signal.Notify(sigc, sigs...)

		<-sigc
		return
	}

	driver, err := driver.NewDriver(cfg)
	if err != nil {
		fmt.Printf("Failed to initialize driver: %s", err.Error())
		os.Exit(1)
	}

	if err := driver.Run(ctx); err != nil {
		fmt.Printf("Failed to run driver: %s", err.Error())
		os.Exit(1)

	}
}
