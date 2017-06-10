// Copyright 2017 Istio Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	bt "github.com/opentracing/basictracer-go"
	ot "github.com/opentracing/opentracing-go"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	configpb "istio.io/api/config/v1"
	"istio.io/mixer/cmd/shared"
	"istio.io/mixer/pkg/config"
	"istio.io/mixer/pkg/tracing"
)

var (
	configStoreURL string
	port           uint16
	enableTracing  bool
	duration       int
	maxMessageSize uint
)

func runServer() {
	var listener net.Listener
	var err error
	// get the network stuff setup
	if listener, err = net.Listen("tcp", fmt.Sprintf(":%d", port)); err != nil {
		shared.Fatalf("Unable to listen on socket: %v", err)
	}

	s, err := config.NewConfigAPIServer(configStoreURL, time.Duration(duration)*time.Millisecond)
	if err != nil {
		shared.Fatalf("can't establish the connection with the server %s: %v", configStoreURL, err)
	}

	grpcOptions := []grpc.ServerOption{
		grpc.MaxMsgSize(int(maxMessageSize)),
		grpc.RPCCompressor(grpc.NewGZIPCompressor()),
		grpc.RPCDecompressor(grpc.NewGZIPDecompressor()),
	}

	// TODO: cert

	if enableTracing {
		tracer := bt.New(tracing.IORecorder(os.Stdout))
		ot.InitGlobalTracer(tracer)
		grpcOptions = append(grpcOptions, grpc.UnaryInterceptor(otgrpc.OpenTracingServerInterceptor(tracer)))
	}
	gs := grpc.NewServer(grpcOptions...)
	configpb.RegisterServiceServer(gs, s)

	if err = gs.Serve(listener); err != nil {
		shared.Fatalf("Failed serving gRPC server: %v", err)
	}
}

func main() {
	rootCmd := &cobra.Command{
		Use:   "configserver",
		Short: "configserver abstracts the storage(s) for various configurations",
		Long: "configserver abstracts the storage(s) for the various configurations for Istio components\n" +
			"like Mixer/Pilot, and offers the interface for the stored data.",
		Run: func(cmd *cobra.Command, args []string) {
			runServer()
		},
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if configStoreURL == "" {
				return fmt.Errorf("configStoreURL is not specified")
			}
			return nil
		},
	}
	rootCmd.PersistentFlags().Uint16VarP(&port, "port", "p", 9099, "TCP port to use for configserver's gRPC API")
	rootCmd.PersistentFlags().IntVar(&duration, "interval", 500, "The interval to emit changes")
	rootCmd.PersistentFlags().StringVar(&configStoreURL, "configStoreURL", "", "The URL for the backend config store")
	rootCmd.PersistentFlags().BoolVar(&enableTracing, "enable-tracing", false, "enable tracing")
	rootCmd.PersistentFlags().UintVarP(&maxMessageSize, "maxMessageSize", "", 1024*1024, "Maximum size of individual gRPC messages")

	rootCmd.PersistentFlags().AddGoFlagSet(flag.CommandLine)

	// hack to make flag.Parsed return true such that glog is happy
	// about the flags having been parsed
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	/* #nosec */
	_ = fs.Parse([]string{})
	flag.CommandLine = fs

	if err := rootCmd.Execute(); err != nil {
		os.Exit(-1)
	}
}
