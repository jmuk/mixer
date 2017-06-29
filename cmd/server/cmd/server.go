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

package cmd

import (
	"crypto/tls"
	"crypto/x509"
	_ "expvar" // For /debug/vars registration. Note: temporary, NOT for general use
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	bt "github.com/opentracing/basictracer-go"
	ot "github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	galleypb "istio.io/api/galley/v1"
	mixerpb "istio.io/api/mixer/v1"
	"istio.io/mixer/adapter"
	"istio.io/mixer/cmd/shared"
	"istio.io/mixer/pkg/adapterManager"
	"istio.io/mixer/pkg/api"
	"istio.io/mixer/pkg/aspect"
	"istio.io/mixer/pkg/config"
	"istio.io/mixer/pkg/expr"
	"istio.io/mixer/pkg/pool"
	"istio.io/mixer/pkg/tracing"
	"istio.io/mixer/pkg/version"
)

const (
	metricsPath = "/metrics"
	versionPath = "/version"
)

type serverArgs struct {
	maxMessageSize                uint
	maxConcurrentStreams          uint
	apiWorkerPoolSize             int
	adapterWorkerPoolSize         int
	expressionEvalCacheSize       int
	port                          uint16
	configAPIPort                 uint16
	singleThreaded                bool
	compressedPayload             bool
	enableTracing                 bool
	serverCertFile                string
	serverKeyFile                 string
	clientCertFiles               string
	configStoreURL                string
	configFetchIntervalSec        uint
	configIdentityAttribute       string
	configIdentityAttributeDomain string
	monitoringPort                uint16
	galleyEndpoint                string

	// @deprecated
	serviceConfigFile string
	// @deprecated
	globalConfigFile string
}

func serverCmd(printf, fatalf shared.FormatFn) *cobra.Command {
	sa := &serverArgs{}
	serverCmd := cobra.Command{
		Use:   "server",
		Short: "Starts Mixer as a server",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if sa.apiWorkerPoolSize <= 0 {
				return fmt.Errorf("api worker pool size must be >= 0 and <= 2^31-1, got pool size %d", sa.apiWorkerPoolSize)
			}

			if sa.adapterWorkerPoolSize <= 0 {
				return fmt.Errorf("adapter worker pool size must be >= 0 and <= 2^31-1, got pool size %d", sa.adapterWorkerPoolSize)
			}

			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			runServer(sa, printf, fatalf)
		},
	}

	// TODO: need to pick appropriate defaults for all these settings below

	serverCmd.PersistentFlags().Uint16VarP(&sa.port, "port", "p", 9091, "TCP port to use for Mixer's gRPC API")
	serverCmd.PersistentFlags().Uint16Var(&sa.monitoringPort, "monitoringPort", 9093, "HTTP port to use for the exposing mixer self-monitoring information")
	serverCmd.PersistentFlags().Uint16VarP(&sa.configAPIPort, "configAPIPort", "", 9094, "HTTP port to use for Mixer's Configuration API")
	serverCmd.PersistentFlags().UintVarP(&sa.maxMessageSize, "maxMessageSize", "", 1024*1024, "Maximum size of individual gRPC messages")
	serverCmd.PersistentFlags().UintVarP(&sa.maxConcurrentStreams, "maxConcurrentStreams", "", 1024, "Maximum number of outstanding RPCs per connection")
	serverCmd.PersistentFlags().IntVarP(&sa.apiWorkerPoolSize, "apiWorkerPoolSize", "", 1024, "Max number of goroutines in the API worker pool")
	serverCmd.PersistentFlags().IntVarP(&sa.adapterWorkerPoolSize, "adapterWorkerPoolSize", "", 1024, "Max number of goroutines in the adapter worker pool")
	// TODO: what is the right default value for expressionEvalCacheSize.
	serverCmd.PersistentFlags().IntVarP(&sa.expressionEvalCacheSize, "expressionEvalCacheSize", "", expr.DefaultCacheSize,
		"Number of entries in the expression cache")
	serverCmd.PersistentFlags().BoolVarP(&sa.singleThreaded, "singleThreaded", "", false,
		"If true, each request to Mixer will be executed in a single go routine (useful for debugging)")
	serverCmd.PersistentFlags().BoolVarP(&sa.compressedPayload, "compressedPayload", "", false, "Whether to compress gRPC messages")
	serverCmd.PersistentFlags().StringVar(&sa.galleyEndpoint, "galleyEndpoint", "istio-galley:9096", "The endpoint of the galley server")

	serverCmd.PersistentFlags().StringVarP(&sa.serverCertFile, "serverCertFile", "", "", "The TLS cert file")
	_ = serverCmd.MarkPersistentFlagFilename("serverCertFile")

	serverCmd.PersistentFlags().StringVarP(&sa.serverKeyFile, "serverKeyFile", "", "", "The TLS key file")
	_ = serverCmd.MarkPersistentFlagFilename("serverKeyFile")

	serverCmd.PersistentFlags().StringVarP(&sa.clientCertFiles, "clientCertFiles", "", "", "A set of comma-separated client X509 cert files")

	// TODO: implement an option to specify how traces are reported (hardcoded to report to stdout right now).
	serverCmd.PersistentFlags().BoolVarP(&sa.enableTracing, "trace", "", false, "Whether to trace rpc executions")

	serverCmd.PersistentFlags().StringVarP(&sa.configStoreURL, "configStoreURL", "", "",
		"URL of the config store. May be fs:// for file system, or redis:// for redis url")

	// Hide configIdentityAttribute and configIdentityAttributeDomain until we have a need to expose it.
	// These parameters ensure that rest of Mixer makes no assumptions about specific identity attribute.
	// Rules selection is based on scopes.
	serverCmd.PersistentFlags().StringVarP(&sa.configIdentityAttribute, "configIdentityAttribute", "", "target.service",
		"Attribute that is used to identify applicable scopes.")
	if err := serverCmd.PersistentFlags().MarkHidden("configIdentityAttribute"); err != nil {
		fatalf("unable to hide: %v", err)
	}
	serverCmd.PersistentFlags().StringVarP(&sa.configIdentityAttributeDomain, "configIdentityAttributeDomain", "", "svc.cluster.local",
		"The domain to which all values of the configIdentityAttribute belong. For kubernetes services it is svc.cluster.local")
	if err := serverCmd.PersistentFlags().MarkHidden("configIdentityAttributeDomain"); err != nil {
		fatalf("unable to hide: %v", err)
	}

	// serviceConfig and gobalConfig are for compatibility only
	serverCmd.PersistentFlags().StringVarP(&sa.serviceConfigFile, "serviceConfigFile", "", "", "Combined Service Config")
	serverCmd.PersistentFlags().StringVarP(&sa.globalConfigFile, "globalConfigFile", "", "", "Global Config")

	serverCmd.PersistentFlags().UintVarP(&sa.configFetchIntervalSec, "configFetchInterval", "", 5, "Configuration fetch interval in seconds")
	return &serverCmd
}

func runServer(sa *serverArgs, printf, fatalf shared.FormatFn) {
	var err error
	apiPoolSize := sa.apiWorkerPoolSize
	adapterPoolSize := sa.adapterWorkerPoolSize
	expressionEvalCacheSize := sa.expressionEvalCacheSize

	gp := pool.NewGoroutinePool(apiPoolSize, sa.singleThreaded)
	gp.AddWorkers(apiPoolSize)
	defer gp.Close()

	adapterGP := pool.NewGoroutinePool(adapterPoolSize, sa.singleThreaded)
	adapterGP.AddWorkers(adapterPoolSize)
	defer adapterGP.Close()

	// get aspect registry with proper aspect --> api mappings
	eval, err := expr.NewCEXLEvaluator(expressionEvalCacheSize)
	if err != nil {
		fatalf("Failed to create expression evaluator with cache size %d: %v", expressionEvalCacheSize, err)
	}
	adapterMgr := adapterManager.NewManager(adapter.Inventory(), aspect.Inventory(), eval, gp, adapterGP)
	var watchConn *grpc.ClientConn
	// TODO: should use secure connection?
	watchConn, err = grpc.Dial(
		sa.galleyEndpoint,
		grpc.WithInsecure(),
		grpc.WithDecompressor(grpc.NewGZIPDecompressor()),
		grpc.WithCompressor(grpc.NewGZIPCompressor()),
	)
	if err != nil {
		fatalf("Failed to connect to the galley server")
	}
	configManager := config.NewManager(eval, adapterMgr.AspectValidatorFinder, adapterMgr.BuilderValidatorFinder,
		adapterMgr.SupportedKinds,
		galleypb.NewWatcherClient(watchConn),
		sa.configIdentityAttribute,
		sa.configIdentityAttributeDomain)

	var serverCert *tls.Certificate
	var clientCerts *x509.CertPool

	if sa.serverCertFile != "" && sa.serverKeyFile != "" {
		var sc tls.Certificate
		if sc, err = tls.LoadX509KeyPair(sa.serverCertFile, sa.serverKeyFile); err != nil {
			fatalf("Failed to load server certificate and server key: %v", err)
		}
		serverCert = &sc
	}

	if sa.clientCertFiles != "" {
		clientCerts = x509.NewCertPool()
		for _, clientCertFile := range strings.Split(sa.clientCertFiles, ",") {
			var pem []byte
			if pem, err = ioutil.ReadFile(clientCertFile); err != nil {
				fatalf("Failed to load client certificate: %v", err)
			}
			clientCerts.AppendCertsFromPEM(pem)
		}
	}

	var listener net.Listener
	// get the network stuff setup
	if listener, err = net.Listen("tcp", fmt.Sprintf(":%d", sa.port)); err != nil {
		fatalf("Unable to listen on socket: %v", err)
	}

	// construct the gRPC options

	var grpcOptions []grpc.ServerOption
	grpcOptions = append(grpcOptions, grpc.MaxConcurrentStreams(uint32(sa.maxConcurrentStreams)))
	grpcOptions = append(grpcOptions, grpc.MaxMsgSize(int(sa.maxMessageSize)))

	if sa.compressedPayload {
		grpcOptions = append(grpcOptions, grpc.RPCCompressor(grpc.NewGZIPCompressor()))
		grpcOptions = append(grpcOptions, grpc.RPCDecompressor(grpc.NewGZIPDecompressor()))
	}

	if serverCert != nil {
		// enable TLS
		tlsConfig := &tls.Config{}
		tlsConfig.Certificates = []tls.Certificate{*serverCert}

		if clientCerts != nil {
			// enable TLS mutual auth
			tlsConfig.ClientCAs = clientCerts
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		}
		tlsConfig.BuildNameToCertificate()

		grpcOptions = append(grpcOptions, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}

	var interceptors []grpc.UnaryServerInterceptor
	if sa.enableTracing {
		tracer := bt.New(tracing.IORecorder(os.Stdout))
		ot.InitGlobalTracer(tracer)
		interceptors = append(interceptors, otgrpc.OpenTracingServerInterceptor(tracer))
	}

	// setup server prometheus monitoring (as final interceptor in chain)
	interceptors = append(interceptors, grpc_prometheus.UnaryServerInterceptor)
	grpc_prometheus.EnableHandlingTimeHistogram()
	grpcOptions = append(grpcOptions, grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(interceptors...)))

	configManager.Register(adapterMgr)
	configManager.Start()

	var monitoringListener net.Listener
	// get the network stuff setup
	if monitoringListener, err = net.Listen("tcp", fmt.Sprintf(":%d", sa.monitoringPort)); err != nil {
		fatalf("Unable to listen on socket: %v", err)
	}

	// NOTE: this is a temporary solution for provide bare-bones debug functionality
	// for mixer. a full design / implementation of self-monitoring and reporting
	// is coming. that design will include proper coverage of statusz/healthz type
	// functionality, in addition to how mixer reports its own metrics.
	http.Handle(metricsPath, promhttp.Handler())
	http.HandleFunc(versionPath, func(out http.ResponseWriter, req *http.Request) {
		if _, verErr := out.Write([]byte(version.Info.String())); verErr != nil {
			printf("error printing version info: %v", verErr)
		}
	})
	monitoring := &http.Server{Addr: fmt.Sprintf(":%d", sa.monitoringPort)}
	printf("Starting self-monitoring on port %d", sa.monitoringPort)
	go func() {
		if monErr := monitoring.Serve(monitoringListener.(*net.TCPListener)); monErr != nil {
			printf("monitoring server error: %v", monErr)
		}
	}()

	// get everything wired up
	gs := grpc.NewServer(grpcOptions...)
	s := api.NewGRPCServer(adapterMgr, gp)
	mixerpb.RegisterMixerServer(gs, s)

	printf("Istio Mixer: %s", version.Info)
	printf("Starting gRPC server on port %v", sa.port)

	if err = gs.Serve(listener); err != nil {
		fatalf("Failed serving gRPC server: %v", err)
	}
}
