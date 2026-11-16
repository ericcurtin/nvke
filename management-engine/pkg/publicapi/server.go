// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package publicapi

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"k8s.io/client-go/discovery"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"

	"github.com/dsx-ai-factory/nvke/management-engine/pkg/resource"
)

const (
	// LivenessPath identifies the process liveness endpoint.
	LivenessPath = "/healthz"
	// ReadinessPath identifies the datastore readiness endpoint.
	ReadinessPath = "/readyz"
)

// MountHealth installs liveness and readiness probes using the supplied datastore check.
func MountHealth(mux *http.ServeMux, checkReadiness func(context.Context) error) {
	mux.HandleFunc("GET "+LivenessPath, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET "+ReadinessPath, func(w http.ResponseWriter, r *http.Request) {
		if err := checkReadiness(r.Context()); err != nil {
			http.Error(w, "datastore unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
}

// Run parses common flags, mounts the command's public surface, and serves it until shutdown.
func Run(version string, surface Surface, clientConfig resource.ClientConfig) error {
	address := flag.String("listen", ":8080", "HTTP listen address")
	contextName := flag.String("context", "", "Datastore kubeconfig context")
	showVersion := flag.Bool("version", false, "Print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return nil
	}

	cfg, err := config.GetConfigWithContext(*contextName)
	if err != nil {
		return fmt.Errorf("load datastore configuration: %w", err)
	}
	cfg.Timeout = 5 * time.Second
	disco, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return fmt.Errorf("configure readiness discovery: %w", err)
	}
	clientConfig.Datastore = cfg
	clientConfig.Surface = surface.Name
	clientConfig.Naming = surface.Naming
	mux := http.NewServeMux()
	for _, registration := range surface.Registrations {
		if err := registration.Mount(mux, clientConfig); err != nil {
			return fmt.Errorf("mount %s surface: %w", clientConfig.Surface, err)
		}
	}
	MountHealth(mux, func(ctx context.Context) error {
		_, err := disco.ServerResourcesForGroupVersionWithContext(ctx, "service.nvke.nvidia.com/v1")
		return err
	})
	server := &http.Server{
		Addr: *address, Handler: resource.WithLimits(mux, resource.ServerOptions{Naming: clientConfig.Naming}),
		ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 35 * time.Second, IdleTimeout: time.Minute,
	}
	ctx := signals.SetupSignalHandler()
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.ListenAndServe() }()
	slog.Info("serving REST API", "surface", clientConfig.Surface, "address", *address)
	select {
	case err := <-serveErr:
		return fmt.Errorf("serve HTTP: %w", err)
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shut down HTTP: %w", err)
		}
		return nil
	}
}
