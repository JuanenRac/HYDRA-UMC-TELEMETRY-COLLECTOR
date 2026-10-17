// =============================================================================
// HYDRA-UMC-TELEMETRY-COLLECTOR - src/guard.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
// =============================================================================
// Startup guard: the ingest routes write into the factory's history, so the
// collector refuses to listen beyond loopback unless a bearer token protects
// them or the operator explicitly accepts an open endpoint.
package main

import (
	"net"
	"strings"
)

// tokenEnv is the environment variable that supplies the ingest bearer token.
const tokenEnv = "HYDRA_UMC_TELEMETRY_COLLECTOR_TOKEN"

// minTokenLength is the shortest bearer token accepted; shorter ones are
// guessable, so they are refused instead of silently weakening the check.
const minTokenLength = 16

// isLoopbackAddr reports whether a listen address binds loopback only. An
// empty host (":8092") listens on every interface, so it is not loopback.
func isLoopbackAddr(addr string) bool {
	host := addr
	if h, _, err := net.SplitHostPort(addr); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	if host == "" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// startupProblem returns why the collector must not start with this
// configuration, or "" when it is fine.
func startupProblem(addr, token string, allowAnonymous bool) string {
	if token != "" && len(token) < minTokenLength {
		return "the bearer token must be at least 16 characters"
	}
	if !isLoopbackAddr(addr) && token == "" && !allowAnonymous {
		return "refusing to listen on " + addr + " without a token: set -token (or $" + tokenEnv + "), " +
			"or pass -allow-anonymous to expose the ingest routes openly"
	}
	return ""
}
