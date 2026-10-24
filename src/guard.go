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
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// metricsTokenEnv supplies the bearer token GET /stats and GET /metrics require.
const metricsTokenEnv = "HYDRA_UMC_TELEMETRY_COLLECTOR_METRICS_TOKEN"

// metricsTokenProblem applies the same minimum length as the ingest token.
func metricsTokenProblem(token string) string {
	if token != "" && len(token) < minTokenLength {
		return "the metrics token must be at least 16 characters"
	}
	return ""
}

// parseStatuses reads the -datalake-permanent-statuses list ("401,403,404") into a set. Only
// client-error statuses can be permanent; 2xx and 5xx are refused so a typo cannot discard
// samples that a retry would deliver.
func parseStatuses(list string) (map[int]bool, string) {
	if strings.TrimSpace(list) == "" {
		return nil, ""
	}
	out := map[int]bool{}
	for _, part := range strings.Split(list, ",") {
		code, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || code < 400 || code > 499 {
			return nil, fmt.Sprintf("-datalake-permanent-statuses: %q is not an HTTP client-error status (400-499)", strings.TrimSpace(part))
		}
		out[code] = true
	}
	return out, ""
}

// parsePriority reads the -priority list ("fault=9,motor_temp=5"); an empty list means no policy.
func parsePriority(list string) (map[string]int, string) {
	if strings.TrimSpace(list) == "" {
		return nil, ""
	}
	out := map[string]int{}
	for _, part := range strings.Split(list, ",") {
		kind, level, found := strings.Cut(strings.TrimSpace(part), "=")
		n, err := strconv.Atoi(strings.TrimSpace(level))
		if !found || strings.TrimSpace(kind) == "" || err != nil {
			return nil, fmt.Sprintf("-priority: %q is not kind=level", strings.TrimSpace(part))
		}
		out[strings.TrimSpace(kind)] = n
	}
	return out, ""
}

// tokenEnv is the environment variable that supplies the ingest bearer token.
const tokenEnv = "HYDRA_UMC_TELEMETRY_COLLECTOR_TOKEN"

// datalakeTokenEnv supplies the bearer token sent to HYDRA-UMC-DATALAKE.
const datalakeTokenEnv = "HYDRA_UMC_DATALAKE_TOKEN"

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

// flagProblem returns why the numeric settings cannot be used, or "" when they
// are fine: a negative batch size makes the flush goroutine panic, zero never
// drains the buffer, and a non-positive interval panics the ticker.
func flagProblem(bufferCap, batchSize int, flushInterval time.Duration) string {
	if bufferCap < 1 {
		return "-buffer must be at least 1"
	}
	if batchSize < 1 {
		return "-batch-size must be at least 1"
	}
	if flushInterval <= 0 {
		return "-flush-interval must be a positive duration"
	}
	return ""
}

// mqttPasswordEnv and mqttUserEnv supply the credentials of the optional MQTT input.
const (
	mqttUserEnv     = "HYDRA_UMC_TELEMETRY_MQTT_USER"
	mqttPasswordEnv = "HYDRA_UMC_TELEMETRY_MQTT_PASSWORD"
)

// mqttProblem returns why the MQTT input cannot be started as asked, or "" when it can. Credentials sent over a plain connection
// to a machine other than this one can be read by anyone on the way: that needs tls:// or the explicit -mqtt-allow-plaintext.
func mqttProblem(rawURL, username string, allowPlaintext bool) string {
	if rawURL == "" {
		return ""
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		return "-mqtt-url must look like tcp://127.0.0.1:1883 or tls://broker:8883"
	}
	switch strings.ToLower(u.Scheme) {
	case "tls", "ssl", "mqtts":
		return ""
	case "tcp", "mqtt":
	default:
		return "-mqtt-url must start with tcp:// or tls://"
	}
	host := u.Hostname()
	if ip := net.ParseIP(host); host == "localhost" || (ip != nil && ip.IsLoopback()) {
		return ""
	}
	if !allowPlaintext {
		return "refusing to talk MQTT in the clear to " + host + ": use tls:// (with -mqtt-ca-file for a private authority) or pass -mqtt-allow-plaintext on a network you trust"
	}
	return ""
}
