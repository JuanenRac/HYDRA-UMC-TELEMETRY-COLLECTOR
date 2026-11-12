// =============================================================================
// HYDRA-UMC-TELEMETRY-COLLECTOR - src/dockerfile_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// The Dockerfile agrees with the rest of the repository: it builds the program of this module, copies files that
// exist, listens on the port of the systemd unit, runs without root and keeps the token out of the image. Docker
// itself is not needed.
// =============================================================================
package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// dockerInstructions returns the instructions of the Dockerfile without blank lines and comments.
func dockerInstructions(t *testing.T) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "Dockerfile"))
	if err != nil {
		t.Fatalf("Dockerfile: %v", err)
	}
	var out []string
	for _, line := range strings.Split(strings.ReplaceAll(string(raw), "\r\n", "\n"), "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") {
			out = append(out, line)
		}
	}
	return out
}

func TestDockerfileCopiesOnlyWhatExists(t *testing.T) {
	for _, line := range dockerInstructions(t) {
		if !strings.HasPrefix(line, "COPY ") || strings.Contains(line, "--from=") {
			continue
		}
		fields := strings.Fields(line)[1:]
		for _, source := range fields[:len(fields)-1] {
			if _, err := os.Stat(filepath.Join("..", source)); err != nil {
				t.Errorf("COPY source %q does not exist: %v", source, err)
			}
		}
	}
}

func TestDockerfileListensOnThePortOfTheServiceUnit(t *testing.T) {
	units, _ := filepath.Glob(filepath.Join("..", "systemd", "*.service"))
	if len(units) != 1 {
		t.Fatalf("expected one systemd unit, got %v", units)
	}
	unit, err := os.ReadFile(units[0])
	if err != nil {
		t.Fatal(err)
	}
	match := regexp.MustCompile(`--addr 127\.0\.0\.1:(\d+)`).FindSubmatch(unit)
	if match == nil {
		t.Fatal("the service unit names no --addr on loopback")
	}
	port := string(match[1])
	joined := "\n" + strings.Join(dockerInstructions(t), "\n")
	if !strings.Contains(joined, "\nEXPOSE "+port) {
		t.Errorf("the image does not expose port %s", port)
	}
	if !strings.Contains(joined, `CMD ["--addr", "0.0.0.0:`+port+`"]`) {
		t.Errorf("the image does not listen on every interface on port %s", port)
	}
}

func TestDockerfileRunsWithoutRootAndHoldsNoSecret(t *testing.T) {
	user := false
	for _, line := range dockerInstructions(t) {
		if strings.HasPrefix(line, "USER ") && !strings.Contains(line, "root") {
			user = true
		}
		upper := strings.ToUpper(line)
		if (strings.HasPrefix(line, "ENV ") || strings.HasPrefix(line, "ARG ")) && strings.Contains(upper, "TOKEN") {
			t.Errorf("a token is baked into the image: %q", line)
		}
	}
	if !user {
		t.Error("the image never leaves root")
	}
}

func TestDockerfileEntrypointIsTheBinaryItBuilds(t *testing.T) {
	built, entry := "", ""
	for _, line := range dockerInstructions(t) {
		if m := regexp.MustCompile(`go build .* -o (\S+)`).FindStringSubmatch(line); m != nil {
			built = filepath.Base(m[1])
		}
		if m := regexp.MustCompile(`^ENTRYPOINT \["([^"]+)"\]`).FindStringSubmatch(line); m != nil {
			entry = m[1]
		}
	}
	if built == "" || built != entry {
		t.Errorf("built %q but starts %q", built, entry)
	}
}

func TestDockerfileHealthCheckDoesNotDependOnAClosedRoute(t *testing.T) {
	for _, line := range dockerInstructions(t) {
		if strings.HasPrefix(line, "HEALTHCHECK") {
			if strings.Contains(line, "/health") {
				t.Error("GET /health is closed by default; the check must only open the port")
			}
			return
		}
	}
	t.Error("no HEALTHCHECK")
}
