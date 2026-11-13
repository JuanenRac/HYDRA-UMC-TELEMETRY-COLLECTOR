// =============================================================================
// HYDRA-UMC-TELEMETRY-COLLECTOR - src/deps_floor_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// The modules with published security fixes never go back below the patched release: go.mod keeps them at or
// above the floors below, and the Dockerfile builds with a Go toolchain new enough for go.mod.
// =============================================================================
package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// dependencyFloors lists the lowest release allowed for each module that had a security advisory.
var dependencyFloors = map[string]string{
	"google.golang.org/grpc": "1.83.1",
	"golang.org/x/net":       "0.55.0",
}

// versionParts turns "v1.83.1" into [1 83 1].
func versionParts(t *testing.T, v string) [3]int {
	t.Helper()
	var out [3]int
	parts := strings.Split(strings.TrimPrefix(v, "v"), ".")
	if len(parts) != 3 {
		t.Fatalf("version %q is not major.minor.patch", v)
	}
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			t.Fatalf("version %q: %v", v, err)
		}
		out[i] = n
	}
	return out
}

func versionLess(a, b [3]int) bool {
	for i := range a {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}

func readGoMod(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatalf("go.mod: %v", err)
	}
	return strings.ReplaceAll(string(raw), "\r\n", "\n")
}

func TestVulnerableModulesStayAtOrAboveTheirFixedRelease(t *testing.T) {
	mod := readGoMod(t)
	for name, floor := range dependencyFloors {
		re := regexp.MustCompile(`(?m)^\s*` + regexp.QuoteMeta(name) + ` (v[0-9]+\.[0-9]+\.[0-9]+)`)
		m := re.FindStringSubmatch(mod)
		if m == nil {
			t.Errorf("%s is not listed in go.mod", name)
			continue
		}
		if versionLess(versionParts(t, m[1]), versionParts(t, floor)) {
			t.Errorf("%s is at %s, below the fixed release %s", name, m[1], floor)
		}
	}
}

func TestDockerfileToolchainIsNotOlderThanGoMod(t *testing.T) {
	mod := regexp.MustCompile(`(?m)^go ([0-9]+)\.([0-9]+)`).FindStringSubmatch(readGoMod(t))
	if mod == nil {
		t.Fatal("go.mod has no go directive")
	}
	raw, err := os.ReadFile(filepath.Join("..", "Dockerfile"))
	if err != nil {
		t.Fatalf("Dockerfile: %v", err)
	}
	img := regexp.MustCompile(`(?m)^FROM golang:([0-9]+)\.([0-9]+)`).FindStringSubmatch(string(raw))
	if img == nil {
		t.Fatal("Dockerfile has no golang build image")
	}
	want := [3]int{}
	have := [3]int{}
	want[0], _ = strconv.Atoi(mod[1])
	want[1], _ = strconv.Atoi(mod[2])
	have[0], _ = strconv.Atoi(img[1])
	have[1], _ = strconv.Atoi(img[2])
	if versionLess(have, want) {
		t.Errorf("the build image golang:%s.%s is older than go.mod (go %s.%s)", img[1], img[2], mod[1], mod[2])
	}
}
