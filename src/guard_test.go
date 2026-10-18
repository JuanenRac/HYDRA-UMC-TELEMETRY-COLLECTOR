// =============================================================================
// HYDRA-UMC-TELEMETRY-COLLECTOR - src/guard_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
// =============================================================================
package main

import (
	"strings"
	"testing"
	"time"
)

func TestIsLoopbackAddr(t *testing.T) {
	cases := map[string]bool{
		"127.0.0.1:8092": true,
		"127.0.0.1":      true,
		"[::1]:8092":     true,
		"localhost:8092": true,
		"LOCALHOST:8092": true,
		"0.0.0.0:8092":   false,
		":8092":          false,
		"[::]:8092":      false,
		"192.168.0.5:80": false,
		"example.com:80": false,
		"":               false,
	}
	for addr, want := range cases {
		if got := isLoopbackAddr(addr); got != want {
			t.Errorf("isLoopbackAddr(%q) = %v, want %v", addr, got, want)
		}
	}
}

func TestStartupProblem(t *testing.T) {
	long := strings.Repeat("a", 16)
	cases := []struct {
		name           string
		addr, token    string
		allowAnonymous bool
		wantProblem    bool
	}{
		{"loopback without token is the default", "127.0.0.1:8092", "", false, false},
		{"network without token is refused", "0.0.0.0:8092", "", false, true},
		{"network with a token is fine", "0.0.0.0:8092", long, false, false},
		{"network anonymous only when asked", "0.0.0.0:8092", "", true, false},
		{"a short token is refused even on loopback", "127.0.0.1:8092", "short", false, true},
		{"a bare port listens everywhere", ":8092", "", false, true},
	}
	for _, c := range cases {
		got := startupProblem(c.addr, c.token, c.allowAnonymous)
		if (got != "") != c.wantProblem {
			t.Errorf("%s: startupProblem = %q, wantProblem %v", c.name, got, c.wantProblem)
		}
	}
}

func TestFlagProblem(t *testing.T) {
	if p := flagProblem(10000, 500, 2*time.Second); p != "" {
		t.Fatalf("the defaults are fine, got %q", p)
	}
	for _, c := range []struct {
		name          string
		buf, batch    int
		interval      time.Duration
		wantSubstring string
	}{
		{"zero buffer", 0, 500, time.Second, "-buffer"},
		{"negative batch", 10, -1, time.Second, "-batch-size"},
		{"zero batch", 10, 0, time.Second, "-batch-size"},
		{"zero interval", 10, 10, 0, "-flush-interval"},
		{"negative interval", 10, 10, -time.Second, "-flush-interval"},
	} {
		if p := flagProblem(c.buf, c.batch, c.interval); !strings.Contains(p, c.wantSubstring) {
			t.Errorf("%s: got %q, want it to name %s", c.name, p, c.wantSubstring)
		}
	}
}
