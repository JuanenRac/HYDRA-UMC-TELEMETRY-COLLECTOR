// HYDRA-UMC-TELEMETRY-COLLECTOR - api/identity_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// The identity of HYDRA-UMC-SERVER: the verifier, and the HTTP surface with the key off (nothing changes) and on (nothing is open, a viewer only reads).
package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/collector"
)

var identityKey = []byte("0123456789abcdef0123456789abcdef")

func encodedPart(v any) string {
	data, _ := json.Marshal(v)
	return base64.RawURLEncoding.EncodeToString(data)
}

func tokenWith(key []byte, alg string, claims map[string]any) string {
	head := encodedPart(map[string]string{"alg": alg, "typ": "JWT"})
	body := encodedPart(claims)
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(head + "." + body))
	return head + "." + body + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func claimsFor(role string, now time.Time) map[string]any {
	return map[string]any{"username": "ana", "role": role, "exp": now.Add(10 * time.Minute).Unix()}
}

func signedFor(role string) string {
	return tokenWith(identityKey, "HS256", claimsFor(role, time.Now()))
}

func TestVerifyServerTokenGivesTheUserAndTheRole(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	identity, err := VerifyServerToken(tokenWith(identityKey, "HS256", claimsFor("operator", now)), identityKey, now)
	if err != nil || identity.Username != "ana" || identity.Role != "operator" || !identity.CanWrite() {
		t.Fatalf("identity = %+v, err = %v", identity, err)
	}
	viewer, err := VerifyServerToken(tokenWith(identityKey, "HS256", claimsFor("viewer", now)), identityKey, now)
	if err != nil || viewer.CanWrite() {
		t.Fatalf("a viewer must read only: %+v %v", viewer, err)
	}
}

func TestVerifyServerTokenRefusesEveryDefect(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	other := []byte("another-key-of-more-than-32-characters!")
	with := func(extra map[string]any) map[string]any {
		claims := claimsFor("admin", now)
		for k, v := range extra {
			claims[k] = v
		}
		return claims
	}
	cases := map[string]string{
		"none algorithm":   tokenWith(identityKey, "none", claimsFor("admin", now)),
		"other algorithm":  tokenWith(identityKey, "HS512", claimsFor("admin", now)),
		"wrong key":        tokenWith(other, "HS256", claimsFor("admin", now)),
		"expired":          tokenWith(identityKey, "HS256", with(map[string]any{"exp": now.Add(-5 * time.Second).Unix()})),
		"no expiry":        tokenWith(identityKey, "HS256", map[string]any{"username": "ana", "role": "admin"}),
		"text expiry":      tokenWith(identityKey, "HS256", with(map[string]any{"exp": "tomorrow"})),
		"not yet":          tokenWith(identityKey, "HS256", with(map[string]any{"nbf": now.Add(5 * time.Minute).Unix()})),
		"issued in future": tokenWith(identityKey, "HS256", with(map[string]any{"iat": now.Add(5 * time.Minute).Unix()})),
		"no user":          tokenWith(identityKey, "HS256", with(map[string]any{"username": ""})),
		"unknown role":     tokenWith(identityKey, "HS256", with(map[string]any{"role": "root"})),
		"not a token":      "abc",
		"empty parts":      "..",
		"empty":            "",
		"too long":         strings.Repeat("a.", 3000),
	}
	for label, token := range cases {
		if _, err := VerifyServerToken(token, identityKey, now); err == nil {
			t.Errorf("%s: the token was accepted", label)
		}
	}
}

func TestLoadIdentityKeyChecksTheLengthAndNamesNoKey(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "good.key")
	short := filepath.Join(dir, "short.key")
	if err := os.WriteFile(good, append(identityKey, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(short, []byte("too-short"), 0o600); err != nil {
		t.Fatal(err)
	}
	key, err := LoadIdentityKey(good)
	if err != nil || string(key) != string(identityKey) {
		t.Fatalf("a good key file was not read: %v", err)
	}
	if _, err := LoadIdentityKey(short); err == nil || strings.Contains(err.Error(), "too-short") {
		t.Fatalf("a short key must be refused without naming it: %v", err)
	}
	if _, err := LoadIdentityKey(filepath.Join(dir, "missing.key")); err == nil {
		t.Fatal("a missing file must be refused")
	}
}

func identityServer(opts Options) *Server {
	return NewWithOptions(collector.New(10, &memorySink{}), opts)
}

func request(s *Server, method, path, bearer string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(validWSBody))
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}

func TestIdentityOffByDefaultNothingChanges(t *testing.T) {
	s := identityServer(Options{Token: "0123456789abcdef", HealthOpen: true})
	if rec := request(s, http.MethodGet, "/stats", ""); rec.Code != http.StatusOK {
		t.Errorf("GET /stats = %d, want 200 while the identity is off", rec.Code)
	}
	if rec := request(s, http.MethodPost, "/ingest/ws", signedFor("admin")); rec.Code != http.StatusUnauthorized {
		t.Errorf("a token of the SERVER means nothing while the identity is off, got %d", rec.Code)
	}
}

func TestIdentityOnNothingIsOpenAndAViewerOnlyReads(t *testing.T) {
	s := identityServer(Options{IdentityKey: identityKey, HealthOpen: true})
	for _, path := range []string{"/stats", "/metrics", "/health"} {
		if rec := request(s, http.MethodGet, path, ""); rec.Code != http.StatusUnauthorized {
			t.Errorf("GET %s without a credential = %d, want 401 (not even from this machine)", path, rec.Code)
		}
		if rec := request(s, http.MethodGet, path, signedFor("viewer")); rec.Code != http.StatusOK {
			t.Errorf("GET %s as a viewer = %d, want 200", path, rec.Code)
		}
	}
	if rec := request(s, http.MethodPost, "/ingest/ws", ""); rec.Code != http.StatusUnauthorized {
		t.Errorf("POST /ingest/ws without a credential = %d, want 401", rec.Code)
	}
	if rec := request(s, http.MethodPost, "/ingest/ws", signedFor("viewer")); rec.Code != http.StatusUnauthorized {
		t.Errorf("POST /ingest/ws as a viewer = %d, want 401", rec.Code)
	}
	for _, role := range []string{"operator", "admin"} {
		if rec := request(s, http.MethodPost, "/ingest/ws", signedFor(role)); rec.Code != http.StatusAccepted {
			t.Errorf("POST /ingest/ws as %s = %d, want 202", role, rec.Code)
		}
	}
	forged := tokenWith([]byte("another-key-of-more-than-32-characters!"), "HS256", claimsFor("admin", time.Now()))
	if rec := request(s, http.MethodGet, "/stats", forged); rec.Code != http.StatusUnauthorized {
		t.Errorf("a token signed with another key = %d, want 401", rec.Code)
	}
}

func TestIdentityOnTheOwnTokensKeepWorking(t *testing.T) {
	s := identityServer(Options{Token: "0123456789abcdef", MetricsToken: "fedcba9876543210", IdentityKey: identityKey, HealthOpen: true})
	if rec := request(s, http.MethodPost, "/ingest/ws", "0123456789abcdef"); rec.Code != http.StatusAccepted {
		t.Errorf("the ingest token = %d, want 202", rec.Code)
	}
	if rec := request(s, http.MethodGet, "/stats", "fedcba9876543210"); rec.Code != http.StatusOK {
		t.Errorf("the metrics token on /stats = %d, want 200", rec.Code)
	}
	if rec := request(s, http.MethodGet, "/health", "0123456789abcdef"); rec.Code != http.StatusOK {
		t.Errorf("the ingest token on /health = %d, want 200", rec.Code)
	}
}
