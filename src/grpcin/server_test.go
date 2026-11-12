// HYDRA-UMC-TELEMETRY-COLLECTOR - grpcin/server_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// The gRPC ingest with a real server and a real client in the same process, over loopback: samples, validation, the token, TLS and
// mutual TLS (certificates made here, nothing on disk but the temp directory of the test), limits and the guard on the options.
package grpcin

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/grpcin/pb"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

const validCredential = "a-token-of-16-or-more"

// sink records what the server submitted.
type sink struct {
	mu      sync.Mutex
	samples []telemetry.Sample
	err     error
}

func (k *sink) submit(s telemetry.Sample) error {
	if err := s.Validate(); err != nil {
		return err
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.err != nil {
		return k.err
	}
	k.samples = append(k.samples, s)
	return nil
}

func (k *sink) count() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.samples)
}

func start(t *testing.T, opts Options) (*Server, string, *sink) {
	t.Helper()
	k := &sink{}
	opts.Submit = k.submit
	srv, addr, _ := startWith(t, opts)
	return srv, addr, k
}

func startWith(t *testing.T, opts Options) (*Server, string, struct{}) {
	t.Helper()
	opts.Addr = "127.0.0.1:0"
	srv, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(srv.Stop)
	return srv, l.Addr().String(), struct{}{}
}

func dial(t *testing.T, addr string, creds credentials.TransportCredentials) pb.TelemetryIngestClient {
	t.Helper()
	if creds == nil {
		creds = insecure.NewCredentials()
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewTelemetryIngestClient(conn)
}

func good(i int) *pb.Sample {
	return &pb.Sample{SourceId: "robot-1", Kind: "motor_current", TimestampMs: time.Now().UnixMilli() + int64(i), Fields: map[string]float64{"value": 1.5}, Sequence: uint64(i + 1)}
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return c
}

func withToken(c context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(c, "authorization", "Bearer "+token)
}

func code(err error) codes.Code { return status.Code(err) }

func TestPushOneSampleReachesTheCollector(t *testing.T) {
	srv, addr, k := start(t, Options{})
	ack, err := dial(t, addr, nil).Push(ctx(t), good(0))
	if err != nil || ack.Accepted != 1 || ack.Rejected != 0 {
		t.Fatalf("ack=%v err=%v", ack, err)
	}
	if k.count() != 1 || k.samples[0].SourceID != "robot-1" || k.samples[0].Fields["value"] != 1.5 || k.samples[0].Sequence != 1 {
		t.Fatalf("submitted: %+v", k.samples)
	}
	if st := srv.Stats(); st.Accepted != 1 || st.Rejected != 0 {
		t.Fatalf("stats: %+v", st)
	}
}

func TestABadSampleIsInvalidArgumentWithTheSameRulesAsHTTP(t *testing.T) {
	srv, addr, k := start(t, Options{})
	client := dial(t, addr, nil)
	bad := []*pb.Sample{
		{Kind: "k", TimestampMs: time.Now().UnixMilli()},     // no source
		{SourceId: "s", TimestampMs: time.Now().UnixMilli()}, // no kind
		{SourceId: "s", Kind: "k"},                           // no time
		{SourceId: "s", Kind: "k", TimestampMs: 1700000000},  // seconds, not milliseconds
		{SourceId: "s", Kind: "k", TimestampMs: time.Now().UnixMilli(), Fields: map[string]float64{"v": math.NaN()}},  // NaN
		{SourceId: "s", Kind: "k", TimestampMs: time.Now().UnixMilli(), Fields: map[string]float64{"v": math.Inf(1)}}, // Inf
		{SourceId: "s\x00", Kind: "k", TimestampMs: time.Now().UnixMilli()},                                           // not printable
		{SourceId: "s", Kind: "k", TimestampMs: time.Now().Add(time.Hour).UnixMilli()},                                // too far ahead
	}
	for i, m := range bad {
		if _, err := client.Push(ctx(t), m); code(err) != codes.InvalidArgument {
			t.Errorf("case %d: %v", i, err)
		}
	}
	tooMany := map[string]float64{}
	for i := 0; i <= MaxFields; i++ {
		tooMany[string(rune('a'+i%26))+string(rune('a'+i/26))] = 1
	}
	if _, err := client.Push(ctx(t), &pb.Sample{SourceId: "s", Kind: "k", TimestampMs: time.Now().UnixMilli(), Fields: tooMany}); code(err) != codes.InvalidArgument {
		t.Errorf("too many fields: %v", err)
	}
	if k.count() != 0 || srv.Stats().Rejected != uint64(len(bad)+1) {
		t.Fatalf("count=%d stats=%+v", k.count(), srv.Stats())
	}
}

func TestAFullBufferIsUnavailableSoTheClientRetries(t *testing.T) {
	_, addr, k := start(t, Options{})
	k.err = errors.New("buffer full")
	if _, err := dial(t, addr, nil).Push(ctx(t), good(0)); code(err) != codes.Unavailable {
		t.Fatalf("%v", err)
	}
}

func TestAStreamCountsTheGoodAndTheBadAndGoesOn(t *testing.T) {
	_, addr, k := start(t, Options{})
	stream, err := dial(t, addr, nil).PushStream(ctx(t))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := stream.Send(good(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := stream.Send(&pb.Sample{Kind: "k", TimestampMs: time.Now().UnixMilli()}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(good(9)); err != nil {
		t.Fatal(err)
	}
	ack, err := stream.CloseAndRecv()
	if err != nil || ack.Accepted != 6 || ack.Rejected != 1 || ack.FirstError == "" {
		t.Fatalf("ack=%v err=%v", ack, err)
	}
	if k.count() != 6 {
		t.Fatalf("count=%d", k.count())
	}
}

func TestAnEmptyStreamIsAnEmptyAnswer(t *testing.T) {
	_, addr, _ := start(t, Options{})
	stream, _ := dial(t, addr, nil).PushStream(ctx(t))
	ack, err := stream.CloseAndRecv()
	if err != nil || ack.Accepted != 0 || ack.Rejected != 0 {
		t.Fatalf("ack=%v err=%v", ack, err)
	}
}

func TestTheTokenIsRequiredWhenSet(t *testing.T) {
	srv, addr, k := start(t, Options{Token: validCredential})
	client := dial(t, addr, nil)
	if _, err := client.Push(ctx(t), good(0)); code(err) != codes.Unauthenticated {
		t.Fatalf("no token: %v", err)
	}
	if _, err := client.Push(withToken(ctx(t), "a-wrong-token-of-16-plus"), good(0)); code(err) != codes.Unauthenticated {
		t.Fatalf("wrong token: %v", err)
	}
	if _, err := client.Push(withToken(ctx(t), validCredential[:len(validCredential)-1]), good(0)); code(err) != codes.Unauthenticated {
		t.Fatalf("prefix of the token: %v", err)
	}
	raw := metadata.AppendToOutgoingContext(ctx(t), "authorization", validCredential) // no "Bearer "
	if _, err := client.Push(raw, good(0)); code(err) != codes.Unauthenticated {
		t.Fatalf("no scheme: %v", err)
	}
	stream, _ := client.PushStream(ctx(t))
	_ = stream.Send(good(0))
	if _, err := stream.CloseAndRecv(); code(err) != codes.Unauthenticated {
		t.Fatalf("stream without token: %v", err)
	}
	if k.count() != 0 {
		t.Fatalf("a call without the token reached the collector")
	}
	if _, err := client.Push(withToken(ctx(t), validCredential), good(0)); err != nil {
		t.Fatalf("right token: %v", err)
	}
	if srv.Stats().Unauthorized != 5 || k.count() != 1 {
		t.Fatalf("stats=%+v count=%d", srv.Stats(), k.count())
	}
}

func TestAStreamWithTheTokenWorks(t *testing.T) {
	_, addr, _ := start(t, Options{Token: validCredential})
	stream, _ := dial(t, addr, nil).PushStream(withToken(ctx(t), validCredential))
	_ = stream.Send(good(0))
	if ack, err := stream.CloseAndRecv(); err != nil || ack.Accepted != 1 {
		t.Fatalf("ack=%v err=%v", ack, err)
	}
}

func TestAMessageOverTheLimitIsRefused(t *testing.T) {
	_, addr, k := start(t, Options{})
	big := good(0)
	big.SourceId = string(make([]byte, MaxMessageBytes+1))
	if _, err := dial(t, addr, nil).Push(ctx(t), big); code(err) != codes.ResourceExhausted {
		t.Fatalf("%v", err)
	}
	if k.count() != 0 {
		t.Fatal("reached the collector")
	}
}

// ---- TLS ----------------------------------------------------------------------------------------------------------------------

type pki struct {
	caPEM, certFile, keyFile string
	pool                     *x509.CertPool
}

func makeCert(t *testing.T, dir, name string, ca *x509.Certificate, caKey *ecdsa.PrivateKey, isCA bool) (*x509.Certificate, *ecdsa.PrivateKey, []byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	tmpl := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, DNSNames: []string{"localhost"},
		BasicConstraintsValid: true, IsCA: isCA,
	}
	if isCA {
		tmpl.KeyUsage |= x509.KeyUsageCertSign
	}
	parent, signer := tmpl, key
	if ca != nil {
		parent, signer = ca, caKey
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, &key.PublicKey, signer)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	keyDER, _ := x509.MarshalECPrivateKey(key)
	return cert, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

func writePKI(t *testing.T) pki {
	t.Helper()
	dir := t.TempDir()
	ca, caKey, caPEM, _ := makeCert(t, dir, "test-ca", nil, nil, true)
	_, _, certPEM, keyPEM := makeCert(t, dir, "server", ca, caKey, false)
	p := pki{caPEM: filepath.Join(dir, "ca.pem"), certFile: filepath.Join(dir, "cert.pem"), keyFile: filepath.Join(dir, "key.pem"), pool: x509.NewCertPool()}
	p.pool.AppendCertsFromPEM(caPEM)
	for path, data := range map[string][]byte{p.caPEM: caPEM, p.certFile: certPEM, p.keyFile: keyPEM} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return p
}

func TestTLSListenerServesAClientThatTrustsItsAuthority(t *testing.T) {
	p := writePKI(t)
	_, addr, k := start(t, Options{CertFile: p.certFile, KeyFile: p.keyFile, Token: validCredential})
	creds := credentials.NewTLS(&tls.Config{RootCAs: p.pool, ServerName: "localhost", MinVersion: tls.VersionTLS12})
	if _, err := dial(t, addr, creds).Push(withToken(ctx(t), validCredential), good(0)); err != nil {
		t.Fatalf("%v", err)
	}
	if _, err := dial(t, addr, nil).Push(ctx(t), good(0)); err == nil {
		t.Fatal("a plain client reached a TLS listener")
	}
	other := credentials.NewTLS(&tls.Config{RootCAs: x509.NewCertPool(), ServerName: "localhost", MinVersion: tls.VersionTLS12})
	if _, err := dial(t, addr, other).Push(withToken(ctx(t), validCredential), good(0)); err == nil {
		t.Fatal("a client that does not trust the authority connected")
	}
	if k.count() != 1 {
		t.Fatalf("count=%d", k.count())
	}
}

func TestMutualTLSDemandsAClientCertificateOfTheAuthority(t *testing.T) {
	p := writePKI(t)
	_, addr, k := start(t, Options{CertFile: p.certFile, KeyFile: p.keyFile, ClientCAFile: p.caPEM})
	clientCert, err := tls.LoadX509KeyPair(p.certFile, p.keyFile) // signed by the same authority, valid as a client
	if err != nil {
		t.Fatal(err)
	}
	with := credentials.NewTLS(&tls.Config{RootCAs: p.pool, ServerName: "localhost", Certificates: []tls.Certificate{clientCert}, MinVersion: tls.VersionTLS12})
	if _, err := dial(t, addr, with).Push(ctx(t), good(0)); err != nil {
		t.Fatalf("with certificate: %v", err)
	}
	without := credentials.NewTLS(&tls.Config{RootCAs: p.pool, ServerName: "localhost", MinVersion: tls.VersionTLS12})
	if _, err := dial(t, addr, without).Push(ctx(t), good(0)); err == nil {
		t.Fatal("a client without a certificate was served")
	}
	// a certificate of another authority
	pq := writePKI(t)
	foreign, _ := tls.LoadX509KeyPair(pq.certFile, pq.keyFile)
	bad := credentials.NewTLS(&tls.Config{RootCAs: p.pool, ServerName: "localhost", Certificates: []tls.Certificate{foreign}, MinVersion: tls.VersionTLS12})
	if _, err := dial(t, addr, bad).Push(ctx(t), good(0)); err == nil {
		t.Fatal("a certificate of another authority was served")
	}
	if k.count() != 1 {
		t.Fatalf("count=%d", k.count())
	}
}

func TestNewRefusesUnusableFilesAndOptions(t *testing.T) {
	p := writePKI(t)
	submit := func(telemetry.Sample) error { return nil }
	dir := t.TempDir()
	empty := filepath.Join(dir, "empty.pem")
	_ = os.WriteFile(empty, nil, 0o600)
	cases := map[string]Options{
		"missing cert":     {Addr: "127.0.0.1:1", Submit: submit, CertFile: filepath.Join(dir, "nope.pem"), KeyFile: p.keyFile},
		"cert and no key":  {Addr: "127.0.0.1:1", Submit: submit, CertFile: p.certFile},
		"key and no cert":  {Addr: "127.0.0.1:1", Submit: submit, KeyFile: p.keyFile},
		"client ca no tls": {Addr: "127.0.0.1:1", Submit: submit, ClientCAFile: p.caPEM},
		"empty client ca":  {Addr: "127.0.0.1:1", Submit: submit, CertFile: p.certFile, KeyFile: p.keyFile, ClientCAFile: empty},
		"missing ca":       {Addr: "127.0.0.1:1", Submit: submit, CertFile: p.certFile, KeyFile: p.keyFile, ClientCAFile: filepath.Join(dir, "nope.pem")},
		"short token":      {Addr: "127.0.0.1:1", Submit: submit, Token: "short"},
		"no submit":        {Addr: "127.0.0.1:1"},
	}
	for name, opts := range cases {
		if _, err := New(opts); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestTheGuardOnTheListenAddress(t *testing.T) {
	submit := func(telemetry.Sample) error { return nil }
	p := writePKI(t)
	open := Options{Addr: "0.0.0.0:8093", Submit: submit}
	if open.Problem() == "" {
		t.Error("an open listener beyond loopback was accepted")
	}
	open.AllowAnonymous = true
	if open.Problem() != "" {
		t.Errorf("explicitly open was refused: %s", open.Problem())
	}
	if (Options{Addr: ":8093", Submit: submit}).Problem() == "" {
		t.Error("an empty host is every interface")
	}
	if (Options{Addr: "0.0.0.0:8093", Submit: submit, Token: validCredential}).Problem() == "" {
		t.Error("a token in clear text beyond loopback was accepted")
	}
	if problem := (Options{Addr: "0.0.0.0:8093", Submit: submit, Token: validCredential, CertFile: p.certFile, KeyFile: p.keyFile}).Problem(); problem != "" {
		t.Errorf("token over TLS: %s", problem)
	}
	if problem := (Options{Addr: "0.0.0.0:8093", Submit: submit, CertFile: p.certFile, KeyFile: p.keyFile, ClientCAFile: p.caPEM}).Problem(); problem != "" {
		t.Errorf("mutual TLS: %s", problem)
	}
	for _, loop := range []string{"127.0.0.1:8093", "localhost:8093", "[::1]:8093"} {
		if problem := (Options{Addr: loop, Submit: submit}).Problem(); problem != "" {
			t.Errorf("%s: %s", loop, problem)
		}
	}
	if (Options{}).Problem() != "" {
		t.Error("off must have no problem")
	}
}

func TestReadTokenTakesTheFirstLineAndRefusesAnEmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	_ = os.WriteFile(path, []byte("  "+validCredential+"  \nsecond line\n"), 0o600)
	if got, err := ReadToken(path); err != nil || got != validCredential {
		t.Fatalf("%q %v", got, err)
	}
	_ = os.WriteFile(path, []byte("\n"), 0o600)
	if _, err := ReadToken(path); err == nil {
		t.Fatal("empty token accepted")
	}
	if _, err := ReadToken(filepath.Join(dir, "nope")); err == nil {
		t.Fatal("missing file accepted")
	}
}

func TestTheCopyOfTheProtoNextToTheSDKIsTheSame(t *testing.T) {
	theirs, err := os.ReadFile(filepath.Join("..", "..", "..", "HYDRA-UMC-SDK", "contracts", "proto", "hydra_telemetry.proto"))
	if err != nil {
		t.Skip("the SDK is not next to this repository")
	}
	ours, err := os.ReadFile(filepath.Join("pb", "hydra_telemetry.proto"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.ReplaceAll(string(theirs), "\r\n", "\n") != strings.ReplaceAll(string(ours), "\r\n", "\n") {
		t.Fatal("pb/hydra_telemetry.proto differs from contracts/proto/hydra_telemetry.proto of the SDK: copy it again and regenerate")
	}
}
