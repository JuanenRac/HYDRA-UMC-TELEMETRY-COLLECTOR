// HYDRA-UMC-TELEMETRY-COLLECTOR - grpcin/bind_source_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// With BindSourceToCN a client certificate writes only as itself.
package grpcin

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/grpcin/pb"
)

// robotPKI makes an authority, the certificate of the server and the certificate of a client whose common name is `client`.
func robotPKI(t *testing.T, client string) (Options, credentials.TransportCredentials) {
	t.Helper()
	dir := t.TempDir()
	ca, caKey, caPEM, _ := makeCert(t, dir, "test-ca", nil, nil, true)
	_, _, serverPEM, serverKey := makeCert(t, dir, "server", ca, caKey, false)
	_, _, clientPEM, clientKey := makeCert(t, dir, client, ca, caKey, false)
	files := map[string][]byte{"ca.pem": caPEM, "cert.pem": serverPEM, "key.pem": serverKey}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	pair, err := tls.X509KeyPair(clientPEM, clientKey)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEM)
	options := Options{CertFile: filepath.Join(dir, "cert.pem"), KeyFile: filepath.Join(dir, "key.pem"), ClientCAFile: filepath.Join(dir, "ca.pem")}
	return options, credentials.NewTLS(&tls.Config{RootCAs: pool, ServerName: "localhost", Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12})
}

func from(source string, sequence int) *pb.Sample {
	sample := good(sequence)
	sample.SourceId = source
	return sample
}

func TestBoundSourceAcceptsOnlyTheNameOfTheCertificate(t *testing.T) {
	options, creds := robotPKI(t, "robot-1")
	options.BindSourceToCN = true
	srv, addr, k := start(t, options)
	client := dial(t, addr, creds)
	for i, source := range []string{"robot-1", "robot-1:arm"} {
		if _, err := client.Push(ctx(t), from(source, i)); err != nil {
			t.Fatalf("%s: %v", source, err)
		}
	}
	for i, source := range []string{"robot-2", "robot-10", "robot-1x:arm", "arm:robot-1"} {
		_, err := client.Push(ctx(t), from(source, 10+i))
		if status.Code(err) != codes.PermissionDenied {
			t.Errorf("%s: %v, want PERMISSION_DENIED", source, err)
		}
	}
	stream, err := client.PushStream(ctx(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, sample := range []*pb.Sample{from("robot-2", 20), from("robot-1", 21)} {
		if err := stream.Send(sample); err != nil {
			t.Fatal(err)
		}
	}
	ack, err := stream.CloseAndRecv()
	if err != nil || ack.Accepted != 1 || ack.Rejected != 1 {
		t.Fatalf("stream ack %+v %v", ack, err)
	}
	if k.count() != 3 {
		t.Errorf("count=%d, want the 3 samples of robot-1", k.count())
	}
	if got := srv.Stats().RejectedSource; got != 5 {
		t.Errorf("rejected_source=%d, want 5", got)
	}
}

func TestWithoutTheSwitchTheSourceIsTakenAsBefore(t *testing.T) {
	options, creds := robotPKI(t, "robot-1")
	_, addr, k := start(t, options)
	if _, err := dial(t, addr, creds).Push(ctx(t), from("robot-2", 1)); err != nil {
		t.Fatal(err)
	}
	if k.count() != 1 {
		t.Fatalf("count=%d", k.count())
	}
}

func TestBindingTheSourceNeedsMutualTLS(t *testing.T) {
	options := Options{Addr: "127.0.0.1:1", BindSourceToCN: true, Submit: (&sink{}).submit}
	if options.Problem() == "" {
		t.Fatal("binding without client certificates was accepted")
	}
}
