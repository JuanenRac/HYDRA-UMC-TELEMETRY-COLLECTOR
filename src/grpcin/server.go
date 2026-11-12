// HYDRA-UMC-TELEMETRY-COLLECTOR - grpcin/server.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// The gRPC ingest: the same samples as POST /ingest, over the service TelemetryIngest of the SDK (contracts/proto/hydra_telemetry.proto).
// It is off unless -grpc-addr is given. A sample goes through the same Submit as every other input (the collector validates it there),
// so nothing is accepted here that the HTTP route would refuse.
//
// Who may call: a bearer token (the "authorization" metadata, read from a file by the caller), and/or a client certificate when the
// listener has TLS with a client authority (mutual TLS). A listener that is not on loopback needs at least one of them, or the operator
// has to say that it is open. A message is limited in size, a stream in samples, and nothing here keeps state between calls.
package grpcin

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/collector"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/grpcin/pb"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

const (
	// MaxMessageBytes is the largest message the server reads (a sample is a few hundred bytes).
	MaxMessageBytes = 64 << 10
	// MaxStreamSamples is the most samples one PushStream call may carry; a longer stream is closed with RESOURCE_EXHAUSTED.
	MaxStreamSamples = 100_000
	// MinTokenLength is the shortest bearer token, like the HTTP routes.
	MinTokenLength = 16
	// MaxFields bounds the fields of one sample, like the MQTT input.
	MaxFields = 64
)

// Options is the configuration of the listener.
type Options struct {
	// Addr is the address to listen on ("127.0.0.1:8093"). Empty means the gRPC input is off and Listen is not called.
	Addr string
	// Token is the bearer token a caller must send (empty: none is required).
	Token string
	// CertFile and KeyFile turn TLS on (both or none).
	CertFile, KeyFile string
	// ClientCAFile makes the listener demand and verify a client certificate (mutual TLS); it needs CertFile and KeyFile.
	ClientCAFile string
	// AllowAnonymous allows a listener that is not on loopback with neither a token nor a client certificate.
	AllowAnonymous bool
	// Submit takes a sample into the collector (collector.IngestSample).
	Submit func(telemetry.Sample) error
}

// Problem returns why the options cannot be used, or "" when they can.
func (o Options) Problem() string {
	if o.Addr == "" {
		return ""
	}
	if o.Token != "" && len(o.Token) < MinTokenLength {
		return "the gRPC bearer token must be at least 16 characters"
	}
	if (o.CertFile == "") != (o.KeyFile == "") {
		return "-grpc-tls-cert and -grpc-tls-key go together"
	}
	if o.ClientCAFile != "" && o.CertFile == "" {
		return "-grpc-client-ca needs -grpc-tls-cert and -grpc-tls-key (mutual TLS is a TLS listener that asks for a client certificate)"
	}
	if o.Submit == nil {
		return "the gRPC input has nowhere to put the samples"
	}
	if !IsLoopbackAddr(o.Addr) && o.Token == "" && o.ClientCAFile == "" && !o.AllowAnonymous {
		return "refusing to listen for gRPC on " + o.Addr + " without a token or a client certificate: set -grpc-token-file or -grpc-client-ca, or pass -grpc-allow-anonymous to open it"
	}
	if o.Token != "" && o.CertFile == "" && !IsLoopbackAddr(o.Addr) && !o.AllowAnonymous {
		return "refusing to send a bearer token in clear text to " + o.Addr + ": set -grpc-tls-cert and -grpc-tls-key, or listen on loopback"
	}
	return ""
}

// IsLoopbackAddr reports whether a listen address binds loopback only (an empty host listens on every interface).
func IsLoopbackAddr(addr string) bool {
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

// ReadToken reads a token from a file: the first line, trimmed. Nothing of it is ever logged.
func ReadToken(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	line, _, _ := strings.Cut(string(raw), "\n")
	token := strings.TrimSpace(line)
	if token == "" {
		return "", errors.New("the token file is empty")
	}
	return token, nil
}

// Counters are the totals since the start, for GET /stats.
type Counters struct {
	Accepted     uint64 `json:"accepted"`
	Rejected     uint64 `json:"rejected"`
	Unauthorized uint64 `json:"unauthorized"`
}

// Server is the TelemetryIngest service.
type Server struct {
	pb.UnimplementedTelemetryIngestServer
	opts         Options
	accepted     atomic.Uint64
	rejected     atomic.Uint64
	unauthorized atomic.Uint64
	grpc         *grpc.Server
}

// New builds the server (it does not listen yet).
func New(opts Options) (*Server, error) {
	if problem := opts.Problem(); problem != "" {
		return nil, errors.New(problem)
	}
	s := &Server{opts: opts}
	serverOptions := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(MaxMessageBytes),
		grpc.UnaryInterceptor(s.unaryAuth),
		grpc.StreamInterceptor(s.streamAuth),
	}
	if opts.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(opts.CertFile, opts.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("-grpc-tls-cert/-grpc-tls-key: %w", err)
		}
		config := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
		if opts.ClientCAFile != "" {
			pem, err := os.ReadFile(opts.ClientCAFile)
			if err != nil {
				return nil, fmt.Errorf("-grpc-client-ca: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, errors.New("-grpc-client-ca holds no certificate")
			}
			config.ClientCAs = pool
			config.ClientAuth = tls.RequireAndVerifyClientCert
		}
		serverOptions = append(serverOptions, grpc.Creds(credentials.NewTLS(config)))
	}
	s.grpc = grpc.NewServer(serverOptions...)
	pb.RegisterTelemetryIngestServer(s.grpc, s)
	return s, nil
}

// Serve answers calls on the listener until Stop.
func (s *Server) Serve(l net.Listener) error { return s.grpc.Serve(l) }

// Stop closes the listener and ends the calls in flight.
func (s *Server) Stop() { s.grpc.Stop() }

// GracefulStop lets the calls in flight finish.
func (s *Server) GracefulStop() { s.grpc.GracefulStop() }

// Stats returns the totals.
func (s *Server) Stats() Counters {
	return Counters{Accepted: s.accepted.Load(), Rejected: s.rejected.Load(), Unauthorized: s.unauthorized.Load()}
}

func (s *Server) authorized(ctx context.Context) error {
	if s.opts.Token == "" {
		return nil
	}
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	for _, v := range values {
		if got, ok := strings.CutPrefix(v, "Bearer "); ok && subtle.ConstantTimeCompare([]byte(got), []byte(s.opts.Token)) == 1 {
			return nil
		}
	}
	s.unauthorized.Add(1)
	return status.Error(codes.Unauthenticated, "a valid bearer token is required")
}

func (s *Server) unaryAuth(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if err := s.authorized(ctx); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

func (s *Server) streamAuth(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := s.authorized(ss.Context()); err != nil {
		return err
	}
	return handler(srv, ss)
}

// toSample converts the message; it refuses what cannot be a sample before the collector sees it.
func toSample(m *pb.Sample) (telemetry.Sample, error) {
	if m == nil {
		return telemetry.Sample{}, errors.New("empty sample")
	}
	if len(m.Fields) > MaxFields {
		return telemetry.Sample{}, fmt.Errorf("a sample carries at most %d fields", MaxFields)
	}
	fields := make(map[string]float64, len(m.Fields))
	for k, v := range m.Fields {
		fields[k] = v
	}
	return telemetry.Sample{SourceID: m.SourceId, Kind: m.Kind, Timestamp: m.TimestampMs, Fields: fields, Sequence: m.Sequence}, nil
}

func (s *Server) submit(m *pb.Sample) error {
	sample, err := toSample(m)
	if err == nil {
		err = s.opts.Submit(sample)
	}
	if errors.Is(err, collector.ErrDuplicate) {
		// the collector already has this (source, sequence): a client that reconnected and resent it is told it is done, so it stops
		s.accepted.Add(1)
		return nil
	}
	if err != nil {
		s.rejected.Add(1)
		return err
	}
	s.accepted.Add(1)
	return nil
}

// Push takes one sample; one the collector refuses is INVALID_ARGUMENT (or UNAVAILABLE when the buffer is full).
func (s *Server) Push(_ context.Context, m *pb.Sample) (*pb.PushAck, error) {
	if err := s.submit(m); err != nil {
		return nil, status.Error(codeFor(err), err.Error())
	}
	return &pb.PushAck{Accepted: 1}, nil
}

// PushStream takes samples until the client closes the stream and answers with the totals of the call.
func (s *Server) PushStream(stream pb.TelemetryIngest_PushStreamServer) error {
	ack := &pb.PushAck{}
	var seen int
	for {
		m, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(ack)
		}
		if err != nil {
			return err
		}
		seen++
		if seen > MaxStreamSamples {
			return status.Errorf(codes.ResourceExhausted, "a stream carries at most %d samples", MaxStreamSamples)
		}
		if err := s.submit(m); err != nil {
			ack.Rejected++
			if ack.FirstError == "" {
				ack.FirstError = err.Error()
			}
			continue
		}
		ack.Accepted++
	}
}

// codeFor maps a refusal to a status: a bad sample is the caller's fault, anything else (a full buffer) is a reason to try again.
func codeFor(err error) codes.Code {
	if errors.Is(err, telemetry.ErrMissingSourceID) || errors.Is(err, telemetry.ErrInvalidSourceID) || errors.Is(err, telemetry.ErrMissingKind) ||
		errors.Is(err, telemetry.ErrInvalidTimestamp) || errors.Is(err, telemetry.ErrImplausibleTimestamp) || errors.Is(err, telemetry.ErrInvalidField) ||
		strings.HasPrefix(err.Error(), "a sample carries") || err.Error() == "empty sample" {
		return codes.InvalidArgument
	}
	return codes.Unavailable
}
