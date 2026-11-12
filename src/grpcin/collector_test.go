// HYDRA-UMC-TELEMETRY-COLLECTOR - grpcin/collector_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// The gRPC ingest wired to the real collector: what arrives over gRPC is buffered, delivered and counted exactly like a sample of any
// other input, and the rules of the collector (dedup by sequence) apply.
package grpcin

import (
	"sync"
	"testing"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/collector"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

type recordingSink struct {
	mu      sync.Mutex
	samples []telemetry.Sample
}

func (r *recordingSink) Write(batch []telemetry.Sample) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.samples = append(r.samples, batch...)
	return nil
}

func TestSamplesOverGRPCReachTheSinkThroughTheRealCollector(t *testing.T) {
	out := &recordingSink{}
	c := collector.New(100, out)
	_, addr, _ := startWith(t, Options{Submit: c.IngestSample})
	client := dial(t, addr, nil)
	stream, err := client.PushStream(ctx(t))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		_ = stream.Send(good(i))
	}
	ack, err := stream.CloseAndRecv()
	if err != nil || ack.Accepted != 3 {
		t.Fatalf("ack=%v err=%v", ack, err)
	}
	if c.FlushOnce(10) != 3 || len(out.samples) != 3 {
		t.Fatalf("delivered %d", len(out.samples))
	}
	if c.Stats().Ingested != 3 {
		t.Fatalf("%+v", c.Stats())
	}
}

func TestAResentSequenceIsAcknowledgedSoTheClientStopsResendingAndTheCollectorCountsIt(t *testing.T) {
	c := collector.New(100, &recordingSink{})
	_, addr, _ := startWith(t, Options{Submit: c.IngestSample})
	client := dial(t, addr, nil)
	first := good(0)
	if _, err := client.Push(ctx(t), first); err != nil {
		t.Fatal(err)
	}
	ack, err := client.Push(ctx(t), first)
	if err != nil || ack.Accepted != 1 {
		t.Fatalf("a resent sample must be acknowledged: ack=%v err=%v", ack, err)
	}
	if c.Stats().Duplicates != 1 || c.Stats().Ingested != 1 {
		t.Fatalf("%+v", c.Stats())
	}
}
