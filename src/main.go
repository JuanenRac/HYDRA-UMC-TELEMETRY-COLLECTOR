// =============================================================================
// HYDRA-UMC-TELEMETRY-COLLECTOR - src/main.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
// =============================================================================
// HYDRA-UMC-TELEMETRY-COLLECTOR - high-throughput ingestion node for CAN
// and WebSocket logs.
//
// Real ingestion pipeline, no longer just an identity print: telemetry/
// parses CAN frames and WebSocket JSON into a normalized Sample,
// buffer/ holds them in a bounded, backpressure-reporting queue,
// collector/ orchestrates ingest+flush (retrying a batch instead of
// losing it if the sink fails), and sink/ delivers it - to a real
// HYDRA-UMC-DATALAKE instance via sink.DatalakeSink when -datalake-url
// is set (DATALAKE now has a real POST /ingest to write to), or to
// stdout via sink.ConsoleSink otherwise, for running this collector
// standalone without a DATALAKE instance up.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/api"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/collector"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/sink"
)

const projectName = "HYDRA-UMC-TELEMETRY-COLLECTOR"

const role = "Telemetry-Collector - high-throughput multi-protocol " +
	"ingestion node (CAN/WebSocket/gRPC), feeds HYDRA-UMC-DATALAKE."

func main() {
	addr := flag.String("addr", "127.0.0.1:8092", "address to listen on for the HTTP API")
	bufferCap := flag.Int("buffer", 10000, "max buffered samples awaiting delivery")
	flushInterval := flag.Duration("flush-interval", 2*time.Second, "how often to flush buffered samples to the sink")
	batchSize := flag.Int("batch-size", 500, "max samples per flush")
	datalakeURL := flag.String("datalake-url", "", "HYDRA-UMC-DATALAKE base URL (e.g. http://localhost:8095) to write samples to; empty prints to stdout instead")
	flag.Parse()

	fmt.Printf("%s v%s\n", projectName, Version)
	fmt.Println(role)

	var s sink.Sink
	if *datalakeURL != "" {
		s = sink.NewDatalakeSink(*datalakeURL)
		fmt.Printf("[telemetry-collector] delivering to HYDRA-UMC-DATALAKE at %s\n", *datalakeURL)
	} else {
		s = sink.ConsoleSink{W: os.Stdout}
		fmt.Println("[telemetry-collector] no -datalake-url given, printing flushed samples to stdout")
	}

	c := collector.New(*bufferCap, s)
	server := api.New(c)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go c.Run(ctx, *flushInterval, *batchSize)

	fmt.Printf("[telemetry-collector] HTTP API listening on %s (buffer=%d, flush every %s, batch=%d)\n",
		*addr, *bufferCap, *flushInterval, *batchSize)
	fmt.Println("[telemetry-collector] POST /ingest/can, POST /ingest/ws, GET /stats")

	httpServer := &http.Server{Addr: *addr, Handler: server}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
	fmt.Println("[telemetry-collector] shutting down")
}
