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
	"strings"
	"syscall"
	"time"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/api"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/buffer"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/collector"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/mqttin"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/sink"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/spool"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

const projectName = "HYDRA-UMC-TELEMETRY-COLLECTOR"

const role = "Telemetry-Collector - ingestion node for CAN frames and JSON " +
	"telemetry received over HTTP, feeds HYDRA-UMC-DATALAKE."

func main() {
	addr := flag.String("addr", "127.0.0.1:8092", "address to listen on for the HTTP API")
	bufferCap := flag.Int("buffer", 10000, "max buffered samples awaiting delivery")
	flushInterval := flag.Duration("flush-interval", 2*time.Second, "how often to flush buffered samples to the sink")
	batchSize := flag.Int("batch-size", 500, "max samples per flush")
	datalakeURL := flag.String("datalake-url", "", "HYDRA-UMC-DATALAKE base URL (e.g. http://localhost:8095) to write samples to; empty prints to stdout instead")
	token := flag.String("token", os.Getenv(tokenEnv), "bearer token the ingest routes require (default: $"+tokenEnv+"); empty leaves them open")
	futureSkew := flag.Duration("max-future-skew", telemetry.MaxFutureSkew, "how far ahead of this machine's clock a sample timestamp may be (0 turns the check off)")
	datalakeToken := flag.String("datalake-token", os.Getenv(datalakeTokenEnv), "bearer token sent to HYDRA-UMC-DATALAKE with every sample (default: $"+datalakeTokenEnv+")")
	metricsToken := flag.String("metrics-token", os.Getenv(metricsTokenEnv), "bearer token GET /stats and GET /metrics require (default: $"+metricsTokenEnv+"); empty leaves them open")
	batch := flag.Bool("datalake-batch", true, "send each flush to DATALAKE's POST /ingest/batch (falls back to one request per sample when the datalake has no such route)")
	permanentStatuses := flag.String("datalake-permanent-statuses", "", "comma-separated extra HTTP statuses (for example 401,403,404) the datalake may answer that discard the sample instead of retrying it for ever; empty keeps retrying")
	spoolPath := flag.String("spool", "", "file that holds the samples of a datalake outage on disk instead of in memory (empty: no spool)")
	spoolMax := flag.Int64("spool-max-bytes", 256<<20, "most unsent bytes the -spool file may hold")
	priority := flag.String("priority", "", "kind=level pairs (for example fault=9,motor_temp=5) that let a full buffer drop the least important queued sample for a more important one; empty keeps plain backpressure")
	priorityDefault := flag.Int("priority-default", 0, "level of the kinds that -priority does not list")
	allowAnonymous := flag.Bool("allow-anonymous", false, "allow a non-loopback -addr with no token (the ingest routes are then open to the whole network)")
	mqttURL := flag.String("mqtt-url", "", "broker to read the state of the bridges from (tcp://127.0.0.1:1883 or tls://host:8883); empty leaves the MQTT input off")
	mqttTopics := flag.String("mqtt-topics", mqttin.StateTopic+","+mqttin.AvailabilityTopic, "comma-separated topic filters the MQTT input subscribes to")
	mqttUser := flag.String("mqtt-username", os.Getenv(mqttUserEnv), "MQTT user name (default: $"+mqttUserEnv+")")
	mqttPasswordFile := flag.String("mqtt-password-file", "", "file holding the MQTT password (default: $"+mqttPasswordEnv+")")
	mqttCA := flag.String("mqtt-ca-file", "", "PEM file of the authority that signed the broker certificate (tls:// only)")
	mqttPlain := flag.Bool("mqtt-allow-plaintext", false, "allow credentials over a plain connection to a broker that is not on this machine")
	mqttMaxAge := flag.Duration("mqtt-max-age", mqttin.DefaultMaxAge, "oldest retained state that still counts as a reading")
	flag.Parse()
	telemetry.MaxFutureSkew = *futureSkew

	if problem := flagProblem(*bufferCap, *batchSize, *flushInterval); problem != "" {
		fmt.Fprintln(os.Stderr, "[telemetry-collector] "+problem)
		os.Exit(2)
	}
	statuses, problem := parseStatuses(*permanentStatuses)
	if problem == "" {
		problem = metricsTokenProblem(*metricsToken)
	}
	var priorityTable map[string]int
	if problem == "" {
		priorityTable, problem = parsePriority(*priority)
	}
	if problem != "" {
		fmt.Fprintln(os.Stderr, "[telemetry-collector] "+problem)
		os.Exit(2)
	}
	if problem := mqttProblem(*mqttURL, *mqttUser, *mqttPlain); problem != "" {
		fmt.Fprintln(os.Stderr, "[telemetry-collector] "+problem)
		os.Exit(2)
	}
	if *spoolMax < 1 {
		fmt.Fprintln(os.Stderr, "[telemetry-collector] -spool-max-bytes must be at least 1")
		os.Exit(2)
	}
	if problem := startupProblem(*addr, *token, *allowAnonymous); problem != "" {
		fmt.Fprintln(os.Stderr, "[telemetry-collector] "+problem)
		os.Exit(2)
	}

	fmt.Printf("%s v%s\n", projectName, Version)
	fmt.Println(role)

	var s sink.Sink
	if *datalakeURL != "" {
		lake := sink.NewDatalakeSink(*datalakeURL)
		lake.Token = *datalakeToken
		lake.Batch = *batch
		lake.PermanentStatuses = statuses
		s = lake
		fmt.Printf("[telemetry-collector] delivering to HYDRA-UMC-DATALAKE at %s\n", *datalakeURL)
	} else {
		s = sink.ConsoleSink{W: os.Stdout}
		fmt.Println("[telemetry-collector] no -datalake-url given, printing flushed samples to stdout")
	}

	c := collector.New(*bufferCap, s)
	if *spoolPath != "" {
		sp, err := spool.Open(*spoolPath, *spoolMax)
		if err != nil {
			fmt.Fprintln(os.Stderr, "[telemetry-collector] "+err.Error())
			os.Exit(2)
		}
		c.SetSpool(sp)
		fmt.Printf("[telemetry-collector] outages are kept on disk in %s (%d samples already waiting)\n", *spoolPath, sp.Pending())
	}
	if priorityTable != nil {
		c.SetPriority(buffer.PriorityByKind(priorityTable, *priorityDefault))
	}
	var mqttStats func() map[string]any
	server := api.NewWithOptions(c, api.Options{Token: *token, MetricsToken: *metricsToken, ExtraStats: func() map[string]any {
		if mqttStats == nil {
			return nil
		}
		return mqttStats()
	}})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go c.Run(ctx, *flushInterval, *batchSize)

	if *mqttURL != "" {
		password := os.Getenv(mqttPasswordEnv)
		if *mqttPasswordFile != "" {
			raw, err := os.ReadFile(*mqttPasswordFile)
			if err != nil {
				fmt.Fprintln(os.Stderr, "[telemetry-collector] cannot read the MQTT password file: "+err.Error())
				os.Exit(2)
			}
			password = strings.TrimSpace(string(raw))
		}
		adapter := &mqttin.Adapter{Submit: c.IngestSample, MaxAge: *mqttMaxAge}
		var topics []string
		for _, t := range strings.Split(*mqttTopics, ",") {
			if t = strings.TrimSpace(t); t != "" {
				topics = append(topics, t)
			}
		}
		client, err := mqttin.New(mqttin.Options{URL: *mqttURL, Username: *mqttUser, Password: password, CAFile: *mqttCA, Topics: topics}, adapter.Handle)
		if err != nil {
			fmt.Fprintln(os.Stderr, "[telemetry-collector] "+err.Error())
			os.Exit(2)
		}
		go client.Run(ctx)
		mqttStats = func() map[string]any {
			return map[string]any{"mqtt": map[string]any{"connection": client.Status(), "samples": adapter.Counters()}}
		}
		fmt.Printf("[telemetry-collector] reading the state of the bridges from %s (%s)\n", *mqttURL, strings.Join(topics, ", "))
	}

	fmt.Printf("[telemetry-collector] HTTP API listening on %s (buffer=%d, flush every %s, batch=%d)\n",
		*addr, *bufferCap, *flushInterval, *batchSize)
	fmt.Println("[telemetry-collector] POST /ingest/can, POST /ingest/ws, GET /stats")

	httpServer := &http.Server{
		Addr:              *addr,
		Handler:           server,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    64 << 10,
	}
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
