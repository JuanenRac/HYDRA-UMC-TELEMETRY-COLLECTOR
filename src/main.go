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
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/anomaly"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/api"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/buffer"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/canids"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/collector"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/downsample"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/grpcin"
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
	batchGzip := flag.Bool("datalake-gzip", false, "compress each batch request with gzip (off by default; the datalake must run with --accept-gzip, otherwise the first batch is repeated uncompressed and the collector keeps sending plain bodies)")
	permanentStatuses := flag.String("datalake-permanent-statuses", "", "comma-separated extra HTTP statuses (for example 401,403,404) the datalake may answer that discard the sample instead of retrying it for ever; empty keeps retrying")
	spoolPath := flag.String("spool", "", "file that holds the samples of a datalake outage on disk instead of in memory (empty: no spool)")
	spoolMax := flag.Int64("spool-max-bytes", 256<<20, "most unsent bytes the -spool file may hold")
	priority := flag.String("priority", "", "kind=level pairs (for example fault=9,motor_temp=5) that let a full buffer drop the least important queued sample for a more important one; empty keeps plain backpressure")
	priorityDefault := flag.Int("priority-default", 0, "level of the kinds that -priority does not list")
	downsampleRules := flag.String("downsample", "", "kind=intervalMs[:function] pairs (for example vibration=1000:max,motor_current=100) that fold the samples of a high-frequency kind into one per time bucket before they are buffered; the function is mean (default), min, max, last or sum; empty keeps every sample")
	allowAnonymous := flag.Bool("allow-anonymous", false, "allow a non-loopback -addr with no token (the ingest routes are then open to the whole network)")
	mqttURL := flag.String("mqtt-url", "", "broker to read the state of the bridges from (tcp://127.0.0.1:1883 or tls://host:8883); empty leaves the MQTT input off")
	mqttTopics := flag.String("mqtt-topics", mqttin.StateTopic+","+mqttin.AvailabilityTopic, "comma-separated topic filters the MQTT input subscribes to")
	mqttUser := flag.String("mqtt-username", os.Getenv(mqttUserEnv), "MQTT user name (default: $"+mqttUserEnv+")")
	mqttPasswordFile := flag.String("mqtt-password-file", "", "file holding the MQTT password (default: $"+mqttPasswordEnv+")")
	mqttCA := flag.String("mqtt-ca-file", "", "PEM file of the authority that signed the broker certificate (tls:// only)")
	mqttPlain := flag.Bool("mqtt-allow-plaintext", false, "allow credentials over a plain connection to a broker that is not on this machine")
	mqttMaxAge := flag.Duration("mqtt-max-age", mqttin.DefaultMaxAge, "oldest retained state that still counts as a reading")
	anomalyURL := flag.String("anomaly-url", "", "HYDRA-UMC-ANOMALY-DETECTOR base URL to stream windows of the delivered samples to (with -anomaly-rules); empty leaves the stream off")
	anomalyRules := flag.String("anomaly-rules", "", "kind/field=window pairs (for example vibration/accel=1024) naming the signals whose windows are sent to the detector; empty leaves the stream off")
	anomalyOverlap := flag.Int("anomaly-overlap", 0, "percent of each window that the next one repeats (0 to 90) for the rules that do not give their own kind/field=window@percent; 0 sends windows that do not overlap")
	anomalyToken := flag.String("anomaly-token", os.Getenv(anomalyTokenEnv), "bearer token sent to the detector (default: $"+anomalyTokenEnv+")")
	anomalyQueue := flag.Int("anomaly-queue", 64, "windows that may wait to be sent to the detector; a full queue drops the new window")
	anomalyMaxSeries := flag.Int("anomaly-max-series", 64, "most distinct (source, kind, field) signals the stream keeps a window for")
	anomalyPlain := flag.Bool("anomaly-allow-plaintext", false, "allow the detector token over plain http to a host that is not on this machine")
	identityKeyFile := flag.String("identity-key-file", os.Getenv(identityKeyFileEnv), "file with the signing key of HYDRA-UMC-SERVER (its JWT_SECRET): a valid token of the SERVER is then accepted next to the tokens, every route (/stats, /metrics and GET /health too) demands an identity and a viewer may read but not ingest; off by default (default: $"+identityKeyFileEnv+")")
	healthOpen := flag.Bool("health-open", healthOpenFromEnv(), "open GET /health (default: $"+healthOpenEnv+"); closed it answers 404; open it answers a caller on loopback, and any other caller needs the ingest or metrics token")
	canIDsFile := flag.String("can-ids", "", "CanIds contract of the SDK (contracts/can_ids.json): POST /ingest/can then reads the identifiers of the HYDRA-UMC stack bus (a health frame of a controller board becomes a sample, other known frames are left alone, an unknown identifier is refused) instead of the v0 convention; empty keeps the v0 convention")
	grpcAddr := flag.String("grpc-addr", "", "address for the gRPC ingest (TelemetryIngest of the SDK), for example 127.0.0.1:8093; empty leaves gRPC off")
	grpcTokenFile := flag.String("grpc-token-file", "", "file whose first line is the bearer token a gRPC caller must send (at least 16 characters); empty requires none")
	grpcCert := flag.String("grpc-tls-cert", "", "PEM certificate that turns TLS on for the gRPC ingest (with -grpc-tls-key)")
	grpcKey := flag.String("grpc-tls-key", "", "PEM private key of -grpc-tls-cert")
	grpcClientCA := flag.String("grpc-client-ca", "", "PEM authority that must have signed the certificate of a gRPC caller (mutual TLS; needs -grpc-tls-cert)")
	grpcAnonymous := flag.Bool("grpc-allow-anonymous", false, "allow a gRPC listener beyond loopback with no token and no client certificate, or a token in clear text")
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
	var downsampleTable map[string]downsample.Rule
	if problem == "" {
		var err error
		if downsampleTable, err = downsample.ParseRules(*downsampleRules); err != nil {
			problem = "-downsample: " + err.Error()
		}
	}
	var anomalyTable []anomaly.Rule
	if problem == "" {
		var err error
		if anomalyTable, err = anomaly.ParseRulesOverlap(*anomalyRules, *anomalyOverlap); err != nil {
			problem = "-anomaly-rules: " + err.Error()
		} else {
			problem = anomalyProblem(*anomalyURL, *anomalyToken, len(anomalyTable), *anomalyQueue, *anomalyMaxSeries, *anomalyPlain)
		}
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
	var identityKey []byte
	if *identityKeyFile != "" {
		key, err := api.LoadIdentityKey(*identityKeyFile)
		if err != nil {
			fmt.Fprintln(os.Stderr, "[telemetry-collector] -identity-key-file: "+err.Error())
			os.Exit(2)
		}
		identityKey = key
	}
	if problem := startupProblem(*addr, *token, *allowAnonymous || identityKey != nil); problem != "" {
		fmt.Fprintln(os.Stderr, "[telemetry-collector] "+problem)
		os.Exit(2)
	}

	grpcOptions := grpcin.Options{Addr: *grpcAddr, CertFile: *grpcCert, KeyFile: *grpcKey, ClientCAFile: *grpcClientCA, AllowAnonymous: *grpcAnonymous}
	if *grpcAddr != "" && *grpcTokenFile != "" {
		token, err := grpcin.ReadToken(*grpcTokenFile)
		if err != nil {
			fmt.Fprintln(os.Stderr, "[telemetry-collector] -grpc-token-file: "+err.Error())
			os.Exit(2)
		}
		grpcOptions.Token = token
	}
	if *grpcAddr == "" && (*grpcTokenFile != "" || *grpcCert != "" || *grpcKey != "" || *grpcClientCA != "") {
		fmt.Fprintln(os.Stderr, "[telemetry-collector] the -grpc-* options need -grpc-addr (the gRPC ingest is off without it)")
		os.Exit(2)
	}
	var canTable *canids.Table
	if *canIDsFile != "" {
		table, err := canids.Load(*canIDsFile)
		if err != nil {
			fmt.Fprintln(os.Stderr, "[telemetry-collector] -can-ids: "+err.Error())
			os.Exit(2)
		}
		canTable = table
	}

	fmt.Printf("%s v%s\n", projectName, Version)
	fmt.Println(role)

	var s sink.Sink
	if *datalakeURL != "" {
		lake := sink.NewDatalakeSink(*datalakeURL)
		lake.Token = *datalakeToken
		lake.Batch = *batch
		lake.Gzip = *batchGzip
		lake.PermanentStatuses = statuses
		s = lake
		fmt.Printf("[telemetry-collector] delivering to HYDRA-UMC-DATALAKE at %s\n", *datalakeURL)
	} else {
		s = sink.ConsoleSink{W: os.Stdout}
		fmt.Println("[telemetry-collector] no -datalake-url given, printing flushed samples to stdout")
	}

	var anomalyPub *anomaly.Publisher
	if len(anomalyTable) > 0 {
		anomalyPub = anomaly.New(anomaly.Options{URL: *anomalyURL, Token: *anomalyToken, Rules: anomalyTable, QueueMax: *anomalyQueue, MaxSeries: *anomalyMaxSeries})
		s = anomaly.Tee{Primary: s, Pub: anomalyPub}
		fmt.Printf("[telemetry-collector] streaming %d signal(s) to the anomaly detector at %s\n", len(anomalyTable), *anomalyURL)
	}
	c := collector.New(*bufferCap, s)
	var grpcServer *grpcin.Server
	if *grpcAddr != "" {
		grpcOptions.Submit = c.IngestSample
		srv, err := grpcin.New(grpcOptions)
		if err != nil {
			fmt.Fprintln(os.Stderr, "[telemetry-collector] "+err.Error())
			os.Exit(2)
		}
		grpcServer = srv
	}
	if canTable != nil {
		c.SetCANTable(canTable)
		fmt.Printf("[telemetry-collector] reading CAN frames with the identifiers of %s\n", *canIDsFile)
	}
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
	if downsampleTable != nil {
		c.SetDownsample(downsampleTable)
		fmt.Printf("[telemetry-collector] downsampling %d kind(s): %s\n", len(downsampleTable), *downsampleRules)
	}
	var mqttStats func() map[string]any
	server := api.NewWithOptions(c, api.Options{Token: *token, MetricsToken: *metricsToken, IdentityKey: identityKey, HealthOpen: *healthOpen, Service: "hydra-umc-telemetry-collector", Version: Version, ExtraStats: func() map[string]any {
		extra := map[string]any{}
		if mqttStats != nil {
			for key, value := range mqttStats() {
				extra[key] = value
			}
		}
		if anomalyPub != nil {
			for key, value := range anomalyPub.Stats() {
				extra[key] = value
			}
		}
		if canTable != nil {
			extra["can"] = canTable.Counters()
		}
		if grpcServer != nil {
			extra["grpc"] = grpcServer.Stats()
		}
		if len(extra) == 0 {
			return nil
		}
		return extra
	}})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go c.Run(ctx, *flushInterval, *batchSize)
	if grpcServer != nil {
		listener, err := net.Listen("tcp", *grpcAddr)
		if err != nil {
			fmt.Fprintln(os.Stderr, "[telemetry-collector] gRPC: "+err.Error())
			os.Exit(2)
		}
		security := "no TLS"
		if *grpcCert != "" {
			security = "TLS"
			if *grpcClientCA != "" {
				security = "mutual TLS"
			}
		}
		fmt.Printf("[telemetry-collector] gRPC ingest listening on %s (%s, token %t): TelemetryIngest.Push and PushStream\n", listener.Addr(), security, grpcOptions.Token != "")
		go func() {
			if err := grpcServer.Serve(listener); err != nil {
				log.Printf("gRPC ingest stopped: %v", err)
			}
		}()
		go func() {
			<-ctx.Done()
			grpcServer.GracefulStop()
		}()
	}
	if anomalyPub != nil {
		go anomalyPub.Run(ctx)
	}

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
