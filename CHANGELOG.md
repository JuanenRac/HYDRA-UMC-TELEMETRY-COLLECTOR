# Changelog

All notable work on **HYDRA-UMC-TELEMETRY-COLLECTOR** is summarized here, newest first. Full
This file intentionally omits calendar dates from individual entries.

## Versioning scheme

`version.go`'s `Version` constant is bumped automatically by
`bump_version.py`, run from `build.sh`/`build.bat` before every real
release build (`go build`).

It follows the ecosystem-wide base-10 "odometer" rule rather than
semantic-versioning judgment calls:

- `PATCH` +1 on every build
- when `PATCH` would exceed 9, it resets to 0 and `MINOR` +1 instead (e.g. `0.0.9` -> `0.1.0`, never `0.0.10`)
- the same carry cascades into `MAJOR` if `MINOR` would exceed 9

---

## Unreleased - finite telemetry field validation

- Rejects empty field names and `NaN`/infinite numeric values before they
  reach the buffer or an external sink. CAN decoding now performs the same
  validation as JSON ingestion, so malformed sensor payloads fail at the
  protocol boundary instead of becoming a delayed downstream error.
- **`main.go`**'s `-addr` flag now defaults to `127.0.0.1:8092` instead of
  `:8092` - an unqualified port binds every interface, and this HTTP API
  (`POST /ingest/can`, `POST /ingest/ws`, `GET /stats`) has no
  authentication of its own. The real CM5 deployment was never exposed
  (`systemd/hydra-umc-telemetry-collector.service` already pins
  `127.0.0.1:8092` explicitly), but running the binary directly without
  `-addr` - local dev, a manual invocation - listened on every interface
  by default. README run examples updated to match in every language.

---

## Documentation - Real HTTP API reference

- **`docs/API.md`** (new) - every real endpoint (`POST /ingest/can`,
  `POST /ingest/ws`, `GET /stats`) documented from the actual handler
  code in `api.go`, the exact 8-byte CAN frame encoding in `telemetry/can.go`
  (signal type byte + little-endian float32), and every `Stats` field's
  meaning in `collector.go` - including the exact `buffer: full` error
  text for the real `503` backpressure case. Documentation-only - no
  code changed, no version bump.

---

## [0.0.9] - Real CM5 deployment

- **`systemd/hydra-umc-telemetry-collector.service`** (new) - loopback-only
  unit for `HYDRA-UMC-OS/provisioning/install_telemetry_collector.sh`
  (new, that repo), which builds this pure-Go binary on-device (`go.mod`
  lives in `src/`, the one build-shape difference from the sibling Go
  services). Wired to HYDRA-UMC-DATALAKE's real `POST /ingest`, already
  installed loopback-only on the same CM5, via `-datalake-url`. Real gap
  found auditing the ecosystem against actual CM5 hardware: the real
  ingestion pipeline had never been built or installed anywhere.

## [0.0.8] - Real ecosystem live-status opt-in

- **`hydra-umc.project.json`** declares its real `service.port` (8092)
  and `health_path` (`/stats`) - HYDRA-UMC-SERVER's ecosystem status
  endpoint now does a real HTTP GET against it (expecting 2xx) instead
  of only reporting static manifest metadata.

## [0.0.7] - Fixed after a live ecosystem bug audit

- **`sink/sink.go`** doc comment - said DATALAKE "has no ingest endpoint
  of its own yet" and that there was "nothing real to write an HTTP/gRPC
  sink against today". That went stale when `sink/datalake.go`'s
  `DatalakeSink` (a real client against DATALAKE's `POST /ingest`) was
  added - `main.go`'s own comment already described it correctly. Reworded
  to describe what actually exists today: `ConsoleSink` as the honest
  default for local development/no-DATALAKE deployments, `DatalakeSink`
  as the real, already-wired alternative.

## [0.0.6] - Real sequence deduplication and transport-vs-invalid-data error classification

- **`Sample.Sequence`** (new, optional `uint64`) - a per-producer monotonic counter a real device can attach; `0`/omitted means "not provided" and behaves exactly as before, unchanged.
- **`src/dedup` (new package)** - `Tracker.Allow(sourceID, sequence)`, a real, bounded (256-entry-per-source) reorder-window deduplicator: the exact `(sourceId, sequence)` pair rejected if already seen or stale (far behind the source's own high-water mark), a genuinely reordered-but-recent sequence still allowed. This is the real mechanism behind "a device that reconnects and resends its last few unacked messages doesn't inflate ingest counts" - `collector.go`'s `ingest()` now checks it before buffering, returning the new `ErrDuplicate`. `POST /ingest/{can,ws}` treats a duplicate as an idempotent `200 {"status": "duplicate"}`, not an error - a real reconnect shouldn't look like a client failure.
- **`sink.InvalidDataError`** (new) - `DatalakeSink` now distinguishes a real HTTP 400 from DATALAKE (the data itself was rejected - retrying the identical bytes won't help) from any other failure (network error, timeout, 5xx - a real transport problem, where a retry might succeed). `collector.go`'s existing all-or-nothing requeue-and-retry behavior is unchanged either way - this only makes the *reason* a flush failed visible, via two new `Stats`/`GET /stats` fields: `invalidDataErrors` and `transportErrors`.
- New `GET /stats` fields: `duplicates`, `invalidDataErrors`, `transportErrors` - all additive, existing fields unchanged.
- 16 new tests across `dedup` (new package, 7 tests), `collector` (5 new: duplicate rejection, a real disconnect/reconnect-resend scenario, samples without a sequence are never deduplicated, invalid-data vs. transport classification), `sink` (3 new: a real 400 classified as invalid data, a real 500 and a real connection failure both classified as transport), and `api` (1 new: the real end-to-end reconnect-resend round trip through `POST /ingest/ws` and `GET /stats`) = 43 total, all passing (`go test ./...`, `go vet ./...` clean).
- Real verification beyond the test suite: ran the actual compiled binary with a 3-sample buffer, sent sequences 1 and 2 over real HTTP, resent sequence 2 (simulating a reconnect) and confirmed a `200 duplicate` with `ingested` still at the pre-resend count, then filled the buffer and confirmed a real `503` backpressure response - `GET /stats` afterward showed `ingested=3, duplicates=1` exactly.

## [0.0.5] - Real HYDRA-UMC-DATALAKE sink

- **`src/sink/datalake.go`** - `DatalakeSink`, a real `Sink` implementation
  that writes each sample to a running HYDRA-UMC-DATALAKE instance's
  `POST /ingest` (one HTTP request per sample - DATALAKE's own API is
  single-sample, not batch). Selected via the new `-datalake-url` flag;
  `ConsoleSink` remains the default for running this collector standalone.
- Honest, documented limitation: a batch that fails partway through
  causes the whole batch to be requeued by `collector.go`'s existing
  retry logic, so already-written samples get re-sent and land as
  duplicate rows in DATALAKE on the next successful flush - at-least-once
  with occasional duplicates, not silently dropping data. Real
  exactly-once delivery (idempotency keys, upserts on the DATALAKE side)
  is future work, not attempted here.
- Verified for real: 3 new `sink` package tests against a real
  `net/http/httptest.Server` implementing DATALAKE's actual `POST
  /ingest` contract (202 on success) - one confirms every sample in a
  batch is delivered with its content intact, one confirms a non-202
  response is treated as a real failure, one confirms a batch stops
  sending at the first failure rather than continuing past it.

## [0.0.4] - Real ingestion pipeline: CAN/WS parsing, bounded buffer, retry-on-outage delivery

- **`src/telemetry`** - `Sample`, the normalized shape both heterogeneous
  sources parse into. `ParseCANFrame`/`EncodeCANFrame` implement a real,
  round-tripping 8-byte CAN payload format (this project's own v0
  convention - the ecosystem's real CAN ID tables live in HYDRA-UMC's and
  URTC's own firmware docs, not guessed at here). `ParseWSMessage` parses
  and validates the JSON telemetry format.
- **`src/buffer`** - `Ring`, a real bounded, concurrency-safe FIFO.
  `Push` rejects (`ErrFull`) instead of silently overwriting when full -
  "zero data loss" and "drop the oldest reading when busy" are
  contradictory promises, so this buffer keeps the honest one. `Requeue`
  puts samples back at the front after a failed delivery.
- **`src/sink`** - the `Sink` interface + `ConsoleSink`, a real (not a
  no-op stub) destination standing in for HYDRA-UMC-DATALAKE, which has
  no ingest endpoint of its own yet.
- **`src/collector`** - the real orchestration: `IngestCAN`/`IngestWS`
  parse and buffer; `FlushOnce` drains a batch and writes it to the sink,
  requeuing the WHOLE batch at the front on failure instead of losing it
  - the actual mechanism behind "Buffered Delivery: zero data loss during
    temporary database outages", not just a comment saying so.
- **`src/api`** - plain JSON/HTTP surface (stdlib `net/http`): `POST
  /ingest/can`, `POST /ingest/ws`, `GET /stats`. A full buffer reports
  HTTP 503, not a silent drop.
- **`src/main.go`** - now wires everything together and starts a real
  HTTP server + background flush loop, instead of only printing identity
  and exiting. `build.sh`/`build.bat`'s old "run it once to verify" step
  was removed - the entry point is now a real long-running server, so
  running it from inside the build script would hang the build instead
  of verifying anything.
- Verified for real: `go build ./...`, `go vet ./...` clean; 24 `go test
  ./...` cases across all 5 packages, including a CAN frame round-trip
  test, a concurrent-push safety test for the buffer, and - the one that
  matters most for this project's own core promise - a test that fails a
  sink write, confirms the batch is requeued (not lost), then confirms
  the SAME samples (order preserved) are delivered once the sink
  recovers. Additionally smoke-tested the compiled binary end-to-end:
  real `curl` requests ingesting one WS sample and one CAN frame,
  confirmed both normalized and flushed correctly via `/stats` and the
  sink's own stdout output.
- `build.sh`/`build.bat`/`run.sh`/`run.bat` updated with the
  ecosystem-wide no-auto-close convention (`pause` in `.bat`, an `EXIT`
  trap in `.sh`) and argument forwarding, matching every other project
  across the ecosystem.
- What's still not real, on purpose: a real
  HYDRA-UMC-DATALAKE sink (DATALAKE has no ingest endpoint yet), a real
  CAN bus / WebSocket stream from HYDRA-UMC-SERVER (this collector is
  fed via its own HTTP endpoints today, not a live listener), and
  integrating the CAN wire format against the ecosystem's actual
  documented CAN IDs.

## [0.0.2] - Initial scaffolding

- **`src/main.go`** - minimal real entry point. No collection logic yet - aggregating per-robot telemetry ahead of HYDRA-UMC-DATALAKE's own ingestion lands in a later pass.
- **`src/version.go`** - version identity (`Version` constant). Module root is `src/`, not the repo root.
- **`build.sh` / `build.bat`**, **`run.sh` / `run.bat`** - `go build ./...` from inside `src/` and run the resulting binary.
