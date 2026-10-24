# HTTP API Reference

Real, plain JSON/HTTP surface (`net/http`, no framework) implemented in
[`src/api/api.go`](../src/api/api.go), over the ingestion pipeline in
[`src/collector/collector.go`](../src/collector/collector.go). There is
collector that reads a CAN bus or opens a WebSocket: it only receives what a
producer pushes to it. `POST /ingest/can` takes one CAN frame and `POST /ingest/ws`
one sample in the collector's own JSON shape (despite the name, it is a plain HTTP
`POST`, not a WebSocket); something else has to read the bus or the stream and
forward it here, and no component of the ecosystem does yet.

Towards HYDRA-UMC-DATALAKE the collector delivers each flush with the datalake's `POST /ingest/batch` (up to 1000 samples per request, one transaction); a datalake that has no such route (`404`/`405`) is remembered and the collector goes back to one `POST /ingest` per sample, and a batch the datalake rejects as a whole (`400`/`413`) is repeated sample by sample so that only the invalid one is discarded. `-datalake-batch=false` always uses one request per sample. It sends `Authorization: Bearer <token>` with every sample when `-datalake-token` (or `HYDRA_UMC_DATALAKE_TOKEN`) is set; a datalake that requires a token and does not get a valid one answers `401`, and the samples wait in the buffer.

Authentication is optional. Started with a token (`-token`, or the
`HYDRA_UMC_TELEMETRY_COLLECTOR_TOKEN` environment variable; at least 16
characters), the two `POST /ingest/*` routes require
`Authorization: Bearer <token>` and answer `401` (`WWW-Authenticate: Bearer`)
otherwise; `GET /stats` and `GET /metrics` stay open unless `-metrics-token` (or `HYDRA_UMC_TELEMETRY_COLLECTOR_METRICS_TOKEN`, at least 16 characters) is set: they then require `Authorization: Bearer <that token>` (a different token from the ingest one, so a scraper needs no write access) and answer `401` otherwise. Without a token the
collector only starts on a loopback address: a non-loopback `-addr` is refused
unless `-allow-anonymous` is passed. A request body larger than 1 MiB is
refused with `413` and never read in full.

---

## `POST /ingest/can`

Ingests one raw CAN frame (this v0's own fixed 8-byte encoding - see [`telemetry/can.go`](../src/telemetry/can.go)).

**Request body**

```json
{
  "arbitrationId": 3,
  "data": "AaAAQBQAAAA="
}
```

- `arbitrationId` (uint32) - the CAN arbitration ID; `telemetry.Sample.SourceID` becomes `"robot-<arbitrationId>"`.
- `data` (base64 string in JSON, per Go's `[]byte` convention) - must decode to **exactly 8 bytes**:
  - byte 0: signal type code - `0x01` = `motor_current`, `0x02` = `motor_temp`, `0x03` = `motor_rpm` (any other value is rejected).
  - bytes 1-4: the value, a little-endian IEEE-754 float32.
  - bytes 5-7: unused in this v0 format.

**Responses**

| Status | Body | Meaning |
|---|---|---|
| 202 | `{"status": "buffered"}` | Frame parsed and pushed onto the internal ring buffer. |
| 401 | `{"error": "a valid bearer token is required"}` | A token is configured and the request has no valid `Authorization: Bearer` header. |
| 413 | `{"error": "the body may be at most 1048576 bytes"}` | The request body is larger than the cap. |
| 400 | `{"error": "<message>"}` | Malformed JSON, `data` not exactly 8 bytes, an unknown signal type code, or a decoded value that fails `Sample.Validate()` (e.g. a non-finite float32). |
| 503 | `{"error": "buffer: full"}` | The ring buffer has no room - real backpressure, not a silently dropped sample. |

## `POST /ingest/ws`

Ingests one telemetry sample already in this collector's own `Sample` JSON shape (the same shape a real WebSocket stream from HYDRA-UMC-SERVER would deliver).

**Request body**

```json
{
  "sourceId": "vision-node-1",
  "kind": "motor_temp",
  "timestamp": 1735689600000,
  "fields": {"value": 41.2}
}
```

`sourceId` and `kind` are required (non-empty) - see `Sample.Validate()` in [`telemetry/sample.go`](../src/telemetry/sample.go). `sourceId` has at most 128 visible ASCII characters. `timestamp` is Unix epoch **milliseconds**: a value below 100000000000 (a producer that sent seconds) and one more than `-max-future-skew` (default 5 minutes, `0` turns it off) ahead of this machine's clock are refused with `400`. Every key in `fields` must be non-empty, and every value must be a finite number - a `NaN`/`Infinity`/`-Infinity` value or an empty field name is rejected (`400`) rather than buffered and surfacing as a downstream error later. `POST /ingest/can` runs through this exact same `Validate()` call after decoding, so a malformed CAN payload is rejected at the same boundary as malformed JSON. `sequence` (uint64, optional) is a real per-producer monotonic counter - see "Deduplication" below.

**Responses** - same shape as `/ingest/can` above (`202` on success, `400` for malformed JSON or a failed `Validate()`, `503` if the buffer is full), plus:

| Status | Body | Meaning |
|---|---|---|
| 200 | `{"status": "duplicate"}` | This exact `(sourceId, sequence)` pair was already ingested - a real reconnect resending an unacked message, treated as an idempotent no-op, not an error. Only possible when `sequence` is provided and non-zero. |

---

## Deduplication (real reconnect/resend handling)

A producer may attach an optional `sequence` (uint64) to each sample - a per-source monotonic counter. `sequence: 0` (or the field omitted) means "not provided": that sample is never deduplicated, matching this collector's original behavior exactly.

When `sequence` is provided, [`dedup.Tracker`](../src/dedup/dedup.go) remembers, per `sourceId`, the most recent sequence numbers within a bounded reorder window (256). A sequence already seen for that source - or one far enough behind the highest one accepted to be a stale replay rather than legitimate reordering - is rejected as a duplicate (`200 {"status": "duplicate"}`, counted in `GET /stats`'s `duplicates`, never re-buffered). This is the real mechanism behind "a device that reconnects and resends its last few unacked messages doesn't inflate ingest counts."

A producer that **restarts** begins its counter again, far behind everything it sent before; its new samples are not rejected for as long as it takes to catch up. A sequence behind the window that is no larger than the window itself (the counter started over) is accepted as a restart at once, and so is the third of a run of increasing numbers that all fall behind the window (a counter that restarted somewhere else); the producer's state is then reset and `producerResets` in `GET /stats` goes up. An isolated old sequence, or old ones in no increasing order, stays a stale replay. At most 10000 producers are remembered: past that, the one that has been silent the longest is forgotten.

---

## `GET /stats`

**Response** - `200`:

```json
{
  "ingested": 1042,
  "ingestErrors": 3,
  "duplicates": 5,
  "flushed": 980,
  "flushErrors": 1,
  "invalidDataErrors": 0,
  "transportErrors": 1,
  "dropped": 0,
  "quarantined": 0,
  "producerResets": 0,
  "producers": 12,
  "inFlight": 0,
  "bufferLen": 62,
  "bufferCap": 4096
}
```

- `ingested` - samples successfully parsed and buffered.
- `ingestErrors` - `/ingest/can` or `/ingest/ws` calls rejected (bad frame/JSON).
- `duplicates` - samples rejected as an already-seen `(sourceId, sequence)` pair - see "Deduplication" above.
- `flushed` - samples successfully written to the sink.
- `flushErrors` - failed sink writes (what was not written is put back, not lost - see `collector.go`'s own doc comment on `FlushOnce`).
- `invalidDataErrors` - of those `flushErrors`, how many were the sink (e.g. HYDRA-UMC-DATALAKE) genuinely rejecting the data's content (a real HTTP 400, or 413 for a sample that is too large) - retrying the identical bytes will not help; see `sink.InvalidDataError`. A `401`, `403` or `404` is NOT counted here: a wrong token or URL is fixed by fixing the configuration, so the samples wait (and `transportErrors` goes up) instead of being discarded.
- `transportErrors` - of those `flushErrors`, how many were a transport-level problem (network error, timeout, 5xx) - a retry might genuinely succeed. Every `flushErrors` is counted as exactly one of `invalidDataErrors` or `transportErrors`.
- `dropped` - samples actually lost because a requeue after a flush failure outran the buffer's capacity. A batch being delivered keeps its slots in the buffer until it is settled (`inFlight`), so a sink outage fills the buffer and makes ingestion answer `503` instead of discarding the oldest samples: this stays 0 unless something is badly wrong.
- `quarantined` - samples discarded because the sink permanently rejected them.
- `producerResets` / `producers` - producers whose counter started over (a restart) and producers currently remembered by the duplicate check.
- `inFlight` - buffer slots held by the batch being delivered.
- `bufferLen` / `bufferCap` - current vs. maximum ring buffer occupancy.
- `evicted` - queued samples dropped to make room for a more important one; only possible with `-priority` (see below), and not part of `dropped`.
- `lastFlushMs` / `maxFlushMs` - how long the last and the slowest delivery of a batch to the sink took.
- `oldestSampleAgeMs` - age, by its own `timestamp`, of the sample that has waited longest in the buffer (`0` when it is empty).
- `spooled` / `spoolPending` / `spoolBytes` - samples written to the disk spool, and what it still holds (all `0` without `-spool`).

## Optional protections and queues (all off by default)

| Flag | What it does |
|---|---|
| `-spool <file>` | A sink failure writes the batch to this file (one JSON line per sample) instead of keeping it in memory, so the buffer keeps accepting samples during a long outage. When the sink answers again the spool is replayed before anything newer. Bounded by `-spool-max-bytes` (default 256 MiB); a full spool falls back to keeping the batch in memory. What a previous run left in the file is replayed at start. Delivery is at least once: after a crash a batch can be delivered twice, and HYDRA-UMC-DATALAKE stores a point once per `(source, kind, field, timestamp)`. |
| `-priority kind=level,...` and `-priority-default <n>` | A full buffer drops the least important queued sample (the oldest among equals) to take a more important one; without it a full buffer refuses new samples with `503`. Dropped ones are counted in `evicted`. |
| `-datalake-permanent-statuses 401,403,404` | HTTP client errors (400-499) from the datalake that discard the sample (it is counted as `quarantined`) instead of waiting for the configuration to be fixed. Default empty: they wait. |
| `-metrics-token` | Makes `GET /stats` and `GET /metrics` require a bearer token. |

---

## Errors

Startup settings are checked: `-buffer`, `-batch-size` and `-flush-interval` must be positive (a zero or negative one used to crash the flush loop or never drain the buffer). Any other path/method returns Go's default `405 Method Not Allowed` (with an `Allow` header) for a known path with the wrong verb, or `404 page not found` (stdlib `net/http` default) for an unknown path.

## MQTT input

Off unless `-mqtt-url` is given (`tcp://host:1883` or `tls://host:8883`). Flags: `-mqtt-topics` (default `hydra/bridges/+/state,hydra/bridges/+/availability`), `-mqtt-username` / `HYDRA_UMC_TELEMETRY_MQTT_USER`, `-mqtt-password-file` / `HYDRA_UMC_TELEMETRY_MQTT_PASSWORD`, `-mqtt-ca-file`, `-mqtt-allow-plaintext`, `-mqtt-max-age`. Samples: `bridge_state` (source `bridge-<name>`; fields are the numbers and flags of the state, nested one level as `name.sub`, plus `machine_state.<value>` = 1) and `bridge_availability` (`online` = 1 or 0). `GET /stats` adds `mqtt.connection` (`connected`, `received`, `reconnects`, `lastError`) and `mqtt.samples` (`accepted`, `ignored`, `rejected`, `submitFailures`).
