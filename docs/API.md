# HTTP API Reference

Real, plain JSON/HTTP surface (`net/http`, no framework) implemented in
[`src/api/api.go`](../src/api/api.go), over the ingestion pipeline in
[`src/collector/collector.go`](../src/collector/collector.go). There is
no real CAN bus or WebSocket stream from HYDRA-UMC-SERVER available in
this environment yet - `POST /ingest/can` and `POST /ingest/ws` let a
real caller (or a test/curl) feed the collector genuine frames without
needing that hardware/network dependency, so the buffering/flush
pipeline is proven with real data either way.

No authentication - internal/same-network use.

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

`sourceId` and `kind` are required (non-empty) - see `Sample.Validate()` in [`telemetry/sample.go`](../src/telemetry/sample.go). `timestamp` is Unix epoch milliseconds. Every key in `fields` must be non-empty, and every value must be a finite number - a `NaN`/`Infinity`/`-Infinity` value or an empty field name is rejected (`400`) rather than buffered and surfacing as a downstream error later. `POST /ingest/can` runs through this exact same `Validate()` call after decoding, so a malformed CAN payload is rejected at the same boundary as malformed JSON. `sequence` (uint64, optional) is a real per-producer monotonic counter - see "Deduplication" below.

**Responses** - same shape as `/ingest/can` above (`202` on success, `400` for malformed JSON or a failed `Validate()`, `503` if the buffer is full), plus:

| Status | Body | Meaning |
|---|---|---|
| 200 | `{"status": "duplicate"}` | This exact `(sourceId, sequence)` pair was already ingested - a real reconnect resending an unacked message, treated as an idempotent no-op, not an error. Only possible when `sequence` is provided and non-zero. |

---

## Deduplication (real reconnect/resend handling)

A producer may attach an optional `sequence` (uint64) to each sample - a per-source monotonic counter. `sequence: 0` (or the field omitted) means "not provided": that sample is never deduplicated, matching this collector's original behavior exactly.

When `sequence` is provided, [`dedup.Tracker`](../src/dedup/dedup.go) remembers, per `sourceId`, the most recent sequence numbers within a bounded reorder window (256). A sequence already seen for that source - or one far enough behind the highest one accepted to be a stale replay rather than legitimate reordering - is rejected as a duplicate (`200 {"status": "duplicate"}`, counted in `GET /stats`'s `duplicates`, never re-buffered). This is the real mechanism behind "a device that reconnects and resends its last few unacked messages doesn't inflate ingest counts."

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
  "bufferLen": 62,
  "bufferCap": 4096
}
```

- `ingested` - samples successfully parsed and buffered.
- `ingestErrors` - `/ingest/can` or `/ingest/ws` calls rejected (bad frame/JSON).
- `duplicates` - samples rejected as an already-seen `(sourceId, sequence)` pair - see "Deduplication" above.
- `flushed` - samples successfully written to the sink.
- `flushErrors` - failed sink writes (the whole batch is requeued, not lost - see `collector.go`'s own doc comment on `FlushOnce`).
- `invalidDataErrors` - of those `flushErrors`, how many were the sink (e.g. HYDRA-UMC-DATALAKE) genuinely rejecting the data's content (a real HTTP 400) - retrying the identical bytes will not help; see `sink.InvalidDataError`.
- `transportErrors` - of those `flushErrors`, how many were a transport-level problem (network error, timeout, 5xx) - a retry might genuinely succeed. Every `flushErrors` is counted as exactly one of `invalidDataErrors` or `transportErrors`.
- `dropped` - samples actually lost because a requeue after a flush failure outran the buffer's capacity.
- `bufferLen` / `bufferCap` - current vs. maximum ring buffer occupancy.

---

## Errors

Any other path/method returns Go's default `405 Method Not Allowed` (with an `Allow` header) for a known path with the wrong verb, or `404 page not found` (stdlib `net/http` default) for an unknown path.
