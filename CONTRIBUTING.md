# Contributing to HYDRA-UMC-TELEMETRY-COLLECTOR 🦾

We welcome contributions to the high-throughput ingestion node of the HYDRA-UMC platform.

## Technology Stack
- **Languages**: Go 1.22+, Rust 1.80+.
- **Protocols**: FDCAN (SocketCAN), gRPC, WebSocket.
- **Serialization**: Protobuf, FlatBuffers (for ultra-low latency).
- **Infrastructure**: Linux (Real-time network stack).

## Guidelines
1. **Zero-Copy Ingestion**: Ensure that packet parsing minimizes memory allocations to handle thousands of messages per millisecond.
2. **Buffer Management**: Implement back-pressure mechanisms to prevent memory exhaustion during datalake ingestion spikes.
3. **Multi-Protocol Support**: Any new listener must follow the common `TelemetryIngester` interface for consistent normalization.
4. **Testing**: Validate ingestion throughput using the `scripts/stress_test.go` tool against a simulated 8-node swarm.
