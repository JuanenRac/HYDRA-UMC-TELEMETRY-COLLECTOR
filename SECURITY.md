# Security Policy 🔒 (HYDRA-UMC-TELEMETRY-COLLECTOR)

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.x.x  | ✅ Yes             |

## Reporting a Vulnerability

**CRITICAL: Do not report safety-critical vulnerabilities through public GitHub issues.**

In a telemetry gateway, a security flaw can lead to "telemetry poisoning" or masking of critical hardware failures. If you discover a vulnerability affecting the **CAN packet parser**, **gRPC stream authentication**, or **buffer overflow in ingestion**:

1. **Email**: Send a detailed report to `electrohobby3d@gmail.com`.
2. **Impact**: Describe if the bug allows injecting fake sensor data, hiding motor current spikes, or crashing the ingestion pipeline.
3. **Response**: Initial acknowledgment within 48 hours.

We follow a coordinated disclosure policy to ensure hardware safety before public release.
