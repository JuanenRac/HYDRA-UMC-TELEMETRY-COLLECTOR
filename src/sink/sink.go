// HYDRA-UMC-TELEMETRY-COLLECTOR - sink/sink.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// Sink is where drained batches actually go. HYDRA-UMC-DATALAKE (the
// real destination the README names) has no ingest endpoint of its own
// yet - it's andamiaje beyond its own entry point too - so there is
// nothing real to write an HTTP/gRPC sink against today. ConsoleSink is
// the honest v0: a real, working sink (not a no-op stub) that proves the
// collector's own delivery path end-to-end, swappable for a DATALAKE
// sink later without touching collector.go (Sink is the seam).
package sink

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

// Sink writes a batch of samples somewhere durable. Returning an error
// means NONE of the batch was durably written - collector.go relies on
// that all-or-nothing contract to decide whether to requeue the whole
// batch.
type Sink interface {
	Write(batch []telemetry.Sample) error
}

// ConsoleSink writes each batch as newline-delimited JSON to an
// io.Writer (stdout in main.go) - a real destination, genuinely
// inspectable, standing in for HYDRA-UMC-DATALAKE until that project has
// a real ingest endpoint to write against.
type ConsoleSink struct {
	W io.Writer
}

func (c ConsoleSink) Write(batch []telemetry.Sample) error {
	for _, s := range batch {
		raw, err := json.Marshal(s)
		if err != nil {
			return fmt.Errorf("sink: marshal sample: %w", err)
		}
		if _, err := fmt.Fprintln(c.W, string(raw)); err != nil {
			return fmt.Errorf("sink: write: %w", err)
		}
	}
	return nil
}
