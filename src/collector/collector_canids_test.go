// HYDRA-UMC-TELEMETRY-COLLECTOR - collector/collector_canids_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
//
// IngestCAN with and without the table of the CanIds contract: the v0 convention is untouched when no table is set, and with one the
// frame is read by its identifier.
package collector

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/canids"
	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

func healthFrame(sequence byte) []byte {
	frame := []byte{0x48, sequence, 0x20, 1, 2, 3, 4, 0}
	check := byte(0xA5)
	for _, b := range frame[:7] {
		check ^= b
	}
	frame[7] = check
	return frame
}

func table(t *testing.T) *canids.Table {
	t.Helper()
	tab, err := canids.Load(filepath.Join("..", "canids", "testdata", "can_ids.json"))
	if err != nil {
		t.Fatal(err)
	}
	return tab
}

func TestWithoutATableTheV0ConventionIsUntouched(t *testing.T) {
	c := New(10, &memorySink{})
	frame, _ := telemetry.EncodeCANFrame("motor_current", 2.5)
	if err := c.IngestCAN(7, frame[:]); err != nil {
		t.Fatal(err)
	}
	if err := c.IngestCAN(0x611, healthFrame(1)); err == nil {
		t.Fatal("a health frame is not a v0 frame and was accepted")
	}
	if c.Stats().Ingested != 1 || c.Stats().IngestErrors != 1 {
		t.Fatalf("%+v", c.Stats())
	}
}

func TestWithATableAHealthFrameIsDeliveredAsASampleOfItsBoard(t *testing.T) {
	sink := &memorySink{}
	c := New(10, sink)
	c.SetCANTable(table(t))
	if err := c.IngestCAN(0x600+5*64+0x11, healthFrame(9)); err != nil {
		t.Fatal(err)
	}
	if c.FlushOnce(10) != 1 || len(sink.written) != 1 {
		t.Fatalf("flushed %d", len(sink.written))
	}
	got := sink.written[0][0]
	if got.SourceID != "board-5" || got.Kind != "board_health" || got.Fields["sequence"] != 9 || got.Fields["drivers_enabled"] != 1 {
		t.Fatalf("%+v", got)
	}
	if age := time.Now().UnixMilli() - got.Timestamp; age < 0 || age > 5000 {
		t.Fatalf("timestamp %d is not the arrival time", got.Timestamp)
	}
}

func TestWithATableKnownTrafficIsAcceptedAndNotBuffered(t *testing.T) {
	c := New(10, &memorySink{})
	c.SetCANTable(table(t))
	for _, id := range []uint32{0x600 + 0x10, 0x600 + 3} {
		if err := c.IngestCAN(id, []byte{1, 2}); err != nil {
			t.Fatalf("0x%X: %v", id, err)
		}
	}
	if c.BufferLen() != 0 || c.Stats().Ingested != 0 || c.Stats().IngestErrors != 0 {
		t.Fatalf("len=%d %+v", c.BufferLen(), c.Stats())
	}
}

func TestWithATableAnUnknownIdentifierOrABadPayloadIsAnIngestError(t *testing.T) {
	c := New(10, &memorySink{})
	c.SetCANTable(table(t))
	if err := c.IngestCAN(0x095, healthFrame(1)); !errors.Is(err, canids.ErrUnknownID) {
		t.Fatalf("%v", err)
	}
	if err := c.IngestCAN(0x611, []byte{0x48}); !errors.Is(err, canids.ErrBadPayload) {
		t.Fatalf("%v", err)
	}
	if c.Stats().IngestErrors != 2 || c.BufferLen() != 0 {
		t.Fatalf("%+v", c.Stats())
	}
}

func TestWithATableAFullBufferIsStillBackpressure(t *testing.T) {
	c := New(1, &memorySink{})
	c.SetCANTable(table(t))
	if err := c.IngestCAN(0x611, healthFrame(1)); err != nil {
		t.Fatal(err)
	}
	if err := c.IngestCAN(0x611, healthFrame(2)); err == nil {
		t.Fatal("a full buffer accepted another sample")
	}
}

func TestWithATableAStopFrameIsBufferedAsASampleOfTheStack(t *testing.T) {
	sink := &memorySink{}
	c := New(10, sink)
	c.SetCANTable(table(t))
	if err := c.IngestCAN(1, []byte{1, 4, 3, 7, 5, 0, 0xFE, 0xFB}); err != nil {
		t.Fatal(err)
	}
	if c.FlushOnce(10) != 1 || sink.written[0][0].Kind != "estop_frame" || sink.written[0][0].Fields["malformed"] != 0 || sink.written[0][0].Fields["is_trip"] != 1 {
		t.Fatalf("%+v", sink.written)
	}
}
