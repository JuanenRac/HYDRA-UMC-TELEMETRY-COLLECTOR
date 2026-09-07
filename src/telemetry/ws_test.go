// HYDRA-UMC-TELEMETRY-COLLECTOR - telemetry/ws_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
package telemetry

import (
	"errors"
	"math"
	"testing"
)

func TestParseWSMessage_Valid(t *testing.T) {
	raw := []byte(`{"sourceId":"robot-1","kind":"motor_temp","timestamp":1700000000000,"fields":{"value":42.5}}`)
	sample, err := ParseWSMessage(raw)
	if err != nil {
		t.Fatalf("ParseWSMessage: %v", err)
	}
	if sample.SourceID != "robot-1" || sample.Kind != "motor_temp" {
		t.Errorf("unexpected sample: %+v", sample)
	}
	if sample.Fields["value"] != 42.5 {
		t.Errorf("Fields[value] = %v, want 42.5", sample.Fields["value"])
	}
}

func TestParseWSMessage_RejectsMalformedJSON(t *testing.T) {
	_, err := ParseWSMessage([]byte(`{not valid json`))
	if err == nil {
		t.Fatal("expected an error for malformed JSON, got nil")
	}
}

func TestParseWSMessage_RejectsMissingSourceID(t *testing.T) {
	raw := []byte(`{"kind":"motor_temp","timestamp":1700000000000,"fields":{}}`)
	_, err := ParseWSMessage(raw)
	if err != ErrMissingSourceID {
		t.Fatalf("err = %v, want ErrMissingSourceID", err)
	}
}

func TestParseWSMessage_RejectsMissingTimestamp(t *testing.T) {
	raw := []byte(`{"sourceId":"robot-1","kind":"motor_temp","fields":{}}`)
	_, err := ParseWSMessage(raw)
	if err != ErrInvalidTimestamp {
		t.Fatalf("err = %v, want ErrInvalidTimestamp", err)
	}
}

func TestSampleValidateRejectsNonFiniteOrUnnamedFields(t *testing.T) {
	base := Sample{SourceID: "robot-1", Kind: "motor_temp", Timestamp: 1}
	for name, fields := range map[string]map[string]float64{
		"nan":      {"value": math.NaN()},
		"infinity": {"value": math.Inf(1)},
		"unnamed":  {"": 1},
	} {
		t.Run(name, func(t *testing.T) {
			sample := base
			sample.Fields = fields
			if !errors.Is(sample.Validate(), ErrInvalidField) {
				t.Fatalf("Validate() = %v, want ErrInvalidField", sample.Validate())
			}
		})
	}
}
