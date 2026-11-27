// HYDRA-UMC-TELEMETRY-COLLECTOR - canids/table_damaged_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
package canids

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTheContractFileWithAMarkLoadsAndOneWithContentAfterItDoesNot(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "can_ids.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Parse(append([]byte("\ufeff"), raw...)); err != nil {
		t.Fatalf("with a byte order mark: %v", err)
	}
	for name, tail := range map[string]string{"a second document": "{}", "a stray brace": "}", "junk": "xx", "NUL bytes of a power cut": "\x00\x00"} {
		damaged := append(append([]byte{}, raw...), tail...)
		if _, err := Parse(damaged); err == nil {
			t.Errorf("%s after the document was accepted", name)
		}
	}
	if _, err := Parse(raw[:len(raw)/2]); err == nil {
		t.Error("half a document was accepted")
	}
}
